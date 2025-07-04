use std::{any, fmt::Debug, hash::Hash, sync::Arc};

use n0_future::{FuturesUnordered, future};
use tokio::sync::{mpsc, oneshot};

/// Parameters for the entity manager system.
pub trait Params: Send + Sync + 'static {
    /// Entity id type.
    type Id: Debug + Hash + Eq + Clone + Send + Sync + 'static;
    /// Global state type.
    type GlobalState: Default + Send + Sync + 'static;
    /// Entity state type.
    type State: Default + Send + Sync + 'static;
    /// Function to call when shutting down an entity actor.
    fn on_drop(state: entity_actor::State<Self>) -> impl Future<Output = ()> + Send + 'static
    where
        Self: Sized;
}

/// Sent to the main actor and then delegated to the entity actor to spawn a new task.
pub struct Spawn<P: Params> {
    id: P::Id,
    f: Box<dyn FnOnce(CbArg<P>) -> future::Boxed<()> + Send>,
}

pub enum CbArg<P: Params> {
    /// We successfully spawned a new task inside the entity actor.
    Ok(entity_actor::State<P>),
    /// The entity actor is busy and cannot spawn a new task.
    Busy,
    /// The entity actor is dead.
    Dead,
}

/// Sent from the entity actor to the main actor to notify that it is shutting down.
///
/// With this message the entity actor gives back the receiver for its command channel,
/// so it can be reusd either immediately if commands come in during shutdown, or later
/// if the entity actor is reused for a different entity.
struct Shutdown<P: Params> {
    id: P::Id,
    receiver: mpsc::Receiver<entity_actor::Command<P>>,
}

struct ShutdownAll {
    tx: oneshot::Sender<()>,
}

/// Sent from the main actor to the entity actor to notify that it has completed shutdown.
///
/// With this message the entity actor sends back the remaining state. The tasks set
/// at this point must be empty, as the entity actor has already completed all tasks.
struct ShutdownComplete<P: Params> {
    state: entity_actor::State<P>,
    tasks: FuturesUnordered<future::Boxed<()>>,
}

mod entity_actor {
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use n0_future::{FuturesUnordered, StreamExt, future};
    use tokio::sync::mpsc;

    use super::{Params, Spawn};
    use crate::CbArg;

    #[derive(Debug)]
    pub struct State<P: Params> {
        pub id: P::Id,
        pub global: Arc<P::GlobalState>,
        pub state: Arc<AtomicRefCell<P::State>>,
    }

    impl<P: Params> Clone for State<P> {
        fn clone(&self) -> Self {
            Self {
                id: self.id.clone(),
                global: self.global.clone(),
                state: self.state.clone(),
            }
        }
    }

    pub enum Command<P: Params> {
        Spawn(Spawn<P>),
    }

    pub struct Actor<P: Params> {
        pub recv: mpsc::Receiver<Command<P>>,
        pub main_send: mpsc::Sender<super::main_actor::Command<P>>,
        pub state: State<P>,
        pub tasks: FuturesUnordered<future::Boxed<()>>,
    }

    impl<P: Params> Actor<P> {

        pub async fn run(mut self) {
            loop {
                tokio::select! {
                    command = self.recv.recv() => {
                        let Some(command) = command else {
                            // Channel closed, this means that the main actor is shutting down.
                            self.recycle_state().await;
                            break;
                        };
                        match command {
                            Command::Spawn(spawn) => {
                                let task = (spawn.f)(CbArg::Ok(self.state.clone()));
                                self.tasks.push(task);
                            }
                        }
                    }
                    Some(_) = self.tasks.next() => {
                        if self.tasks.is_empty() && self.recv.is_empty() {
                            // No more tasks and no more commands, we can recycle the actor.
                            self.recycle_state().await;
                            break; // Exit the loop, actor is done.
                        }
                    }
                }
            }
        }

        async fn recycle_state(self) {
            // we can't check if recv is empty here, since new messages might come in while we are in recycle_state.
            assert!(self.tasks.is_empty(), "Tasks must be empty before recycling");
            // notify main actor that we are starting to shut down.
            // if the main actor is shutting down, this could fail, but we don't care.
            self.main_send
                .send(super::main_actor::Command::Shutdown(super::Shutdown {
                    id: self.state.id.clone(),
                    receiver: self.recv,
                }))
                .await.ok();
            P::on_drop(self.state.clone()).await;
            // Notify the main actor that we have completed shutdown.
            // here we also give back the rest of ourselves so the main actor can recycle us.
            self.main_send
                .send(super::main_actor::Command::ShutdownComplete(
                    super::ShutdownComplete {
                        state: self.state,
                        tasks: self.tasks,
                    },
                ))
                .await
                .ok();
        }

        /// Recycle the actor for reuse by setting its state to default.
        ///
        /// This also checks several invariants:
        /// - There must be no pending messages in the receive channel.
        /// - The sender must have a strong count of 1, meaning no other references exist
        /// - The tasks set must be empty, meaning no tasks are running.
        /// - The global state must match the scope provided.
        /// - The state must be unique to the actor, meaning no other references exist.
        pub fn recycle(&mut self, scope: &Arc<P::GlobalState>) {
            assert!(
                self.recv.is_empty(),
                "Cannot recycle actor with pending messages"
            );
            assert!(
                self.recv.sender_strong_count() == 1,
                "There must be only one sender left"
            );
            assert!(
                self.tasks.is_empty(),
                "Tasks must be empty before recycling"
            );
            assert!(
                Arc::ptr_eq(&self.state.global, scope),
                "Global state must match the scope"
            );
            assert!(
                Arc::strong_count(&self.state.state) == 1,
                "State must be unique to the actor"
            );
            let state =
                Arc::get_mut(&mut self.state.state).expect("State must be unique to the actor");
            *state.borrow_mut() = P::State::default(); // Clear the state for reuse.
        }
    }
}

mod main_actor {
    use std::{collections::HashMap, sync::Arc};

    use atomic_refcell::AtomicRefCell;
    use n0_future::FuturesUnordered;
    use tokio::{sync::mpsc, task::JoinSet};
    use tracing::{error, warn};

    use super::{Params, Shutdown, Spawn};
    use crate::{CbArg, ShutdownComplete, ShutdownAll, entity_actor};

    pub(super) enum Command<P: Params> {
        Spawn(Spawn<P>),
        Shutdown(Shutdown<P>),
        ShutdownComplete(ShutdownComplete<P>),
        ShutdownAll(ShutdownAll),
    }

    impl<P: Params> From<Shutdown<P>> for Command<P> {
        fn from(shutdown: Shutdown<P>) -> Self {
            Command::Shutdown(shutdown)
        }
    }

    impl<P: Params> From<ShutdownComplete<P>> for Command<P> {
        fn from(shutdown_complete: ShutdownComplete<P>) -> Self {
            Command::ShutdownComplete(shutdown_complete)
        }
    }

    pub enum EntityHandle<P: Params> {
        /// A running entity actor.
        Live {
            send: mpsc::Sender<entity_actor::Command<P>>,
        },
        ShuttingDown {
            send: mpsc::Sender<entity_actor::Command<P>>,
            recv: mpsc::Receiver<entity_actor::Command<P>>,
        },
    }

    impl<P: Params> EntityHandle<P> {
        pub fn send(&self) -> &mpsc::Sender<entity_actor::Command<P>> {
            match self {
                EntityHandle::Live { send: sender } => sender,
                EntityHandle::ShuttingDown { send: sender, .. } => sender,
            }
        }
    }

    pub struct Actor<P: Params> {
        /// Channel to receive commands.
        recv: mpsc::Receiver<Command<P>>,
        /// Channel to send commands to ourselves, to hand out to entity actors.
        send: mpsc::Sender<Command<P>>,
        /// Map of live entity actors.
        live: HashMap<P::Id, EntityHandle<P>>,
        /// Global state shared across all entity actors.
        state: Arc<P::GlobalState>,
        /// Tasks that are currently running.
        tasks: JoinSet<()>,
        /// Pool of inactive entity actors to reuse.
        pool: Vec<(
            mpsc::Sender<entity_actor::Command<P>>,
            entity_actor::Actor<P>,
        )>,
        /// Maximum size of the inbox of an entity actor.
        queue_size: usize,
    }

    impl<P: Params> Actor<P> {
        pub fn new(
            state: Arc<P::GlobalState>,
            send: mpsc::Sender<Command<P>>,
            recv: tokio::sync::mpsc::Receiver<Command<P>>,
            pool_capacity: usize,
            queue_size: usize,
        ) -> Self {
            Self {
                recv,
                send,
                live: HashMap::new(),
                tasks: JoinSet::new(),
                state,
                pool: Vec::with_capacity(pool_capacity),
                queue_size,
            }
        }

        pub fn recycle(
            &mut self,
            sender: mpsc::Sender<entity_actor::Command<P>>,
            mut actor: entity_actor::Actor<P>,
        ) {
            assert!(sender.strong_count() == 1);
            // todo: check that sender and receiver are the same channel. tokio does not have an api for this, unfortunately.
            // reset the actor in any case, just to check the invariants.
            actor.recycle(&self.state);
            // Recycle the actor for later use.
            if self.pool.len() < self.pool.capacity() {
                self.pool.push((sender, actor));
            }
        }

        /// Get or create an entity actor for the given id.
        fn get_or_create(&mut self, id: P::Id) -> &mut EntityHandle<P> {
            self.live.entry(id.clone()).or_insert_with(|| {
                if let Some((sender, mut actor)) = self.pool.pop() {
                    actor.state.id = id.clone();
                    actor.state.global = self.state.clone();
                    actor.state.state = Arc::new(AtomicRefCell::new(P::State::default()));
                    self.tasks.spawn(actor.run());
                    EntityHandle::Live { send: sender }
                } else {
                    let (sender, recv) = mpsc::channel(self.queue_size);
                    let state: entity_actor::State<P> = entity_actor::State {
                        id: id.clone(),
                        global: self.state.clone(),
                        state: Arc::new(AtomicRefCell::new(P::State::default())),
                    };
                    let actor = entity_actor::Actor {
                        main_send: self.send.clone(),
                        recv,
                        state,
                        tasks: FuturesUnordered::new(),
                    };
                    self.tasks.spawn(actor.run());
                    EntityHandle::Live { send: sender }
                }
            })
        }

        pub async fn run(mut self) {
            loop {
                tokio::select! {
                    Some(command) = self.recv.recv() => {
                        match command {
                            Command::Spawn(spawn) => {
                                let entity_handle = self.get_or_create(spawn.id.clone());
                                let sender = entity_handle.send();
                                if let Err(e) = sender.try_send(entity_actor::Command::Spawn(spawn)) {
                                    match e {
                                        mpsc::error::TrySendError::Full(cmd) => {
                                            let entity_actor::Command::Spawn(spawn) = cmd;
                                            warn!("Entity actor inbox is full, cannot send command to entity actor {:?}.", spawn.id);
                                            // we await in the select here, but I think this is fine, since the actor is busy.
                                            // maybe slowing things down a bit is helpful.
                                            (spawn.f)(CbArg::Busy).await;
                                        }
                                        mpsc::error::TrySendError::Closed(cmd) => {
                                            let entity_actor::Command::Spawn(spawn) = cmd;
                                            error!("Entity actor inbox is closed, cannot send command to entity actor {:?}.", spawn.id);
                                            // give the caller a chance to react to this bad news.
                                            // at this point we are in trouble anyway, so awaiting is going to be the least of our problems.
                                            (spawn.f)(CbArg::Dead).await;
                                        }
                                    }
                                }
                            }
                            Command::Shutdown(Shutdown { id, receiver }) => {
                                let Some(entity_handle) = self.live.remove(&id) else {
                                    error!("Received shutdown command for unknown entity actor {id:?}");
                                    break;
                                };
                                let EntityHandle::Live { send } = entity_handle else {
                                    error!("Received shutdown command for entity actor {id:?} that is already shutting down");
                                    break;
                                };
                                self.live.insert(id.clone(), EntityHandle::ShuttingDown {
                                    send,
                                    recv: receiver,
                                });
                            }
                            Command::ShutdownComplete(ShutdownComplete { state, tasks }) => {
                                assert!(Arc::ptr_eq(&state.global, &self.state), "Entity actor global state mismatch");
                                let id = state.id.clone();
                                let Some(entity_handle) = self.live.remove(&id) else {
                                    error!("Received shutdown complete command for unknown entity actor {id:?}");
                                    break;
                                };
                                let EntityHandle::ShuttingDown { send, recv } = entity_handle else {
                                    error!("Received shutdown complete command for entity actor {id:?} that is not shutting down");
                                    break;
                                };
                                // re-assemble the actor from the parts
                                let mut actor = entity_actor::Actor {
                                    main_send: self.send.clone(),
                                    recv,
                                    state,
                                    tasks,
                                };
                                if actor.recv.is_empty() {
                                    // No commands during shutdown, we can recycle the actor.
                                    self.recycle(send, actor);
                                } else {
                                    // We have some pending commands, and need to revive the actor.
                                    *Arc::get_mut(&mut actor.state.state)
                                        .expect("Entity actor state must be unique during shutdown complete")
                                        .borrow_mut() = P::State::default();
                                    self.tasks.spawn(actor.run());
                                    self.live.insert(id.clone(), EntityHandle::Live { send });
                                }
                            }
                            Command::ShutdownAll(arg) => {
                                self.shutdown(arg).await;
                                break;
                            }
                        }
                    }
                    Some(task) = self.tasks.join_next(), if !self.pool.is_empty() => {
                        // Handle completed task
                        if let Err(e) = task {
                            eprintln!("Task failed: {:?}", e);
                        }
                    }
                }
            }
        }

        async fn shutdown(mut self, arg: ShutdownAll) {
            self.pool.clear();
            self.live.clear();
            while let Some(task) = self.tasks.join_next().await {
                if let Err(e) = task {
                    eprintln!("Task failed during shutdown: {:?}", e);
                }
            }
            arg.tx.send(()).ok();
        }
    }
}

pub struct EntityManager<P: Params>(mpsc::Sender<main_actor::Command<P>>);

impl<P: Params> EntityManager<P> {
    pub fn new(
        state: Arc<P::GlobalState>,
        pool_capacity: usize,
        entity_queue_size: usize,
        queue_size: usize,
    ) -> Self {
        let (send, recv) = mpsc::channel(queue_size);
        let actor =
            main_actor::Actor::new(state, send.clone(), recv, pool_capacity, entity_queue_size);
        tokio::spawn(actor.run());
        Self(send)
    }

    pub async fn spawn<F, Fut>(
        &self,
        id: P::Id,
        f: F,
    ) -> Result<(), &'static str>
    where
        F: FnOnce(CbArg<P>) -> Fut + Send + 'static,
        Fut: future::Future<Output = ()> + Send + 'static,
    {
        let spawn = Spawn {
            id,
            f: Box::new(|arg| {
                Box::pin(async move {
                    f(arg).await;
                })
            }),
        };
        self.0.send(main_actor::Command::Spawn(spawn)).await.map_err(|_| "Failed to send spawn command")
    }

    pub async fn shutdown(&self) -> std::result::Result<(), &'static str> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(main_actor::Command::ShutdownAll(ShutdownAll { tx }))
            .await.map_err(|_| "Failed to send shutdown command")?;
        rx.await.map_err(|_| "Failed to receive shutdown confirmation")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Params for () {
        type Id = u64;
        type GlobalState = ();
        type State = u128;
        async fn on_drop(state: entity_actor::State<Self>) {
            
        }
    }

    fn drop(_state: entity_actor::State<()>) -> future::Boxed<()> {
        Box::pin(async move {
            // Simulate some cleanup work
            println!("Dropping entity actor with id: {}", _state.id);
        })
    }

    #[tokio::test]
    async fn test_entity_manager() -> testresult::TestResult<()> {
        let state = Arc::new(());
        let manager = EntityManager::<()>::new(state, 10, 10, 10);
        manager.spawn(1, |arg| async move {
            match arg {
                CbArg::Ok(_) => println!("Spawned successfully"),
                CbArg::Busy => println!("Entity actor is busy"),
                CbArg::Dead => println!("Entity actor is dead"),
            }
        }).await?;
        manager.shutdown().await?;
        Ok(())
    }
}