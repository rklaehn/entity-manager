use std::{fmt::Debug, hash::Hash};

use n0_future::{FuturesUnordered, future};
use tokio::sync::{mpsc, oneshot};

/// Trait to reset an entity state in place.
/// 
/// In many cases this is just assigning the default value, but e.g. for an
/// `Arc<Mutex<T>>` resetting to the default value means an allocation, whereas
/// reset can be done without.
pub trait Reset: Default {
    /// Reset the state to its default value.
    fn reset(&mut self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownCause {
    /// The entity is shutting down gracefully because the entity is idle.
    Idle,
    /// The entity is shutting down because the entity manager is shutting down.
    Soft,
    /// The entity is shutting down because the sender was dropped.
    Drop,
}

/// Parameters for the entity manager system.
pub trait Params: Send + Sync + 'static {
    /// Entity id type.
    type EntityId: Debug + Hash + Eq + Clone + Send + Sync + 'static;
    /// Global state type.
    type GlobalState: Debug + Default + Clone + Send + Sync + 'static;
    /// Entity state type.
    type EntityState: Default + Reset + Clone + Send + Sync + 'static;
    /// Function being called when an entity actor is shutting down.
    fn on_shutdown(
        state: entity_actor::State<Self>,
        cause: ShutdownCause,
    ) -> impl Future<Output = ()> + Send + 'static
    where
        Self: Sized;
}

/// Sent to the main actor and then delegated to the entity actor to spawn a new task.
pub(crate) struct Spawn<P: Params> {
    id: P::EntityId,
    f: Box<dyn FnOnce(CbArg<P>) -> future::Boxed<()> + Send>,
}

pub(crate) struct EntityShutdown;

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
    id: P::EntityId,
    receiver: mpsc::Receiver<entity_actor::Command<P>>,
}

/// Sent from the entity actor to the main actor to notify that it is shutting down
/// due to the sender being dropped.
struct DropShutdown<P: Params> {
    id: P::EntityId,
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
    use n0_future::{FuturesUnordered, StreamExt, future};
    use tokio::sync::mpsc;

    use crate::Reset;

    use super::{
        CbArg, DropShutdown, EntityShutdown, Params, Shutdown, ShutdownCause, ShutdownComplete,
        Spawn,
    };

    #[derive(Debug)]
    pub struct State<P: Params> {
        pub id: P::EntityId,
        pub global: P::GlobalState,
        pub state: P::EntityState,
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
        EntityShutdown(EntityShutdown),
    }

    impl<P: Params> From<EntityShutdown> for Command<P> {
        fn from(_: EntityShutdown) -> Self {
            Self::EntityShutdown(EntityShutdown)
        }
    }

    pub struct Actor<P: Params> {
        pub recv: mpsc::Receiver<Command<P>>,
        pub main: mpsc::Sender<super::main_actor::InternalCommand<P>>,
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
                            self.drop_shutdown_state().await;
                            break;
                        };
                        match command {
                            Command::Spawn(spawn) => {
                                let task = (spawn.f)(CbArg::Ok(self.state.clone()));
                                self.tasks.push(task);
                            }
                            Command::EntityShutdown(_) => {
                                self.soft_shutdown_state().await;
                                break;
                            }
                        }
                    }
                    Some(_) = self.tasks.next(), if !self.tasks.is_empty() => {}
                }
                if self.tasks.is_empty() && self.recv.is_empty() {
                    // No more tasks and no more commands, we can recycle the actor.
                    self.recycle_state().await;
                    break; // Exit the loop, actor is done.
                }
            }
        }

        /// drop shutdown state.
        ///
        /// All senders for our receive channel were dropped, so we shut down without waiting for any tasks to complete.
        async fn drop_shutdown_state(self) {
            let Self { state, .. } = self;
            P::on_shutdown(state, ShutdownCause::Drop).await;
        }

        /// Soft shutdown state.
        ///
        /// We have received an explicit shutdown command, so we wait for all tasks to complete and then call the shutdown function.
        async fn soft_shutdown_state(mut self) {
            while let Some(_) = self.tasks.next().await {}
            P::on_shutdown(self.state.clone(), ShutdownCause::Soft).await;
            self.main
                .send(
                    DropShutdown {
                        id: self.state.id.clone(),
                    }
                    .into(),
                )
                .await
                .ok();
        }

        async fn recycle_state(self) {
            // we can't check if recv is empty here, since new messages might come in while we are in recycle_state.
            assert!(
                self.tasks.is_empty(),
                "Tasks must be empty before recycling"
            );
            // notify main actor that we are starting to shut down.
            // if the main actor is shutting down, this could fail, but we don't care.
            self.main
                .send(
                    Shutdown {
                        id: self.state.id.clone(),
                        receiver: self.recv,
                    }
                    .into(),
                )
                .await
                .ok();
            P::on_shutdown(self.state.clone(), ShutdownCause::Idle).await;
            // Notify the main actor that we have completed shutdown.
            // here we also give back the rest of ourselves so the main actor can recycle us.
            self.main
                .send(
                    ShutdownComplete {
                        state: self.state,
                        tasks: self.tasks,
                    }
                    .into(),
                )
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
        pub fn recycle(&mut self) {
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
            self.state.state.reset();
        }
    }
}

mod main_actor {
    use std::{collections::HashMap};

    use n0_future::FuturesUnordered;
    use tokio::{sync::mpsc, task::JoinSet};
    use tracing::{error, warn};

    use crate::Reset;

    use super::{
        CbArg, DropShutdown, EntityShutdown, Params, Shutdown, ShutdownAll, ShutdownComplete,
        Spawn, entity_actor,
    };

    pub(super) enum Command<P: Params> {
        Spawn(Spawn<P>),
        ShutdownAll(ShutdownAll),
    }

    impl<P: Params> From<ShutdownAll> for Command<P> {
        fn from(shutdown_all: ShutdownAll) -> Self {
            Self::ShutdownAll(shutdown_all)
        }
    }

    pub(super) enum InternalCommand<P: Params> {
        DropShutdown(DropShutdown<P>),
        ShutdownComplete(ShutdownComplete<P>),
        Shutdown(Shutdown<P>),
    }

    impl<P: Params> From<Shutdown<P>> for InternalCommand<P> {
        fn from(shutdown: Shutdown<P>) -> Self {
            Self::Shutdown(shutdown)
        }
    }

    impl<P: Params> From<ShutdownComplete<P>> for InternalCommand<P> {
        fn from(shutdown_complete: ShutdownComplete<P>) -> Self {
            Self::ShutdownComplete(shutdown_complete)
        }
    }

    impl<P: Params> From<DropShutdown<P>> for InternalCommand<P> {
        fn from(drop_shutdown: DropShutdown<P>) -> Self {
            Self::DropShutdown(drop_shutdown)
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
        /// Channel to receive commands from the outside world.
        /// If this channel is closed, it means we need to shut down in a hurry.
        recv: mpsc::Receiver<Command<P>>,
        /// Channel to receive internal commands from the entity actors.
        /// This channel will never be closed since we also hold a sender to it.
        internal_recv: mpsc::Receiver<InternalCommand<P>>,
        /// Channel to send internal commands to ourselves, to hand out to entity actors.
        internal_send: mpsc::Sender<InternalCommand<P>>,
        /// Map of live entity actors.
        live: HashMap<P::EntityId, EntityHandle<P>>,
        /// Global state shared across all entity actors.
        state: P::GlobalState,
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
            state: P::GlobalState,
            recv: tokio::sync::mpsc::Receiver<Command<P>>,
            pool_capacity: usize,
            queue_size: usize,
            entity_response_queue_size: usize,
        ) -> Self {
            let (internal_send, internal_recv) = mpsc::channel(entity_response_queue_size);
            Self {
                recv,
                internal_send,
                internal_recv,
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
            actor.recycle();
            // Recycle the actor for later use.
            if self.pool.len() < self.pool.capacity() {
                // self.pool.push((sender, actor));
            } else {
            }
        }

        /// Get or create an entity actor for the given id.
        fn get_or_create(&mut self, id: P::EntityId) -> &mut EntityHandle<P> {
            self.live.entry(id.clone()).or_insert_with(|| {
                if let Some((sender, mut actor)) = self.pool.pop() {
                    actor.state.id = id.clone();
                    actor.state.global = self.state.clone();
                    actor.state.state = P::EntityState::default();
                    self.tasks.spawn(actor.run());
                    EntityHandle::Live { send: sender }
                } else {
                    let (sender, recv) = mpsc::channel(self.queue_size);
                    let state: entity_actor::State<P> = entity_actor::State {
                        id: id.clone(),
                        global: self.state.clone(),
                        state: Default::default(),
                    };
                    let actor = entity_actor::Actor {
                        main: self.internal_send.clone(),
                        recv,
                        state,
                        tasks: FuturesUnordered::with_capacity(16),
                    };
                    self.tasks.spawn(actor.run());
                    EntityHandle::Live { send: sender }
                }
            })
        }

        pub async fn run(mut self) {
            loop {
                tokio::select! {
                    cmd = self.recv.recv() => {
                        let Some(cmd) = cmd else {
                            self.hard_shutdown().await;
                            break;
                        };
                        match cmd {
                            Command::Spawn(spawn) => {
                                let entity_handle = self.get_or_create(spawn.id.clone());
                                let sender = entity_handle.send();
                                if let Err(e) = sender.try_send(entity_actor::Command::Spawn(spawn)) {
                                    match e {
                                        mpsc::error::TrySendError::Full(cmd) => {
                                            let entity_actor::Command::Spawn(spawn) = cmd else { panic!() };
                                            warn!("Entity actor inbox is full, cannot send command to entity actor {:?}.", spawn.id);
                                            // we await in the select here, but I think this is fine, since the actor is busy.
                                            // maybe slowing things down a bit is helpful.
                                            (spawn.f)(CbArg::Busy).await;
                                        }
                                        mpsc::error::TrySendError::Closed(cmd) => {
                                            let entity_actor::Command::Spawn(spawn) = cmd else { panic!() };
                                            error!("Entity actor inbox is closed, cannot send command to entity actor {:?}.", spawn.id);
                                            // give the caller a chance to react to this bad news.
                                            // at this point we are in trouble anyway, so awaiting is going to be the least of our problems.
                                            (spawn.f)(CbArg::Dead).await;
                                        }
                                    }
                                }
                            }
                            Command::ShutdownAll(arg) => {
                                self.soft_shutdown(arg).await;
                                break;
                            }
                        }
                    },
                    Some(cmd) = self.internal_recv.recv() => {
                        match cmd {
                            InternalCommand::Shutdown(Shutdown { id, receiver }) => {
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
                            InternalCommand::ShutdownComplete(ShutdownComplete { state, tasks }) => {
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
                                    main: self.internal_send.clone(),
                                    recv,
                                    state,
                                    tasks,
                                };
                                if actor.recv.is_empty() {
                                    // No commands during shutdown, we can recycle the actor.
                                    self.recycle(send, actor);
                                } else {
                                    actor.state.state.reset();
                                    self.tasks.spawn(actor.run());
                                    self.live.insert(id.clone(), EntityHandle::Live { send });
                                }
                            }
                            InternalCommand::DropShutdown(DropShutdown { id }) => {
                                error!("Received drop shutdown command for entity actor {id:?} despite not being in shutdown state");
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

        async fn soft_shutdown(self, arg: ShutdownAll) {
            for handle in self.live.values() {
                handle.send().send(EntityShutdown {}.into()).await.ok();
            }
            self.await_tasks().await;
            arg.tx.send(()).ok();
        }

        async fn hard_shutdown(self) {
            self.await_tasks().await;
        }

        /// Drop everything but the task set and await the completion of all tasks.
        async fn await_tasks(self) {
            let Self {
                mut tasks,
                internal_recv,
                ..
            } = self;
            // this is needed so calls to internal_send in idle shutdown fail fast.
            // otherwise we would have to drain the channel, but we don't care about the messages at
            // this point.
            drop(internal_recv);
            while let Some(res) = tasks.join_next().await {
                if let Err(e) = res {
                    eprintln!("Task failed during shutdown: {:?}", e);
                }
            }
        }
    }
}

pub struct EntityManager<P: Params>(mpsc::Sender<main_actor::Command<P>>);

impl<P: Params> EntityManager<P> {
    pub fn new(
        state: P::GlobalState,
        pool_capacity: usize,
        entity_queue_size: usize,
        queue_size: usize,
        entity_response_queue_size: usize,
    ) -> Self {
        let (send, recv) = mpsc::channel(queue_size);
        let actor = main_actor::Actor::new(
            state,
            recv,
            pool_capacity,
            entity_queue_size,
            entity_response_queue_size,
        );
        tokio::spawn(actor.run());
        Self(send)
    }

    pub async fn spawn<F, Fut>(&self, id: P::EntityId, f: F) -> Result<(), &'static str>
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
        self.0
            .send(main_actor::Command::Spawn(spawn))
            .await
            .map_err(|_| "Failed to send spawn command")
    }

    pub async fn shutdown(&self) -> std::result::Result<(), &'static str> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ShutdownAll { tx }.into())
            .await
            .map_err(|_| "Failed to send shutdown command")?;
        rx.await
            .map_err(|_| "Failed to receive shutdown confirmation")
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    mod mem {
        use std::sync::Arc;

        use atomic_refcell::AtomicRefCell;

        use super::*;

        #[derive(Debug, Clone, Default)]
        struct State(Arc<AtomicRefCell<u128>>);

        impl Reset for State {
            fn reset(&mut self) {
                *self.0.borrow_mut() = Default::default();
            }
        }

        struct Counters;
        impl Params for Counters {
            type EntityId = u64;
            type GlobalState = ();
            type EntityState = State;
            async fn on_shutdown(_state: entity_actor::State<Self>, _cause: ShutdownCause) {}
        }

                struct CountersManager {
            m: EntityManager<Counters>,
        }

        impl entity_actor::State<Counters> {
            async fn with_value(&self, f: impl FnOnce(&mut u128)) -> Result<(), &'static str> {
                let mut r = self.state.0.borrow_mut();
                f(&mut *r);
                Ok(())
            }
        }

        impl CountersManager {
            pub fn new() -> Self {
                let state = ();
                let pool_capacity = 10;
                let entity_queue_size = 10;
                let queue_size = 10;
                let entity_response_queue_size = 100;
                Self {
                    m: EntityManager::<Counters>::new(
                        state,
                        pool_capacity,
                        entity_queue_size,
                        queue_size,
                        entity_response_queue_size,
                    ),
                }
            }

            pub async fn add(&self, id: u64, value: u128) -> Result<(), &'static str> {
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            CbArg::Ok(state) => {
                                // println!(
                                //     "Adding value {} to entity actor with id {:?}",
                                //     value, state.id
                                // );
                                state
                                    .with_value(|v| *v = v.wrapping_add(value))
                                    .await
                                    .unwrap();
                            }
                            CbArg::Busy => println!("Entity actor is busy"),
                            CbArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await
            }

            pub async fn get(&self, id: u64) -> Result<u128, &'static str> {
                let (tx, rx) = oneshot::channel();
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            CbArg::Ok(state) => {
                                state
                                    .with_value(|v| {
                                        tx.send(*v)
                                            .unwrap_or_else(|_| println!("Failed to send value"))
                                    })
                                    .await
                                    .unwrap();
                            }
                            CbArg::Busy => println!("Entity actor is busy"),
                            CbArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await?;
                rx.await.map_err(|_| "Failed to receive value")
            }

            pub async fn shutdown(&self) -> Result<(), &'static str> {
                self.m.shutdown().await
            }
        }

        #[tokio::test]
        async fn bench_entity_manager_mem() -> testresult::TestResult<()> {
            let counter_manager = CountersManager::new();
            for i in 0..1000000 {
                counter_manager.add(i, i as u128).await?;
            }
            counter_manager.shutdown().await?;
            Ok(())
        }
    }

    mod persistent {
        use atomic_refcell::AtomicRefCell;

        use super::*;
        use std::{
            path::{Path, PathBuf}, sync::Arc, time::Duration
        };

        #[derive(Debug, Clone, Default)]
        enum State {
            #[default]
            Disk,
            Memory(u128),
        }

        #[derive(Debug, Clone, Default)]
        struct EntityState(Arc<AtomicRefCell<State>>);

        impl Reset for EntityState {
            fn reset(&mut self) {
                *self.0.borrow_mut() = State::Disk;
            }
        }

        fn get_path(root: impl AsRef<Path>, id: u64) -> PathBuf {
            root.as_ref().join(hex::encode(&id.to_be_bytes()))
        }

        impl entity_actor::State<Counters> {
            async fn with_value(&self, f: impl FnOnce(&mut u128)) -> Result<(), &'static str> {
                let mut r = self.state.0.borrow_mut();
                if let State::Disk = &*r {
                    let path = get_path(&*self.global, self.id);
                    let value = match tokio::fs::read(path).await {
                        Ok(value) => value,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // If the file does not exist, we initialize it to 0.
                            vec![0; 16]
                        }
                        Err(_) => return Err("Failed to read disk state"),
                    };
                    let value = u128::from_be_bytes(
                        value.try_into().map_err(|_| "Invalid disk state format")?,
                    );
                    *r = State::Memory(value);
                }
                let State::Memory(value) = &mut *r else {
                    panic!("State must be Memory at this point");
                };
                f(value);
                Ok(())
            }
        }

        struct Counters;
        impl Params for Counters {
            type EntityId = u64;
            type GlobalState = Arc<PathBuf>;
            type EntityState = EntityState;
            async fn on_shutdown(state: entity_actor::State<Self>, cause: ShutdownCause) {
                let r = state.state.0.borrow();
                if let State::Memory(value) = &*r {
                    let path = get_path(&*state.global, state.id);
                    println!(
                        "{} persisting value {} to {}, cause {:?}",
                        state.id,
                        value,
                        path.display(),
                        cause
                    );
                    let value_bytes = value.to_be_bytes();
                    tokio::fs::write(&path, &value_bytes)
                        .await
                        .expect("Failed to write disk state");
                    println!(
                        "{} persisted value {} to {}",
                        state.id,
                        value,
                        path.display()
                    );
                }
            }
        }

        struct CountersManager {
            m: EntityManager<Counters>,
        }

        impl CountersManager {
            pub fn new(path: impl AsRef<Path>) -> Self {
                let state = Arc::new(path.as_ref().to_owned());
                let pool_capacity = 10;
                let entity_queue_size = 10;
                let queue_size = 10;
                let entity_response_queue_size = 100;
                Self {
                    m: EntityManager::<Counters>::new(
                        state,
                        pool_capacity,
                        entity_queue_size,
                        queue_size,
                        entity_response_queue_size,
                    ),
                }
            }

            pub async fn add(&self, id: u64, value: u128) -> Result<(), &'static str> {
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            CbArg::Ok(state) => {
                                println!(
                                    "Adding value {} to entity actor with id {:?}",
                                    value, state.id
                                );
                                state
                                    .with_value(|v| *v = v.wrapping_add(value))
                                    .await
                                    .unwrap();
                            }
                            CbArg::Busy => println!("Entity actor is busy"),
                            CbArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await
            }

            pub async fn get(&self, id: u64) -> Result<u128, &'static str> {
                let (tx, rx) = oneshot::channel();
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            CbArg::Ok(state) => {
                                state
                                    .with_value(|v| {
                                        tx.send(*v)
                                            .unwrap_or_else(|_| println!("Failed to send value"))
                                    })
                                    .await
                                    .unwrap();
                            }
                            CbArg::Busy => println!("Entity actor is busy"),
                            CbArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await?;
                rx.await.map_err(|_| "Failed to receive value")
            }

            pub async fn shutdown(&self) -> Result<(), &'static str> {
                self.m.shutdown().await
            }
        }

        #[tokio::test]
        async fn bench_entity_manager_fs() -> testresult::TestResult<()> {
            let dir = tempfile::tempdir()?;
            let counter_manager = CountersManager::new(dir.path().to_owned());
            for i in 0..10000 {
                counter_manager.add(i, i as u128).await?;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            counter_manager.shutdown().await?;
            Ok(())
        }
    }
}
