mod constants;
mod error;
mod ffi;
pub mod settings;

pub use error::Error;
pub use pitaya_core::{
    cluster, context::Context, handler, message, metrics, protos, session::Session, state::State,
    utils, Never, ToError,
};
use pitaya_core::{
    cluster::server::{ServerId, ServerInfo, ServerKind},
    context,
    service::{self, RpcHandler},
    Route,
};
pub use pitaya_etcd_nats_cluster::{EtcdLazy, NatsRpcClient, NatsRpcServer};
pub use pitaya_macros::{handlers, json_handler, protobuf_handler};
use slog::{debug, error, info, o, trace, warn};
use std::sync::Arc;
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex, RwLock},
    task,
};

struct Tasks {
    listen_for_rpc: task::JoinHandle<()>,
    graceful_shutdown: task::JoinHandle<()>,
}

struct SharedState {
    tasks: std::sync::Mutex<Option<Tasks>>,
}

struct ClusterComponents {
    discovery: Arc<Mutex<Box<dyn cluster::Discovery>>>,
    rpc_server: Arc<Box<dyn cluster::RpcServer>>,
    rpc_client: Arc<Box<dyn cluster::RpcClient>>,
}

/// Pitaya represent a pitaya server.
///
/// It will registers itself using a service discovery client in order
/// to be discovered and send RPCs to other Pitaya servers.
pub struct Pitaya {
    discovery: Arc<Mutex<Box<dyn cluster::Discovery>>>,
    rpc_server: Arc<Box<dyn cluster::RpcServer>>,
    rpc_client: Arc<Box<dyn cluster::RpcClient>>,
    shared_state: Arc<SharedState>,
    logger: slog::Logger,
    settings: Arc<settings::Settings>,
    metrics_reporter: metrics::ThreadSafeReporter,
    container: Arc<state::Container>,
    remote: Arc<service::Remote>,
}

impl Clone for Pitaya {
    fn clone(&self) -> Self {
        Self {
            discovery: self.discovery.clone(),
            rpc_server: self.rpc_server.clone(),
            rpc_client: self.rpc_client.clone(),
            shared_state: self.shared_state.clone(),
            logger: self.logger.clone(),
            settings: self.settings.clone(),
            metrics_reporter: self.metrics_reporter.clone(),
            container: self.container.clone(),
            remote: self.remote.clone(),
        }
    }
}

impl Pitaya {
    async fn new<'a>(
        server_info: Arc<ServerInfo>,
        logger: slog::Logger,
        cluster_components: ClusterComponents,
        metrics_reporter: metrics::ThreadSafeReporter,
        settings: settings::Settings,
        rpc_dispatch: service::RpcDispatch,
        container: Arc<state::Container>,
    ) -> Result<Self, Error> {
        if server_info.kind.0.is_empty() {
            return Err(Error::InvalidServerKind);
        }

        debug!(logger, "init"; "settings" => ?settings, "server_info" => ?server_info);
        let remote = Arc::new(service::Remote::new(
            logger.new(o!()),
            cluster_components.discovery.clone(),
            cluster_components.rpc_client.clone(),
            rpc_dispatch,
        ));

        Ok(Self {
            shared_state: Arc::new(SharedState {
                tasks: std::sync::Mutex::new(None),
            }),
            discovery: cluster_components.discovery,
            rpc_client: cluster_components.rpc_client,
            rpc_server: cluster_components.rpc_server,
            logger,
            settings: Arc::new(settings),
            metrics_reporter,
            container,
            remote,
        })
    }

    /// Try to get a server info using its id.
    ///
    /// The function will fail in case of network errors. The server info will be
    /// returned only in the case of this server existing.
    pub async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<ServerInfo>>, Error> {
        let mut discovery = self.discovery.lock().await;
        let server = discovery.server_by_id(server_id, Some(server_kind)).await?;
        Ok(server)
    }

    /// Gracefully shuts down the Pitaya server.
    pub async fn shutdown(self) -> Result<(), Error> {
        let tasks = self
            .shared_state
            .tasks
            .lock()
            .unwrap()
            .take()
            .expect("tasks should've been created");

        info!(self.logger, "shutting down pitaya server");

        info!(self.logger, "stopping service discovery");
        self.discovery.lock().await.shutdown().await?;
        info!(self.logger, "stopped");

        info!(self.logger, "stopping rpc client");
        self.rpc_client.shutdown().await?;

        info!(self.logger, "stopping rpc server");
        self.rpc_server.shutdown().await?;

        info!(self.logger, "waiting listen for rpc task");
        tasks.listen_for_rpc.await?;

        info!(self.logger, "waiting graceful shutdown task");
        tasks.graceful_shutdown.await?;
        Ok(())
    }

    /// Sends an RPC to a server.
    ///
    /// Passing a server kind is only an optimization for the service
    /// discovery client. By giving the kind of the server, it is easier for
    /// the service discovery client to find the provided server id.
    ///
    /// The method will either return an error or a response that comes from the server.
    pub async fn send_rpc_to_server(
        &mut self,
        ctx: context::Context,
        server_id: &ServerId,
        server_kind: Option<&ServerKind>,
        route_str: &str,
        data: Vec<u8>,
    ) -> Result<protos::Response, Error> {
        debug!(self.logger, "sending rpc"; "server_id" => %server_id.0);

        let server_info = {
            debug!(self.logger, "getting servers");
            self.discovery
                .lock()
                .await
                .server_by_id(server_id, server_kind)
                .await?
        };

        if let Some(server_info) = server_info {
            let msg = message::Message {
                kind: message::Kind::Request,
                id: 0,
                route: route_str.to_string(),
                data,
                compressed: false,
                err: false,
            };

            debug!(self.logger, "sending rpc");
            let res = self
                .rpc_client
                .call(ctx, protos::RpcType::User, msg, server_info)
                .await
                .map(|res| {
                    trace!(self.logger, "received rpc response"; "res" => ?res);
                    res
                })?;
            Ok(res)
        } else {
            Err(Error::ServerIdNotFound(server_id.0.clone()))
        }
    }

    /// Similar to the method `send_rpc_to_server`. The difference is that the RPC will not be
    /// sent to a specific Server. It will randomize over all servers of the specified server kind
    /// on the route.
    pub async fn send_rpc(
        &self,
        ctx: context::Context,
        route_str: &str,
        data: Vec<u8>,
    ) -> Result<protos::Response, Error> {
        debug!(self.logger, "sending rpc");

        let route = Route::try_from_str(route_str.to_string()).ok_or(Error::InvalidRoute)?;
        let server_kind = route
            .server_kind()
            .map(ServerKind::from)
            .ok_or_else(|| Error::NoServerKindOnRoute(route_str.to_string()))?;

        debug!(self.logger, "getting servers");
        let servers = self
            .discovery
            .lock()
            .await
            .servers_by_kind(&server_kind)
            .await?;

        debug!(self.logger, "getting random server");
        if let Some(random_server_info) = utils::random_server(&servers) {
            debug!(self.logger, "sending rpc");

            let msg = message::Message {
                kind: message::Kind::Request,
                id: 0,
                route: route_str.to_string(),
                data,
                compressed: false,
                err: false,
            };

            let res = self
                .rpc_client
                .call(ctx, protos::RpcType::User, msg, random_server_info)
                .await
                .map(|res| {
                    trace!(self.logger, "received rpc response"; "res" => ?res);
                    res
                })?;
            Ok(res)
        } else {
            error!(self.logger, "found no servers for kind"; "kind" => &server_kind.0);
            Err(Error::NoServersFound(server_kind))
        }
    }

    pub async fn send_kick(
        &self,
        server_id: ServerId,
        server_kind: ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error> {
        let res = self
            .rpc_client
            .kick_user(server_id, server_kind, kick_msg)
            .await?;
        Ok(res)
    }

    pub async fn send_push_to_user(
        &self,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error> {
        self.rpc_client.push_to_user(server_kind, push_msg).await?;
        Ok(())
    }

    async fn start(&mut self) -> Result<oneshot::Receiver<()>, Error> {
        info!(self.logger, "starting pitaya server");

        let (graceful_shutdown_sender, graceful_shutdown_receiver) = oneshot::channel();
        let (app_die_sender, app_die_receiver) = broadcast::channel(20);

        self.metrics_reporter.write().await.start().await.unwrap();

        let graceful_shutdown = tokio::spawn(Self::graceful_shutdown_task(
            self.logger.new(o!("task" => "graceful_shutdown")),
            graceful_shutdown_sender,
            app_die_receiver,
        ));

        self.rpc_client.start().await?;

        let rpc_server_connection = self.rpc_server.start().await?;
        let listen_for_rpc = tokio::spawn(Self::start_handlers_task(
            self.logger.new(o!("task" => "start_listen_for_rpc")),
            rpc_server_connection,
            self.container.clone(),
            self.remote.clone(),
        ));

        self.shared_state.tasks.lock().unwrap().replace(Tasks {
            listen_for_rpc,
            graceful_shutdown,
        });

        // Always start the service discovery last, since before getting RPCs we need to make
        // sure that the server is set up.
        self.discovery.lock().await.start(app_die_sender).await?;

        info!(self.logger, "finished starting pitaya server");
        Ok(graceful_shutdown_receiver)
    }

    async fn graceful_shutdown_task(
        logger: slog::Logger,
        graceful_shutdown_sender: oneshot::Sender<()>,
        mut app_die_receiver: broadcast::Receiver<()>,
    ) {
        use tokio::signal::unix::{signal, SignalKind};
        let mut signal_hangup =
            signal(SignalKind::hangup()).expect("failed to register signal handling");
        let mut signal_interrupt =
            signal(SignalKind::interrupt()).expect("failed to register signal handling");
        let mut signal_terminate =
            signal(SignalKind::terminate()).expect("failed to register signal handling");

        tokio::select! {
            _ = signal_hangup.recv() => {
                warn!(logger, "received hangup signal");
                let _ = graceful_shutdown_sender.send(());
            }
            _ = signal_interrupt.recv() => {
                warn!(logger, "received interrupt signal");
                let _ = graceful_shutdown_sender.send(());
            }
            _ = signal_terminate.recv() => {
                warn!(logger, "received terminate signal");
                let _ = graceful_shutdown_sender.send(());
            }
            _ = app_die_receiver.recv() => {
                warn!(logger, "received app die message");
                let _ = graceful_shutdown_sender.send(());
            }
        }
    }

    async fn start_handlers_task(
        logger: slog::Logger,
        mut rpc_server_connection: mpsc::Receiver<cluster::Rpc>,
        container: Arc<state::Container>,
        remote: Arc<service::Remote>,
    ) {
        loop {
            let maybe_rpc = rpc_server_connection.recv().await;

            if maybe_rpc.is_none() {
                debug!(logger, "listen rpc task exiting");
                break;
            }

            let rpc = maybe_rpc.unwrap();
            let container = container.clone();
            let remote = remote.clone();

            // Spawn task to handle the incoming RPC.
            let _ = tokio::spawn(async move {
                remote.process_rpc(rpc, container).await;
            });
        }
    }

    async fn add_cluster_subscriber(
        &mut self,
        mut subscriber: Box<dyn FnMut(cluster::Notification) + Send + 'static>,
    ) {
        let logger = self.logger.new(o!());
        let mut subscription = self.discovery.lock().await.subscribe();

        tokio::spawn(async move {
            loop {
                match subscription.recv().await {
                    Ok(n) => {
                        subscriber(n);
                    }
                    Err(broadcast::RecvError::Lagged(num_skipped_msgs)) => {
                        // This should not happen. The only case where this might be an issue is if the
                        // callback is doing some heavy processing for some reason.
                        warn!(logger, "cluster subscriber lagged behind!"; "num_messages" => num_skipped_msgs);
                    }
                    Err(broadcast::RecvError::Closed) => {
                        debug!(logger, "cluster subscriber channel closed");
                        return;
                    }
                }
            }
        });
    }

    /// Gets the logger instance from the Pitaya server.
    pub fn logger(&self) -> slog::Logger {
        self.logger.clone()
    }

    pub async fn inc_counter(&self, name: &str, labels: &[&str]) {
        if let Err(e) = self.metrics_reporter.read().await.inc_counter(name, labels) {
            warn!(self.logger, "failed to increment counter"; "name" => name, "error" => %e);
        }
    }

    pub async fn observe_hist(&self, name: &str, value: f64, labels: &[&str]) {
        if let Err(e) = self
            .metrics_reporter
            .read()
            .await
            .observe_hist(name, value, labels)
        {
            warn!(self.logger, "failed to observe histogram"; "name" => name, "error" => %e);
        }
    }

    pub async fn set_gauge(&self, name: &str, value: f64, labels: &[&str]) {
        if let Err(e) = self
            .metrics_reporter
            .read()
            .await
            .set_gauge(name, value, labels)
        {
            warn!(self.logger, "failed to set gauge value"; "name" => name, "error" => %e);
        }
    }

    pub async fn add_gauge(&self, name: &str, value: f64, labels: &[&str]) {
        if let Err(e) = self
            .metrics_reporter
            .read()
            .await
            .add_gauge(name, value, labels)
        {
            warn!(self.logger, "failed to add value to gauge"; "name" => name, "error" => %e);
        }
    }
}

/// Use to construct a new Pitaya instance.
///
/// You can construct the instance directly as well,
/// but in most cases you'll want to use this struct instead.
pub struct PitayaBuilder<'a> {
    frontend: bool,
    logger: Option<slog::Logger>,
    cluster_subscriber: Option<Box<dyn FnMut(cluster::Notification) + Send + 'static>>,
    env_prefix: Option<&'a str>,
    config_file: Option<&'a str>,
    base_settings: settings::Settings,
    rpc_handler: Option<RpcHandler>,
    client_handlers: handler::Handlers,
    server_handlers: handler::Handlers,
    container: state::Container,
    server_info: Option<Arc<ServerInfo>>,
    metrics_reporter: Option<metrics::ThreadSafeReporter>,
}

impl<'a> Default for PitayaBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> PitayaBuilder<'a> {
    /// Creates a new PitayaBuilder instance.
    pub fn new() -> Self {
        Self {
            frontend: false,
            rpc_handler: None,
            logger: None,
            cluster_subscriber: None,
            env_prefix: None,
            config_file: None,
            base_settings: Default::default(),
            client_handlers: handler::Handlers::new(),
            server_handlers: handler::Handlers::new(),
            container: state::Container::new(),
            server_info: None,
            metrics_reporter: None,
        }
    }

    /// Specify the Pitaya server information.
    pub fn with_server_info(mut self, server_info: Arc<ServerInfo>) -> Self {
        self.server_info.replace(server_info);
        self
    }

    /// Specify the root logger for the Pitaya instance.
    pub fn with_logger(mut self, logger: slog::Logger) -> Self {
        self.logger.replace(logger);
        self
    }

    /// Specifies the client handlers used by the server.
    pub fn with_client_handlers(mut self, handlers: handler::Handlers) -> Self {
        self.client_handlers = handlers;
        self
    }

    /// Specifies the server handlers used by the server.
    pub fn with_server_handlers(mut self, handlers: handler::Handlers) -> Self {
        self.server_handlers = handlers;
        self
    }

    /// Specify whether the server will be a frontend server or not.
    pub fn with_frontend(mut self, frontend: bool) -> Self {
        self.frontend = frontend;
        self
    }

    /// Specifies the raw rpc handler used by the server. This method is mutually exclusive with
    /// `with_client_handlers` and `with_server_handlers`.
    ///
    /// In most cases you'll not be using this method. This is mainly used for creating FFI
    /// bindings with other languages.
    pub fn with_rpc_handler(mut self, handler: RpcHandler) -> Self {
        self.rpc_handler.replace(handler);
        self
    }

    /// Specifies a listener for service discovery. The subscriber will be called whenever a new
    /// server is discovered or when it is removed from the known servers list.
    pub fn with_cluster_subscriber<F>(mut self, subscriber: F) -> Self
    where
        F: FnMut(cluster::Notification) + Send + 'static,
    {
        self.cluster_subscriber.replace(Box::new(subscriber));
        self
    }

    /// Specifies the env prefix for the configuration file. This can be used for overriding configuration
    /// values specified either with a config value or with `with_base_settings`.
    pub fn with_env_prefix(mut self, prefix: &'a str) -> Self {
        self.env_prefix.replace(prefix);
        self
    }

    /// Specifies the config file to be used.
    pub fn with_config_file(mut self, config_file: &'a str) -> Self {
        self.config_file.replace(config_file);
        self
    }

    /// Specifies the base settings to be used. These settings are the least prioritary. Their values
    /// will be overwritten by config files and then environment variables.
    pub fn with_base_settings(mut self, base_settings: settings::Settings) -> Self {
        self.base_settings = base_settings;
        self
    }

    /// Specifies a state to be managed by Pitaya. They can be later queries on a handler using
    /// `pitaya::State`.
    ///
    /// Since a state will be sent and used between threads, the type has to be `Sync + Send + 'static`.
    pub fn with_state<T: Sync + Send + 'static>(self, state: T) -> Self {
        if !self.container.set(state) {
            panic!("cannot set state for the given type, since it was already set");
        }
        self
    }

    /// Specifies a custom metrics reporter for pitaya to use.
    pub fn with_metrics_reporter(mut self, metrics_reporter: metrics::ThreadSafeReporter) -> Self {
        self.metrics_reporter.replace(metrics_reporter);
        self
    }

    /// Builds the Pitaya instance.
    ///
    /// A Pitaya instance will be returned and also a shutdown receiver.
    /// Listening for the receiver channel is a way of having a graceful shutdown implementation.
    /// By default Pitaya will listen for SIGINT and SIGTERM, as well as internal errors.
    /// When the channel is called, the application should call `Pitaya::shutdown`.
    pub async fn build(mut self) -> Result<(Pitaya, oneshot::Receiver<()>), Error> {
        if self.rpc_handler.is_none()
            && (self.client_handlers.is_empty() && self.server_handlers.is_empty())
        {
            panic!("either client and server handlers should be defined or an RPC handler should be provided");
        }

        if self.server_info.is_none() {
            panic!("you need to provide a server info (see .with_server_info)");
        }

        let logger = self
            .logger
            .expect("a logger should be passed to PitayaBuilder");
        let settings =
            settings::Settings::merge(self.base_settings, self.env_prefix, self.config_file)?;
        let etcd_settings = Arc::new(settings.etcd.clone());
        let nats_settings = Arc::new(settings.nats.clone());
        let server_info = self.server_info.expect("server_info should not be None");

        let metrics_reporter: metrics::ThreadSafeReporter = self
            .metrics_reporter
            .unwrap_or_else(|| Arc::new(RwLock::new(Box::new(metrics::DummyReporter {}))));

        let discovery: Arc<Mutex<Box<dyn cluster::Discovery>>> = Arc::new(Mutex::new(Box::new(
            pitaya_etcd_nats_cluster::EtcdLazy::new(
                logger.clone(),
                server_info.clone(),
                etcd_settings,
            )
            .await?,
        )));

        if !self.container.set(metrics_reporter.clone()) {
            panic!("should not fail to set metrics reporter state");
        }

        self.container.freeze();
        // Freeze state, so we cannot modify it later.
        let container = Arc::new(self.container);

        let rpc_server: Arc<Box<dyn cluster::RpcServer>> = Arc::new(Box::new(NatsRpcServer::new(
            logger.clone(),
            server_info.clone(),
            nats_settings.clone(),
            tokio::runtime::Handle::current(),
            metrics_reporter.clone(),
        )));
        let rpc_client: Arc<Box<dyn cluster::RpcClient>> = Arc::new(Box::new(NatsRpcClient::new(
            logger.clone(),
            nats_settings,
            server_info.clone(),
            metrics_reporter.clone(),
        )));

        let rpc_dispatch = if let Some(rpc_handler) = self.rpc_handler {
            service::RpcDispatch::Raw(rpc_handler)
        } else {
            service::RpcDispatch::Handlers {
                client: Arc::new(self.client_handlers),
                server: Arc::new(self.server_handlers),
            }
        };

        let mut p = Pitaya::new(
            server_info,
            logger,
            ClusterComponents {
                discovery,
                rpc_server,
                rpc_client,
            },
            metrics_reporter,
            settings,
            rpc_dispatch,
            container,
        )
        .await?;

        if let Some(subscriber) = self.cluster_subscriber {
            p.add_cluster_subscriber(subscriber).await;
        }

        let shutdown_receiver = p.start().await?;
        Ok((p, shutdown_receiver))
    }
}
