use crate::{context, message, protos};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

pub mod server;
pub use server::{ServerId, ServerInfo, ServerKind};

#[derive(Debug, Error)]
pub enum Error {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("failed to communicate with cluster: {0}")]
    ClusterCommunication(String),

    #[error("connection was lost: {0}")]
    LostConnection(String),

    #[error("nats: {0}")]
    Nats(std::io::Error),

    #[error("nats connection not open")]
    NatsConnectionNotOpen,

    #[error("no servers of kind {0:?} found")]
    NoServersFound(server::ServerKind),

    #[error("frontend server id {0:?} not found")]
    FrontendServerNotFound(server::ServerId),

    #[error("corrupt server: {0}")]
    CorruptServer(String),

    #[error("empty server kind")]
    EmptyServerKind,

    #[error("empty user id")]
    EmptyUserId,

    #[error("internal: {0}")]
    Internal(String),

    #[error("invalid server response: {0}")]
    InvalidServerResponse(prost::DecodeError),

    #[error("rpc server already started")]
    RpcServerAlreadyStarted,

    #[error("already connected")]
    AlreadyConnected,
}

// The Discovery trait allows the program to discover other pitaya servers in the cluster.
#[async_trait]
pub trait Discovery: Send + 'static {
    // Discover a server based on its id.
    async fn server_by_id(
        &mut self,
        id: &server::ServerId,
        kind: Option<&server::ServerKind>,
    ) -> Result<Option<Arc<ServerInfo>>, Error>;

    // Discover servers by a specified kind.
    async fn servers_by_kind(
        &mut self,
        kind: &server::ServerKind,
    ) -> Result<Vec<Arc<ServerInfo>>, Error>;

    // Starts the discovery.
    async fn start(&mut self, app_die_sender: broadcast::Sender<()>) -> Result<(), Error>;

    // Stops the dicovery.
    async fn shutdown(&mut self) -> Result<(), Error>;

    // Allows the current server to subscribe for notifications of added and removed servers.
    fn subscribe(&mut self) -> broadcast::Receiver<Notification>;
}

// Server represents a trait for handling RPCs comming from the cluster.
#[async_trait]
pub trait RpcServer: Sync + Send + 'static {
    // Starts the server.
    async fn start(&self) -> Result<mpsc::Receiver<Rpc>, Error>;

    // Shuts down the server.
    async fn shutdown(&self) -> Result<(), Error>;
}

// Client represents an RPC client for the other servers in the cluster.
#[async_trait]
pub trait RpcClient: Send + Sync + 'static {
    // This function sends an RPC to a given server in the cluster.
    async fn call(
        &self,
        ctx: context::Context,
        rpc_type: protos::RpcType,
        msg: message::Message,
        server_info: Arc<ServerInfo>,
    ) -> Result<protos::Response, Error>;

    // Kicks a user connected to a specific frontend server.
    async fn kick_user(
        &self,
        server_id: server::ServerId,
        server_kind: server::ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error>;

    // Sends a push to a user connected to a specific frontend server.
    async fn push_to_user(
        &self,
        server_kind: server::ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error>;

    // Starts the server.
    async fn start(&self) -> Result<(), Error>;

    // Shuts down the client.
    async fn shutdown(&self) -> Result<(), Error>;
}

// A notification occurs whenever a cluster enters or exists the cluster.
#[derive(Debug, Clone)]
pub enum Notification {
    // Represents a server that was added on the cluster.
    ServerAdded(Arc<ServerInfo>),
    // Represents a server that was removed from the cluster.
    ServerRemoved(Arc<ServerInfo>),
}

// Represents an RPC that comes from another server in the cluster.
#[derive(Debug)]
pub struct Rpc {
    req: Vec<u8>,
    responder: oneshot::Sender<Vec<u8>>,
}

impl Rpc {
    pub fn new(req: Vec<u8>, responder: oneshot::Sender<Vec<u8>>) -> Self {
        Self { req, responder }
    }

    pub fn request(&self) -> &[u8] {
        &self.req
    }

    // Responds to the RPC with the given response. Returns true
    // on success and false if it was not able to answer.
    pub fn respond(self, res: Vec<u8>) -> bool {
        self.responder.send(res).map(|_| true).unwrap_or(false)
    }

    pub fn consume(self) -> (Vec<u8>, oneshot::Sender<Vec<u8>>) {
        (self.req, self.responder)
    }
}
