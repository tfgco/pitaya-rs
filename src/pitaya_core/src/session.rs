use crate::{
    cluster::{self, Discovery, RpcClient, ServerId},
    context::Context,
    message, protos, utils,
};
use slog::{debug, error, warn};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::sync::Mutex;

const BIND_ROUTE: &str = "sys.bindsession";
const SESSION_PUSH_ROUTE: &str = "sys.pushsession";
const KICK_ROUTE: &str = "sys.kick";

#[derive(Debug, Error)]
pub enum Error {
    #[error("session is already bound")]
    AlreadyBound,

    #[error("{0}")]
    Cluster(#[from] cluster::Error),

    #[error("is a frontend session")]
    FrontendSession,

    #[error("session was not found on RPC")]
    SessionNotFound,

    #[error("session data is corrupted: {0}")]
    CorruptedSessionData(String),

    #[error("session is not bound")]
    SessionNotBound,
}

/// Session represents the state of a client connection in a frontend server.
/// The session struct is propagated through RPC calls between frontend and backend servers.
pub struct Session {
    /// The user id of the session.
    pub uid: String,
    /// The server id from which this session belongs to.
    pub frontend_id: ServerId,
    /// The id of the session on the frontend server.
    pub frontend_session_id: i64,
    /// Whether this is a frontend session or not.
    pub is_frontend: bool,
    /// Json information that can be propagated with the session along RPCs.
    pub data: HashMap<String, serde_json::Value>,

    discovery: Arc<Mutex<Box<dyn Discovery>>>,
    rpc_client: Arc<dyn RpcClient>,
    logger: slog::Logger,
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "frontend_id={} frontend_session_id={} uid={} data={}",
            self.frontend_id.0,
            self.frontend_session_id,
            self.uid,
            self.data_encoded()
        )
    }
}

impl Session {
    pub fn new(
        logger: slog::Logger,
        req: &protos::Request,
        rpc_client: Arc<dyn RpcClient>,
        discovery: Arc<Mutex<Box<dyn Discovery>>>,
    ) -> Result<Self, Error> {
        if let Some(s) = req.session.as_ref() {
            let data = serde_json::from_slice(&s.data).map_err(|_| {
                Error::CorruptedSessionData(String::from_utf8_lossy(&s.data).to_string())
            })?;

            Ok(Self {
                logger,
                uid: s.uid.clone(),
                frontend_id: ServerId::from(&req.frontend_id),
                frontend_session_id: s.id,
                rpc_client,
                discovery,
                is_frontend: false,
                data,
            })
        } else {
            Err(Error::SessionNotFound)
        }
    }

    /// Checks whether the session is bound.
    pub fn is_bound(&self) -> bool {
        !self.uid.is_empty()
    }

    /// Pushes a JSON encoded message to the connected client on the given session.
    ///
    /// The session has to be bound in order for the push to work.
    pub async fn push_json_msg(
        &self,
        route: impl ToString,
        msg: impl serde::Serialize,
    ) -> Result<(), Error> {
        if !self.is_bound() {
            return Err(Error::SessionNotBound);
        }

        self.push(protos::Push {
            route: route.to_string(),
            uid: self.uid.clone(),
            data: serde_json::to_vec(&msg).expect("serialize should not fail"),
        })
        .await
    }

    /// Pushes a protobuf encoded message to the connected client on the given session.
    ///
    /// The session has to be bound in order for the push to work.
    pub async fn push_protobuf_msg(
        &self,
        route: impl ToString,
        msg: impl prost::Message,
    ) -> Result<(), Error> {
        if !self.is_bound() {
            return Err(Error::SessionNotBound);
        }

        self.push(protos::Push {
            route: route.to_string(),
            uid: self.uid.clone(),
            data: utils::encode_proto(&msg),
        })
        .await
    }

    async fn push(&self, push_msg: protos::Push) -> Result<(), Error> {
        let frontend_server = match self
            .discovery
            .lock()
            .await
            .server_by_id(
                &self.frontend_id,
                // TODO(lhahn): this is not efficient. Ideallly we should know the server
                // type here, instead of providing None.
                None,
            )
            .await?
        {
            Some(s) => s,
            None => {
                error!(self.logger, "cannot bind session to unexistent frontend server"; "server_id" => %self.frontend_id.0);
                return Err(Error::Cluster(cluster::Error::FrontendServerNotFound(
                    self.frontend_id.clone(),
                )));
            }
        };

        let _ = self
            .rpc_client
            .push_to_user(frontend_server.kind.clone(), push_msg)
            .await?;

        Ok(())
    }

    pub async fn kick(&self) -> Result<(), Error> {
        if self.uid.is_empty() {
            return Err(Error::SessionNotBound);
        }

        let buf = utils::encode_proto(&protos::KickMsg {
            user_id: self.uid.clone(),
        });

        self.send_request_to_frontend(KICK_ROUTE, buf).await
    }

    pub async fn update_in_front(&self) -> Result<(), Error> {
        if self.is_frontend {
            return Err(Error::FrontendSession);
        }

        let session_data = utils::encode_proto(&protos::Session {
            id: self.frontend_session_id,
            uid: self.uid.to_string(),
            data: serde_json::to_vec(&self.data).expect("should not fail encoding"),
        });

        self.send_request_to_frontend(SESSION_PUSH_ROUTE, session_data)
            .await
    }

    pub async fn bind(&mut self, uid: impl ToString) -> Result<(), Error> {
        if !self.uid.is_empty() {
            return Err(Error::AlreadyBound);
        }
        self.uid = uid.to_string();

        let session_data = utils::encode_proto(&protos::Session {
            id: self.frontend_session_id,
            uid: self.uid.to_string(),
            data: vec![],
        });

        self.send_request_to_frontend(BIND_ROUTE, session_data)
            .await
    }

    pub fn set(&mut self, key: impl ToString, value: impl serde::Serialize) {
        let key = key.to_string();
        if let Ok(json_value) = serde_json::to_value(&value) {
            self.data.insert(key, json_value);
        } else {
            warn!(
                self.logger,
                "cannot set value on session that cannot be converted to json";
                "key" => %key,
            );
        }
    }

    async fn send_request_to_frontend(&self, route: &str, data: Vec<u8>) -> Result<(), Error> {
        let frontend_server = match self
            .discovery
            .lock()
            .await
            .server_by_id(
                &self.frontend_id,
                // TODO(lhahn): this is not efficient. Ideallly we should know the server
                // type here, instead of providing None.
                None,
            )
            .await?
        {
            Some(s) => s,
            None => {
                error!(self.logger, "cannot bind session to unexistent frontend server"; "server_id" => %self.frontend_id.0);
                return Err(Error::Cluster(cluster::Error::FrontendServerNotFound(
                    self.frontend_id.clone(),
                )));
            }
        };

        let res = self
            .rpc_client
            .call(
                Context::empty(), // TODO(lhahn): should this not be empty?
                protos::RpcType::User,
                message::Message {
                    kind: message::Kind::Request,
                    route: route.to_owned(),
                    data,
                    ..Default::default()
                },
                frontend_server,
            )
            .await?;

        if let Some(error) = res.error {
            error!(self.logger, "failed to send request to front"; "code" => error.code, "msg" => error.msg, "route" => route);
        } else {
            debug!(self.logger, "request to front complete"; "route" => route);
        }

        Ok(())
    }

    fn data_encoded(&self) -> String {
        serde_json::to_string(&self.data).expect("encoding should not fail")
    }
}
