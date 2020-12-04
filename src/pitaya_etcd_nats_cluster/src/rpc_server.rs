use crate::settings;
use async_trait::async_trait;
use futures::{future, StreamExt};
use nats::{self, asynk};
use pitaya_core::{
    cluster::{Error, Rpc, RpcServer, ServerInfo},
    metrics::{self},
    protos, utils,
};
use slog::{debug, error, info, o, trace, warn};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};

const RPCS_IN_FLIGHT_METRIC: &str = "rpcs_in_flight";

struct RpcServerState {
    connection: asynk::Connection,
    close_sender: oneshot::Sender<()>,
}

impl RpcServerState {
    async fn close(self) -> Result<(), Error> {
        let _ = self.close_sender.send(());
        self.connection.close().await.map_err(Error::Nats)
    }
}

type NatsRpcServerState = Arc<RwLock<Option<RpcServerState>>>;

pub struct NatsRpcServer {
    settings: settings::Nats,
    connection: NatsRpcServerState,
    this_server: Arc<ServerInfo>,
    runtime_handle: tokio::runtime::Handle,
    logger: slog::Logger,
    reporter: metrics::ThreadSafeReporter,
}

impl NatsRpcServer {
    pub fn new(
        logger: slog::Logger,
        this_server: Arc<ServerInfo>,
        settings: settings::Nats,
        runtime_handle: tokio::runtime::Handle,
        reporter: metrics::ThreadSafeReporter,
    ) -> Self {
        Self {
            settings,
            this_server,
            logger,
            connection: Arc::new(RwLock::new(None)),
            runtime_handle,
            reporter,
        }
    }

    fn on_nats_message(
        mut message: asynk::Message,
        logger: &slog::Logger,
        sender: &mpsc::Sender<Rpc>,
        runtime_handle: tokio::runtime::Handle,
        state: NatsRpcServerState,
    ) -> std::io::Result<()> {
        debug!(logger, "received nats message"; "message" => ?message);

        let mut sender = sender.clone();

        let (responder, response_receiver) = oneshot::channel();

        let response_topic = match message.reply.take() {
            Some(topic) => topic,
            None => {
                error!(logger, "received empty response topic from nats message");
                return Ok(());
            }
        };

        match sender.try_send(Rpc::new(message.data, responder)) {
            Ok(_) => {
                // For the moment we are ignoring the handle returned by the task.
                // Worst case scenario we will have to kill the task in the middle of its processing
                // at the end of the program.

                let _ = {
                    let logger = logger.clone();
                    // runtime.spawn(async move {
                    trace!(logger, "spawning response receiver task");
                    runtime_handle.spawn(async move {
                        match response_receiver.await {
                            Ok(response) => {
                                let conn = match state.read().await.as_ref() {
                                    Some(state) => state.connection.clone(),
                                    _ => {
                                        error!(logger, "connection not open, cannot answer");
                                        return;
                                    }
                                };

                                debug!(logger, "responding rpc");
                                if let Err(err) = Self::respond(&conn, &response_topic, response).await
                                {
                                    error!(logger, "failed to respond rpc"; "error" => %err);
                                }
                            }
                            Err(e) => {
                                // Errors happen here if the channel was closed before sending a message.
                                error!(logger, "failed to receive response from RPC"; "error" => %e);
                            }
                        }
                    })
                };
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                let _ = {
                    let logger = logger.clone();
                    runtime_handle.spawn(async move {
                        warn!(logger, "channel is full, dropping request");
                        let conn = match state.read().await.as_ref() {
                            Some(state) => state.connection.clone(),
                            _ => {
                                error!(logger, "connection not open, cannot answer");
                                return;
                            }
                        };

                        let response = utils::encode_proto(&protos::Response {
                            error: Some(protos::Error {
                                code: "PIT-503".to_string(),
                                msg: "server is overloaded".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                        if let Err(err) = Self::respond(&conn, &response_topic, response).await {
                            error!(logger, "failed to respond rpc"; "error" => %err);
                        }
                    })
                };
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(logger, "rpc channel stoped being listened");
            }
        };

        Ok(())
    }

    async fn respond(
        connection: &asynk::Connection,
        reply_topic: &str,
        res: Vec<u8>,
    ) -> Result<(), Error> {
        connection
            .publish(reply_topic, res)
            .await
            .map_err(Error::Nats)
    }

    async fn register_metrics(&self) {
        self.reporter
            .write()
            .await
            .register_gauge(metrics::Opts {
                kind: metrics::MetricKind::Gauge,
                namespace: String::from("pitaya"),
                subsystem: String::from("rpc"),
                name: String::from(RPCS_IN_FLIGHT_METRIC),
                help: String::from("number of in-flight RPCs at the moment"),
                variable_labels: vec![],
                buckets: None,
            })
            .expect("should not failed to register");
    }
}

#[async_trait]
impl RpcServer for NatsRpcServer {
    // Starts the server.
    async fn start(&self) -> Result<mpsc::Receiver<Rpc>, Error> {
        // Register relevant metrics.
        self.register_metrics().await;

        if self.connection.read().await.is_some() {
            warn!(self.logger, "nats rpc server was already started!");
            return Err(Error::RpcServerAlreadyStarted);
        }

        // TODO(lhahn): add callbacks here for sending metrics.
        info!(self.logger, "server connecting to nats"; "url" => &self.settings.url);
        let nats_connection =
            nats::Options::with_user_pass(&self.settings.auth_user, &self.settings.auth_pass)
                .max_reconnects(Some(self.settings.max_reconnection_attempts as usize))
                .connect_async(&self.settings.url)
                .await
                .map_err(Error::Nats)?;

        let (rpc_sender, rpc_receiver) = mpsc::channel(self.settings.max_rpcs_queued as usize);
        let (close_sender, close_receiver) = oneshot::channel();

        let topic = utils::topic_for_server(&self.this_server);
        let logger = self.logger.new(o!());

        info!(self.logger, "rpc server subscribing"; "topic" => &topic);

        let sender = rpc_sender;
        let runtime_handle = self.runtime_handle.clone();
        let connection = self.connection.clone();

        let subscription = nats_connection
            .subscribe(&topic)
            .await
            .map_err(Error::Nats)?;

        self.runtime_handle.spawn(future::select(
            subscription.for_each(move |message| {
                if let Err(e) = Self::on_nats_message(
                    message,
                    &logger,
                    &sender,
                    runtime_handle.clone(),
                    connection.clone(),
                ) {
                    error!(logger, "error consuming message"; "error" => %e);
                }
                future::ready(())
            }),
            close_receiver,
        ));

        self.connection.write().await.replace(RpcServerState {
            close_sender,
            connection: nats_connection,
        });

        Ok(rpc_receiver)
    }

    // Shuts down the server.
    async fn shutdown(&self) -> Result<(), Error> {
        if let Some(state) = self.connection.write().await.take() {
            let handle = self.runtime_handle.clone();
            // need to spawn a thread so it does not block the current runtime thread
            let th = std::thread::spawn(move || handle.block_on(state.close()));
            return th
                .join()
                .unwrap_or_else(|_| Err(Error::Internal("error joining thread".into())));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NatsRpcClient;
    use pitaya_core::{
        cluster::{RpcClient, ServerId, ServerKind},
        context, message,
    };
    use std::collections::HashMap;
    use std::error::Error as StdError;

    #[tokio::test]
    async fn server_starts_and_stops() -> Result<(), Box<dyn StdError>> {
        let sv = Arc::new(ServerInfo {
            id: ServerId::from("my-id"),
            kind: ServerKind::from("room"),
            metadata: HashMap::new(),
            frontend: false,
            hostname: "".to_owned(),
        });

        let rpc_server = NatsRpcServer::new(
            test_helpers::get_root_logger(),
            sv.clone(),
            Default::default(),
            tokio::runtime::Handle::current(),
            Arc::new(RwLock::new(Box::new(metrics::DummyReporter {}))),
        );

        let mut rpc_server_conn = rpc_server.start().await?;

        let handle = {
            tokio::spawn(async move {
                while let Some(rpc) = rpc_server_conn.recv().await {
                    let res = utils::encode_proto(&protos::Response {
                        data: b"HEY, THIS IS THE SERVER".to_vec(),
                        error: None,
                    });
                    if !rpc.respond(res) {
                        panic!("failed to respond rpc");
                    }
                }
            })
        };

        {
            let client = NatsRpcClient::new(
                test_helpers::get_root_logger(),
                Default::default(),
                sv.clone(),
                tokio::runtime::Handle::current(),
                Arc::new(RwLock::new(Box::new(metrics::DummyReporter {}))),
            );
            client.start().await?;

            let res = client
                .call(
                    context::Context::empty(),
                    protos::RpcType::User,
                    message::Message {
                        kind: message::Kind::Request,
                        id: 12,
                        data: b"sending some data".to_vec(),
                        route: "room.room.join".to_owned(),
                        compressed: false,
                        err: false,
                    },
                    sv.clone(),
                )
                .await?;

            assert_eq!(
                String::from_utf8_lossy(&res.data),
                "HEY, THIS IS THE SERVER"
            );

            let res = client
                .call(
                    context::Context::empty(),
                    protos::RpcType::User,
                    message::Message {
                        kind: message::Kind::Request,
                        id: 12,
                        data: b"sending some data".to_vec(),
                        route: "room.room.join".to_owned(),
                        compressed: false,
                        err: false,
                    },
                    sv.clone(),
                )
                .await?;

            assert_eq!(
                String::from_utf8_lossy(&res.data),
                "HEY, THIS IS THE SERVER"
            );

            client.shutdown().await?;
        }

        rpc_server.shutdown().await?;
        handle.await?;
        Ok(())
    }
}
