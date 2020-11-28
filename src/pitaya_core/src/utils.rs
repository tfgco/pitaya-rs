use crate::{
    cluster::{ServerInfo, ServerKind},
    constants, context, message, protos, Error,
};
use rand::Rng;
use std::sync::Arc;

pub fn user_kick_topic(user_id: &str, server_kind: &ServerKind) -> String {
    format!("pitaya/{}/user/{}/kick", server_kind.0, user_id)
}

pub fn user_messages_topic(user_id: &str, server_kind: &ServerKind) -> String {
    format!("pitaya/{}/user/{}/push", server_kind.0, user_id)
}

pub fn topic_for_server(server: &ServerInfo) -> String {
    format!("pitaya/servers/{}/{}", server.kind.0, server.id.0)
}

pub fn server_kind_prefix(server_kind: &ServerKind) -> String {
    format!("pitaya/servers/{}/", server_kind.0)
}

pub fn random_server(servers: &[Arc<ServerInfo>]) -> Option<Arc<ServerInfo>> {
    if servers.is_empty() {
        None
    } else {
        let mut rng = rand::thread_rng();
        let random_index: usize = rng.gen_range(0, servers.len());
        Some(servers[random_index].clone())
    }
}

pub fn encode_proto<P>(msg: &P) -> Vec<u8>
where
    P: prost::Message,
{
    let mut b = Vec::with_capacity(msg.encoded_len());
    // NOTE(lhahn): An error is only returned here if the buffer does not have enough capacity
    // therefore we can safely ignore it
    msg.encode(&mut b).expect("failed to encode proto message");
    b
}

pub fn build_error_response<S, T>(code: S, msg: T) -> Vec<u8>
where
    S: ToString,
    T: ToString,
{
    encode_proto(&protos::Response {
        error: Some(protos::Error {
            code: code.to_string(),
            msg: msg.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })
}

pub fn build_request(
    mut ctx: context::Context,
    rpc_type: protos::RpcType,
    msg: message::Message,
    // consider adding a session here in the future.
    server_info: Arc<ServerInfo>,
) -> Result<protos::Request, Error> {
    if ctx
        .add(constants::PEER_ID_KEY, &server_info.id.0)
        .expect("should not fail")
    {
        return Err(Error::ContextKeyCollistion(
            constants::PEER_ID_KEY.to_string(),
        ));
    }

    if ctx
        .add(constants::PEER_SERVICE_KEY, &server_info.kind.0)
        .expect("should not fail")
    {
        return Err(Error::ContextKeyCollistion(
            constants::PEER_SERVICE_KEY.to_string(),
        ));
    }

    let req = protos::Request {
        r#type: rpc_type as i32,
        msg: Some(protos::Msg {
            r#type: if msg.kind == message::Kind::Request {
                protos::MsgType::MsgRequest as i32
            } else {
                protos::MsgType::MsgNotify as i32
            },
            data: msg.data,
            route: msg.route,
            ..protos::Msg::default()
        }),
        frontend_id: if server_info.frontend {
            server_info.id.0.clone()
        } else {
            String::new()
        },
        metadata: ctx.into(),
        ..protos::Request::default()
    };

    Ok(req)
}
