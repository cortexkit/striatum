//! Authenticate as routing and bind the EVENT durable provisioned by ck-bus.
use std::{path::PathBuf, sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::{ConnectConfig, NatsConnection, NatsStreamCursor};
use cortexkit_bus_trait::{DurableOwner, Stream};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{CallOptions, ConsumerIdentity, ConsumerOptions, SubcConsumer};
use subc_protocol::{BindIdentity, RouteTarget};

const TIMEOUT: Duration = Duration::from_secs(5);

struct CredentialRoute {
    consumer: SubcConsumer,
    identity: BindIdentity,
    principal: ConsumerIdentity,
}

impl CredentialRoute {
    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let reply = self
            .consumer
            .call(
                RouteTarget::ManagementSurface {
                    module_id: "ckbus".into(),
                },
                self.identity.clone(),
                serde_json::to_vec(&json!({"method": method, "params": params}))
                    .map_err(|e| e.to_string())?,
                CallOptions {
                    timeout: TIMEOUT,
                    consumer_identity: Some(self.principal.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "ckbus credential operation unavailable".to_owned())?;
        let value: Value = serde_json::from_slice(&reply).map_err(|_| "invalid ckbus response")?;
        value
            .get("result")
            .cloned()
            .ok_or_else(|| "ckbus response lacks result".into())
    }
}

#[derive(Deserialize)]
struct Credential {
    jwt: String,
    exp: i64,
    acct: String,
    inbox_prefix: String,
    server_url: String,
    credential_public: String,
    spawn_generation: u64,
    credential_epoch: u64,
}

#[derive(Deserialize)]
struct Renewal {
    jwt: String,
    exp: i64,
    credential_public: String,
    spawn_generation: u64,
    credential_epoch: u64,
}

impl Credential {
    fn apply_renewal(&mut self, renewal: Renewal) -> Result<(), String> {
        if renewal.credential_public != self.credential_public
            || renewal.spawn_generation != self.spawn_generation
            || renewal.credential_epoch != self.credential_epoch
            || renewal.exp <= self.exp
        {
            return Err("ckbus renewal identity or expiry mismatch".into());
        }
        self.jwt = renewal.jwt;
        self.exp = renewal.exp;
        Ok(())
    }
}

pub(super) struct AttachedBus {
    pub connection: NatsConnection,
    pub cursor: NatsStreamCursor,
    pub subject: String,
    pub stream: String,
    pub renew_at_ms: u64,
}

pub(super) async fn attach(path: PathBuf) -> Result<AttachedBus, String> {
    tokio::time::timeout(TIMEOUT, attach_inner(path))
        .await
        .map_err(|_| "quota bus attach timeout".to_owned())?
}

async fn attach_inner(path: PathBuf) -> Result<AttachedBus, String> {
    let nonce = subc_client_rs::launch_nonce()
        .map_err(|_| "launch nonce unreadable")?
        .ok_or("routing launch nonce absent")?;
    let consumer = SubcConsumer::connect(&path, ConsumerOptions::default())
        .await
        .map_err(|_| "ckbus route unavailable")?;
    let route = Arc::new(CredentialRoute {
        consumer,
        identity: BindIdentity::new(
            path.parent().unwrap_or(std::path::Path::new(".")),
            super::DEFAULT_MODULE_ID,
            "bus",
        ),
        principal: ConsumerIdentity {
            module_id: super::DEFAULT_MODULE_ID.into(),
            launch_nonce: nonce.value().to_owned(),
        },
    });
    let mut credential: Credential =
        serde_json::from_value(route.call("ckbus.credential", json!({})).await?)
            .map_err(|_| "invalid ckbus credential")?;
    let names = AccountNames::derive(&credential.acct).map_err(|e| e.to_string())?;
    let config = ConnectConfig::new(&credential.credential_public).map_err(|e| e.to_string())?;
    // The local signer must never send a bearer JWT to an untrusted remote listener.
    let port = credential
        .server_url
        .strip_prefix("nats://127.0.0.1:")
        .and_then(|p| p.parse::<u16>().ok())
        .filter(|p| *p != 0);
    if port.is_none() || config.inbox_prefix() != credential.inbox_prefix {
        return Err("invalid ckbus endpoint, inbox, or credential expiry".into());
    }
    // `ckbus.credential` always mints a fresh login (and supersedes routing's
    // previous one), so this renewal is only a guard against a reply that is
    // already close to expiry; nothing here relies on getting an existing login back.
    if credential.exp <= (super::now_ms() / 1000) as i64 + 300 {
        let renewal = route
            .call(
                "ckbus.credential_renew",
                json!({
                    "credential_public": credential.credential_public,
                }),
            )
            .await?;
        credential
            .apply_renewal(serde_json::from_value(renewal).map_err(|_| "invalid ckbus renewal")?)?;
    }
    if credential.exp <= (super::now_ms() / 1000) as i64 + 60 {
        return Err("ckbus credential expires too soon".into());
    }
    let key = credential.credential_public;
    let jwt = credential.jwt;
    let options = async_nats::ConnectOptions::with_auth_callback(move |nonce| {
        let route = Arc::clone(&route);
        let key = key.clone();
        let jwt = jwt.clone();
        async move {
            let reply = route
                .call(
                    "ckbus.nonce_sign",
                    json!({
                        "credential_public": key, "nonce_b64": STANDARD.encode(nonce),
                    }),
                )
                .await
                .map_err(async_nats::AuthError::new)?;
            let signature = reply["signature_b64"]
                .as_str()
                .ok_or_else(|| async_nats::AuthError::new("ckbus signature absent"))?;
            let signature = STANDARD
                .decode(signature)
                .map_err(|_| async_nats::AuthError::new("invalid ckbus signature"))?;
            if signature.len() != 64 {
                return Err(async_nats::AuthError::new("invalid ckbus signature length"));
            }
            let mut auth = async_nats::Auth::new();
            auth.jwt = Some(jwt);
            auth.signature = Some(signature);
            Ok(auth)
        }
    })
    .max_reconnects(0)
    .connection_timeout(TIMEOUT);
    let connection = NatsConnection::connect(&credential.server_url, options, config)
        .await
        .map_err(|_| "quota bus connection unavailable")?;
    let stream = names.streams().event.clone();
    // Stream::consumer reads CONSUMER.INFO; it does not create or reconfigure a durable.
    let cursor = connection
        .stream(&stream)
        .consumer(DurableOwner::Module(super::DEFAULT_MODULE_ID))
        .await
        .map_err(|e| format!("quota durable attach: {e}"))?;
    Ok(AttachedBus {
        connection,
        cursor,
        subject: names
            .event_subject("insula", "quota_sampled", 1)
            .map_err(|e| e.to_string())?,
        stream,
        // Reconnect through issuance/renewal before the authenticated JWT expires.
        renew_at_ms: (credential.exp as u64 - 60) * 1000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential() -> Credential {
        serde_json::from_value(json!({
            "jwt": "old", "exp": 2000000000_i64, "acct": "box_test",
            "inbox_prefix": "_INBOX.UFIXTURE", "server_url": "nats://127.0.0.1:4222",
            "credential_public": "UFIXTURE", "spawn_generation": 7, "credential_epoch": 0,
        }))
        .unwrap()
    }

    fn renewal() -> Value {
        json!({"jwt": "renewed", "exp": 2000003600_i64, "credential_public": "UFIXTURE",
            "spawn_generation": 7, "credential_epoch": 0})
    }

    #[test]
    fn renewal_keeps_identity_and_advances_jwt_expiry() {
        let mut credential = credential();
        credential
            .apply_renewal(serde_json::from_value(renewal()).unwrap())
            .unwrap();
        assert_eq!(credential.jwt, "renewed");
        assert_eq!(credential.exp, 2000003600);
    }

    #[test]
    fn renewal_rejects_changed_identity_and_nonadvancing_expiry() {
        for (field, value) in [
            ("credential_public", json!("OTHER")),
            ("spawn_generation", json!(8)),
            ("credential_epoch", json!(1)),
            ("exp", json!(2000000000)),
        ] {
            let mut credential = credential();
            let mut changed = renewal();
            changed[field] = value;
            assert!(
                credential
                    .apply_renewal(serde_json::from_value(changed).unwrap())
                    .is_err(),
                "{field}"
            );
            assert_eq!(credential.jwt, "old");
        }
    }
}
