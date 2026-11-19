//! Operator command-line modes and the account-quota dry run.
//! One-shot commands share CLI parsing but do not enter the long-running serving loop.

use std::{ffi::OsString, path::PathBuf};

use crate::{
    account_quota, config_convert, config_overrides, print_manifest, quota_consumer::QuotaConsumer,
    route_transport::run_route_handler_off_async_workers, routing_clock::now_ms, run, shutdown,
    startup_settings::ModuleConfig, ModuleError, SharedStore, DEFAULT_MODULE_ID,
};

pub(super) async fn async_main(
    shutdown: &shutdown::Shutdown,
    store: &mut Option<SharedStore>,
) -> Result<(), ModuleError> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "config-convert") {
        if args.len() != 3 {
            return Err(ModuleError::Message(format!(
                "usage: {} <input> <output>",
                config_convert::COMMAND
            )));
        }
        let input = std::fs::read_to_string(&args[1])
            .map_err(|error| ModuleError::Message(error.to_string()))?;
        let output = config_convert::convert(&input).map_err(ModuleError::Message)?;
        config_overrides::validate_document(&output).map_err(ModuleError::Message)?;
        std::fs::write(&args[2], output)
            .map_err(|error| ModuleError::Message(error.to_string()))?;
        return Ok(());
    }
    // Read the launch nonce before anything else can spawn a process: until
    // this first read, the pipe the daemon handed it over on is inheritable,
    // so a child spawned earlier would receive it. The result is cached for
    // the process; HELLO and the credential route read that cached answer.
    let _ = subc_os::launch_nonce();
    let (connection_file_path, managed_settings) = match parse_startup_mode(args)? {
        StartupMode::PrintManifest => return print_manifest(),
        StartupMode::CheckConfig => {
            let path = config_overrides::config_path();
            let report = config_overrides::check_config_offline(
                &path,
                config_overrides::default_store_path().as_deref(),
            )
            .map_err(ModuleError::Message)?;
            eprintln!("config check passed: {}\n{report}", path.display());
            return Ok(());
        }
        StartupMode::PrintConfigSchema => {
            print!("{}", config_overrides::routing_config_schema());
            return Ok(());
        }
        StartupMode::AccountQuotaDryRun {
            connection_file_path,
            capture_dir,
        } => return account_quota_dry_run(connection_file_path, capture_dir).await,
        StartupMode::Serve {
            connection_file_path,
            managed_settings,
        } => (connection_file_path, managed_settings),
    };
    let config = ModuleConfig::from_env(connection_file_path, managed_settings)?;
    let result = run(config, shutdown, store).await;
    shutdown.begin();
    result
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StartupMode {
    PrintManifest,
    CheckConfig,
    PrintConfigSchema,
    /// Read Insula once as an ordinary consumer and print the account views it
    /// would produce. Writes no store and consults no vault.
    AccountQuotaDryRun {
        connection_file_path: PathBuf,
        capture_dir: Option<PathBuf>,
    },
    Serve {
        connection_file_path: PathBuf,
        managed_settings: bool,
    },
}

/// `--account-quota-dry-run --subc <file> [--capture <dir>]`: fetch Insula's
/// unfiltered reply, optionally save its exact bytes under
/// `<dir>/usage-get-all.json`, and print the account report as JSON.
async fn account_quota_dry_run(
    connection_file_path: PathBuf,
    capture_dir: Option<PathBuf>,
) -> Result<(), ModuleError> {
    let project_root =
        std::env::temp_dir().join(format!("{DEFAULT_MODULE_ID}-account-quota-dry-run"));
    let _ = std::fs::create_dir_all(&project_root);
    let mut consumer = QuotaConsumer::connect(&connection_file_path, &project_root)
        .await
        .map_err(|error| ModuleError::Message(format!("connect to insula: {error}")))?;
    let body = consumer
        .usage_get_raw()
        .await
        .map_err(|error| ModuleError::Message(format!("usage.get: {error}")))?;
    let mut state = account_quota::AccountQuotaState {
        roster_poll: account_quota::RosterPoll::NotConsulted {
            reason: "the dry run does not consult the vault: it runs without routing's reserved principal"
                .into(),
        },
        ..Default::default()
    };
    let received_at_ms = now_ms() as i64;
    if let Some(dir) = capture_dir {
        capture_account_quota_reply(dir, body.clone()).await?;
    }
    account_quota::apply_usage_reply(&mut state, &body, received_at_ms)
        .map_err(ModuleError::Message)?;
    let status = account_quota::status_json(&state, now_ms() as i64);
    println!(
        "{}",
        serde_json::to_string_pretty(&status).map_err(ModuleError::Json)?
    );
    Ok(())
}

pub(super) async fn capture_account_quota_reply(
    dir: PathBuf,
    body: Vec<u8>,
) -> Result<(), ModuleError> {
    // Use the route-handler bridge so filesystem waits cannot stall an async
    // worker, including when the caller uses a current-thread runtime.
    run_route_handler_off_async_workers(async move {
        std::fs::create_dir_all(&dir)
            .and_then(|()| std::fs::write(dir.join("usage-get-all.json"), &body))
            .map_err(|error| ModuleError::Message(format!("capture usage: {error}")))
    })
    .await
}

pub(super) fn parse_startup_mode(
    args: impl IntoIterator<Item = OsString>,
) -> Result<StartupMode, ModuleError> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--check-config") {
        return if args.as_slice() == [OsString::from("--check-config")] {
            Ok(StartupMode::CheckConfig)
        } else {
            Err(ModuleError::Message(
                "--check-config must be used without other arguments".into(),
            ))
        };
    }
    if args.as_slice() == [OsString::from("--print-config-schema")] {
        return Ok(StartupMode::PrintConfigSchema);
    }
    if args.iter().any(|arg| arg == "--print-config-schema") {
        return Err(ModuleError::Message(
            "--print-config-schema must be used without other arguments".into(),
        ));
    }
    if args.as_slice() == [OsString::from("--print-manifest")] {
        return Ok(StartupMode::PrintManifest);
    }
    if args.iter().any(|arg| arg == "--print-manifest") {
        return Err(ModuleError::Message(
            "--print-manifest must be used without other arguments".into(),
        ));
    }
    if args.iter().any(|arg| arg == "--account-quota-dry-run") {
        let capture_dir = args
            .iter()
            .position(|arg| arg == "--capture")
            .map(|index| {
                args.get(index + 1)
                    .map(PathBuf::from)
                    .ok_or_else(|| ModuleError::Message("--capture requires a directory".into()))
            })
            .transpose()?;
        return parse_subc_arg(args).map(|connection_file_path| StartupMode::AccountQuotaDryRun {
            connection_file_path,
            capture_dir,
        });
    }
    let managed_settings = args.iter().any(|arg| arg == "--managed-settings");
    parse_subc_arg(args).map(|connection_file_path| StartupMode::Serve {
        connection_file_path,
        managed_settings,
    })
}

fn parse_subc_arg(args: impl IntoIterator<Item = OsString>) -> Result<PathBuf, ModuleError> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--subc" {
            let value = args
                .next()
                .ok_or_else(|| ModuleError::Message("--subc requires a value".into()))?;
            return Ok(PathBuf::from(value));
        }
        if let Some(raw) = arg.to_str().and_then(|a| a.strip_prefix("--subc=")) {
            if raw.is_empty() {
                return Err(ModuleError::Message("--subc= requires a value".into()));
            }
            return Ok(PathBuf::from(raw));
        }
    }
    Err(ModuleError::Message(
        "missing required --subc <connection-file> argument".into(),
    ))
}
