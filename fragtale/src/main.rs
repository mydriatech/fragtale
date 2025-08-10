/*
    Copyright 2025 MydriaTech AB

    Licensed under the Apache License 2.0 with Free world makers exception
    1.0.0 (the "License"); you may not use this file except in compliance with
    the License. You should have obtained a copy of the License with the source
    or binary distribution in file named

        LICENSE-Apache-2.0-with-FWM-Exception-1.0.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub use fragtale_core::conf::AppConfig;
pub use fragtale_core::mb::MessageBroker;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;

/// Application main entrypoint.
fn main() -> ExitCode {
    let startup_ts_micros = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("System time is apparently before unix epoch time which is highly unexpected.")
            .as_micros(),
    )
    .expect("Current epoch time in microseconds did not fit inside a 64-bit unsigned.");
    if let Err(e) = init_logger() {
        println!("Failed to initialize configuration: {e:?}");
        return ExitCode::FAILURE;
    }
    #[cfg(feature = "tracing")]
    {
        // Enable tracing via the RUST_LOG environment variable. Example:
        //
        // ```
        // RUST_LOG="cdrs_tokio=trace,cassandra-protocol=trace,cdrs-tokio-helpers-derive=trace"
        // ```
        let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
            .with_writer(non_blocking)
            .init();
    }
    let app_config = Arc::new(AppConfig::new(env!("CARGO_PKG_NAME"), startup_ts_micros));
    if app_config.limits.cpus() > 0.0 {
        // Defaults to using one thread per core when no limit is set.
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(app_config.limits.available_parallelism())
            .build()
            .unwrap()
            .block_on(run_async(app_config))
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_async(app_config))
    }
}

/// Initialize the logging system and apply filters.
fn init_logger() -> Result<(), log::SetLoggerError> {
    env_logger::builder()
        // Set default log level
        .filter_level(log::LevelFilter::Debug)
        //.filter_level(log::LevelFilter::Trace)
        // Customize logging for dependencies
        .filter(Some("actix_server::builder"), log::LevelFilter::Warn)
        .filter(Some("actix_http::h1"), log::LevelFilter::Debug)
        .filter(Some("mio::poll"), log::LevelFilter::Debug)
        //.filter(Some("ureq"), log::LevelFilter::Info)
        .filter(Some("rustls::client"), log::LevelFilter::Info)
        .filter(
            Some("fragtale_dbp_cassandra::cassandra_provider"),
            log::LevelFilter::Debug,
        )
        .filter(Some("fragtale::mb::correlation"), log::LevelFilter::Info)
        .filter(
            Some("fragtale::mb::event_descriptor_cache"),
            log::LevelFilter::Info,
        )
        .filter(Some("fragtale::mb"), log::LevelFilter::Debug)
        .filter(
            Some("fragtale::rest_api::ws_resources"),
            log::LevelFilter::Info,
        )
        .filter(
            Some("fragtale::mb::consumers::consumer_delivery_cache"),
            log::LevelFilter::Debug,
        )
        //.write_style(env_logger::fmt::WriteStyle::Never)
        .write_style(env_logger::fmt::WriteStyle::Auto)
        .target(env_logger::fmt::Target::Stdout)
        .is_test(false)
        .parse_env(
            env_logger::Env::new()
                .filter("LOG_LEVEL")
                .write_style("LOG_STYLE"),
        )
        .try_init()
}

/// Async code entry point.
pub async fn run_async(app_config: Arc<AppConfig>) -> ExitCode {
    let mb = MessageBroker::new(&app_config).await;
    let liveness_failsafe_future = mb.liveness_failsafe();
    let app_future = fragtale_api::rest_api::run_http_server(&app_config, &mb);
    let signals_future = block_until_signaled();
    let res = tokio::select! {
        res = liveness_failsafe_future => {
            log::trace!("liveness_failsafe_future finished");
            res
        },
        res = app_future => {
            log::trace!("app_future finished");
            res
        },
        _ = signals_future => {
            log::trace!("signals_future finished");
            Ok(())
        },
    }
    .map_err(|e| log::error!("{e}"));
    mb.exit_hook().await;
    if res.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Block until SIGTERM or SIGINT is recieved.
async fn block_until_signaled() {
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigterm.recv() => {
            log::debug!("SIGTERM recieved.")
        },
        _ = sigint.recv() => {
            log::debug!("SIGINT recieved.")
        },
    };
}
