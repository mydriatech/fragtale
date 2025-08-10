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

//! CLI for Fragtale.

use fragtale_client::RestApiClient;
use std::process::ExitCode;

/// Basic CLI that can be extended later.
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    if let Err(e) = init_logger() {
        println!("Failed to initialize logging: {e}");
        return ExitCode::FAILURE;
    }
    let mut args = std::env::args();
    let app_version = "todo";
    let cli_name = args.next().unwrap_or_default();
    if let Some(api_base_url) = args.next() {
        let client = RestApiClient::new(&api_base_url, &cli_name, app_version, 1).await;

        match args.next().as_deref() {
            Some("event") => {
                if let Some(topic_id) = args.next()
                    && let Some(event_id) = args.next()
                {
                    return event_by_topic_and_event_id(&client, &topic_id, &event_id).await;
                }
            }
            Some(_other) => {}
            None => {}
        }
    }
    println!(
        "{cli_name} - Ceso REST CLI

Usage:
    {cli_name} [base_url] event [topic_id] [event_id]

Example
    {cli_name} http://fragtale.localdomain/api/v1 event test_topic 66d67d1f750017ae4ebc1cdd4b4b031f8a7300afd4485c30bae846886cb7a275107f405f4e3db0e9349d879629f3b9802b23e9588b4e8ee9c31fdf22e62d19b7
    "
    );
    ExitCode::FAILURE
}

fn init_logger() -> Result<(), log::SetLoggerError> {
    env_logger::builder()
        // Set default log level
        .filter_level(log::LevelFilter::Info)
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

#[allow(unused_variables)]
async fn event_by_topic_and_event_id(
    client: &RestApiClient,
    topic_id: &str,
    event_id: &str,
) -> ExitCode {
    let event_opt = client.event_by_topic_and_event_id(topic_id, event_id).await;
    if let Some(document) = event_opt {
        // Show relevant info
        log::info!("Event document: '{document}'");
        return ExitCode::SUCCESS;
    }
    log::warn!("Failed to retrieve event!");
    ExitCode::FAILURE
}
