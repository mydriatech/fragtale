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

//! Simplified example of how to use [EventClient].

extern crate fragtale_client;
extern crate tokio;

use fragtale_client::EventClient;
use fragtale_client::EventProcessor;
use fragtale_client::EventSource;
use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use tokio::signal::unix::{SignalKind, signal};

fn main() {
    if let Some(api_base_url) = env::args().skip(1).next() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap()
            .block_on(run(api_base_url));
    } else {
        println!(
            "
Missing API base URL. Run with:

    BEARER_TOKEN_FILENAME=/path/to/bearer_token cargo run --example event_handler -- http://127.0.0.1:8081/api/v1
"
        );
    }
}

async fn run(api_base_url: String) {
    let event_processor = Arc::new(Application::default());
    EventClient::connect(
        &api_base_url,
        "klingon_attacks_topic",
        "starfleet_wins_topic",
        Box::new(event_processor),
        1,
    )
    .await;
    // Await user termination
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = sigint.recv() => {},
    };
}

/// Event driven example app.
#[derive(Default)]
pub struct Application {
    /// A simple counter
    unique_sequence_generator: AtomicUsize,
}

#[async_trait::async_trait]
impl EventProcessor for Application {
    async fn process_message(
        &self,
        topic_id: String,
        event_document: String,
        event_source: &dyn EventSource,
    ) -> Option<String> {
        match topic_id.as_str() {
            "klingon_attacks_topic" => {
                // Do something with the event
                println!("event_document: '{event_document}'");
                let mut klingon_kill_count = 1;
                // Query indexed events as a document database
                let event_ids = event_source
                    .event_ids_by_indexed_column("starfleet_captains", "user_id", "kirk")
                    .await;
                // Get latest event doc if there are multiple updates matched by the index
                if let Some(event_id) = event_ids.first() {
                    let event_document = event_source
                        .event_by_topic_and_event_id("starfleet_captains", event_id)
                        .await
                        .expect("Unexpected: Event was deleted after it's identifier was found.");
                    // In this example: the event document JSON consists of a single String.
                    let starfleet_captain_status = serde_json::from_str::<String>(&event_document).expect("Unexpected: Event document was ingested by message broker dispite being broken JSON.");
                    if starfleet_captain_status.eq("on_duty") {
                        klingon_kill_count += 1;
                    }
                }
                // Publish a new event as a result of this operation
                Some(
                    serde_json::json!({
                        "ts": fragtale_client::time::get_timestamp_micros().to_string(),
                        "count": self
                        .unique_sequence_generator
                        .fetch_add(klingon_kill_count, Relaxed)
                    })
                    .to_string(),
                )
            }
            _ => {
                println!("You will never see this, since you only subscribed to a single topic.");
                None
            }
        }
    }

    fn post_subscribed_hook(&self, topic_id: &str) {
        println!("Application will now recieve events published to topic '{topic_id}'.");
    }
}
