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

//! Session (connection) to the Cassandra database.

use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::NodeAddress;
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::cluster::session::SessionBuildError;
use cdrs_tokio::cluster::session::SessionBuilder;
use cdrs_tokio::cluster::session::TcpSessionBuilder;
use cdrs_tokio::frame::events::SchemaChange;
use cdrs_tokio::frame::events::ServerEvent;
use cdrs_tokio::frame::message_response::ResponseBody;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::transport::TransportTcp;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::time::{Duration, sleep};

/// Listener to Cassandra server schema change events.
pub trait CassandraSchemaChangeListener: Sync + Send {
    /// Invoked for each recieved Cassandra server schema change event.
    fn handle_schema_change(&self, schema_change: &SchemaChange);
}

/// Session (connection) to the Cassandra database.
pub struct CassandraSession {
    /// Connection to Cassandra.
    session: Arc<
        Session<
            TransportTcp,
            TcpConnectionManager,
            RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
        >,
    >,
    schema_change_listener_count: AtomicUsize,
    schema_change_listeners: Arc<SkipMap<usize, Arc<dyn CassandraSchemaChangeListener>>>,
    replication_factor: usize,
}

impl CassandraSession {
    /// Open up a new session to the Cassandra database service and initialize
    /// server side event dispatch.
    pub async fn connect(
        endpoints: &[String],
        username: &str,
        password: &str,
        replication_factor: usize,
    ) -> Arc<Self> {
        let session = Arc::new(
            Self::create_session(endpoints, username, password)
                .await
                .map_err(|e| {
                    log::info!("Failed to create session to {endpoints:?}: {e:?}");
                })
                .unwrap(),
        );
        Arc::new(Self {
            session,
            schema_change_listener_count: AtomicUsize::default(),
            schema_change_listeners: Arc::new(SkipMap::default()),
            replication_factor,
        })
        .init()
        .await
    }

    /// Initialize
    async fn init(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.handle_server_events().await });
        self
    }

    /// Add a [CassandraSchemaChangeListener] that will recieve server events.
    pub fn attach_schema_change_listener(
        &self,
        schema_change_listener: &Arc<dyn CassandraSchemaChangeListener>,
    ) {
        let index = self
            .schema_change_listener_count
            .fetch_add(1, Ordering::Relaxed);
        self.schema_change_listeners
            .insert(index, Arc::clone(schema_change_listener));
    }

    /// Recieve and dispatch server side events from Cassandra.
    async fn handle_server_events(&self) {
        let mut server_event_receiver = self.session.create_event_receiver();
        while let Ok(server_event) = server_event_receiver.recv().await {
            match server_event {
                ServerEvent::TopologyChange(toplogy_change) => {
                    log::debug!("TopologyChange: {toplogy_change:?}");
                }
                ServerEvent::StatusChange(status_change) => {
                    log::debug!("StatusChange: {status_change:?}");
                }
                ServerEvent::SchemaChange(schema_change) => {
                    log::debug!("SchemaChange: {schema_change:?}");
                    self.schema_change_listeners.as_ref().iter().for_each(
                        |schema_change_listener| {
                            schema_change_listener
                                .value()
                                .handle_schema_change(&schema_change)
                        },
                    );
                }
                _ => {
                    log::debug!("Unsupported server event received: {server_event:?}");
                }
            }
        }
        log::debug!("handle_server_events terminated");
    }

    /// Open up a new session to the Cassandra database service.
    async fn create_session(
        endpoints: &[String],
        username: &str,
        password: &str,
    ) -> Result<
        Session<
            TransportTcp,
            TcpConnectionManager,
            RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
        >,
        SessionBuildError,
    > {
        log::info!("Connecting to Cassandra cluster as '{username}'.");
        let endpoints: Vec<NodeAddress> = endpoints.iter().map(|x| x.into()).collect();
        let authenticator_provider =
            Arc::new(StaticPasswordAuthenticatorProvider::new(username, password));
        let cluster_config = NodeTcpConfigBuilder::new()
            .with_authenticator_provider(authenticator_provider)
            .with_contact_points(endpoints.clone())
            .with_version(cdrs_tokio::frame::Version::V5)
            .build()
            .await
            .map_err(|e| {
                log::info!("Failed to connect to {endpoints:?}: {e:?}");
            })
            .unwrap();
        let ret = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
            .build()
            .await;
        if ret.is_ok() {
            log::info!("Connected to Cassandra cluster.");
        }
        ret
    }

    /// Execute raw keyspaced query using this session.
    pub async fn query_raw(&self, query_template: &str, keyspace: &str) -> ResponseBody {
        log::debug!("Running '{query_template}' with keyspace '{keyspace}'.");
        Arc::clone(&self.session)
            .query(&query_template.replace("{{ keyspace }}", keyspace))
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to execute query '{query_template}' in keyspace '{keyspace}': {e:?}")
            })
            .response_body()
            .expect("get body")
    }

    /// Execute keyspaced query with value parameters using this session.
    pub async fn query_with_keyspace_and_values(
        &self,
        query_template: &str,
        keyspace: &str,
        values: QueryValues,
    ) -> Option<ResponseBody> {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Running '{query_template}' in keyspace '{keyspace}'.");
        }
        loop {
            let mut parameters = StatementParamsBuilder::new();
            let consistency = match self.replication_factor {
                1 => cdrs_tokio::consistency::Consistency::One,
                2 => cdrs_tokio::consistency::Consistency::Two,
                _ => cdrs_tokio::consistency::Consistency::Quorum,
            };
            if query_template.contains("IF ") {
                parameters = parameters
                    .with_consistency(consistency)
                    .with_flags(cdrs_tokio::query::QueryFlags::WITH_SERIAL_CONSISTENCY)
                    .with_serial_consistency(cdrs_tokio::consistency::Consistency::Serial)
                    // Don't hide errors for Paxos ops
                    .with_retry_policy(Arc::new(cdrs_tokio::retry::FallthroughRetryPolicy));
            } else {
                parameters = parameters.with_consistency(consistency);
            }
            let query_template = if query_template.contains("{{ keyspace }}") {
                &query_template.replace("{{ keyspace }}", keyspace)
            } else {
                parameters = parameters.with_keyspace(keyspace.to_string());
                query_template
            };
            //let parameters = StatementParamsBuilder::new()
            parameters = parameters
                //.with_consistency(cdrs_tokio::consistency::Consistency::Quorum)
                .with_values(values.clone());
            let result = Arc::clone(&self.session)
                .query_with_params(query_template, parameters.build())
                .await;
            if let Err(ref e) = result {
                match e {
                    /*
                    https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec states
                    ```
                    0x1700    CAS_WRITE_UNKNOWN: An exception occured due to contended Compare And Set write/update.
                              The CAS operation was only partially completed and the operation may or may not get completed by
                              the contending CAS write or SERIAL/LOCAL_SERIAL read. The rest of the ERROR message body will be
                                <cl><received><blockfor>
                              where:
                                <cl> is the [consistency] level of the query having triggered
                                     the exception.
                                <received> is an [int] representing the number of nodes having
                                           acknowledged the request.
                                <blockfor> is an [int] representing the number of replicas whose
                                           acknowledgement is required to achieve <cl>.
                    ```

                    https://stackoverflow.com/questions/76924473/cassandra-lwt-caswriteunknownresultexception-failure-status-is-unclear
                    basically boils down to that the value.

                    The practical take-away is to only use LWTs when really needed.
                    */
                    cdrs_tokio::error::Error::UnexpectedErrorCode(0x1700) => {
                        log::debug!(
                            "Query '{query_template}' in keyspace '{keyspace}' completed with error CAS_WRITE_UNKNOWN. It may or may not complete."
                        );
                    }
                    cdrs_tokio::error::Error::Server { body, addr } => {
                        // 0x2200    Invalid: The query is syntactically correct but invalid.
                        // This happens when the keyspace has not yet been created.
                        if body.ty.to_error_code() == 0x2200
                            && query_template.contains("CREATE TABLE")
                        {
                            log::debug!(
                                "Unable to create table at this time (will retry): {}",
                                body.message,
                            );
                            sleep(Duration::from_millis(250)).await;
                            continue;
                        }
                        log::debug!(
                            "Failed to execute query '{query_template}' in keyspace '{keyspace}' code {}: {:?}, {}",
                            body.ty.to_error_code(),
                            body,
                            addr
                        );
                    }
                    _ => {
                        log::info!(
                            "Failed to execute query '{query_template}' in keyspace '{keyspace}': {e:?}"
                        );
                    }
                }
                return None;
            } else {
                return result
                    .ok()
                    .and_then(|envelope| envelope.response_body()
                    .map_err(|e|{
                        log::info!(
                            "Failed to execute query '{query_template}' in keyspace '{keyspace}': {e:?}"
                        );
                    })
                    .ok());
            }
        }
    }
}
