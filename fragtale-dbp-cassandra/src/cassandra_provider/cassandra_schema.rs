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

//! Cassandra database schema operations.

use super::CassandraResultMapper;
use super::cassandra_session::CassandraSession;
use cdrs_tokio::frame::message_response::ResponseBody;
use uuid::Uuid;

/// Cassandra database schema operations.
pub struct CassandraSchema {}

impl CassandraSchema {
    const KEYSPACE_SYSTEM: &'static str = "system";
    const KEYSPACE_SYSTEM_SCHEMA: &'static str = "system_schema";

    const CQL_TEMPLATE_SELECT_HOST_ID_AND_SCHEMA_VERSION_LOCAL: &'static str =
        "SELECT host_id,schema_version FROM local;";

    /// Retrieve the connected instance's `host_id` and `schema_version`.
    pub async fn get_host_id_and_schema_version_local(
        cs: &CassandraSession,
    ) -> Option<(Uuid, Uuid)> {
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_HOST_ID_AND_SCHEMA_VERSION_LOCAL,
            Self::KEYSPACE_SYSTEM,
            cdrs_tokio::query_values!(),
        )
        .await
        .map(CassandraResultMapper::into_uuid_tuplet_vec)
        .and_then(|uuids| uuids.first().cloned())
    }

    const CQL_TEMPLATE_SELECT_HOST_ID_AND_SCHEMA_VERSION_PEERS: &'static str =
        "SELECT host_id,schema_version FROM peers;";

    /// Retrieve the connected instance's view of peers `host_id` and
    /// `schema_version`.
    pub async fn get_host_id_and_schema_versions_of_peers(
        cs: &CassandraSession,
    ) -> Vec<(Uuid, Uuid)> {
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_HOST_ID_AND_SCHEMA_VERSION_PEERS,
            Self::KEYSPACE_SYSTEM,
            cdrs_tokio::query_values!(),
        )
        .await
        .map(CassandraResultMapper::into_uuid_tuplet_vec)
        .unwrap_or_default()
    }

    const CQL_TEMPLATE_CREATE_KEYSPACE: &'static str = "
        CREATE KEYSPACE IF NOT EXISTS {{ keyspace }}
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : {{ replication_factor }}
        };
        ";

    /// Create keyspace.
    pub async fn create_keyspace(
        cs: &CassandraSession,
        keyspace_name: &str,
        replication_factor: usize,
    ) {
        let replication_factor = std::cmp::max(1, replication_factor);
        if replication_factor < 3 {
            log::warn!(
                "Creating keyspace '{keyspace_name}' with a replication_factor of {replication_factor}. This will not change if you add additional nodes."
            );
        }
        cs.query_raw(
            &Self::CQL_TEMPLATE_CREATE_KEYSPACE.replacen(
                "{{ replication_factor }}",
                &replication_factor.to_string(),
                1,
            ),
            keyspace_name,
        )
        .await;
    }

    const CQL_TEMPLATE_SELECT_KEYSPACE: &'static str = "
        SELECT keyspace_name FROM keyspaces
        WHERE keyspace_name = ?
        ;";

    /// Return `true` if the keyspace exists.
    pub async fn keyspace_exists(cs: &CassandraSession, keyspace: &str) -> bool {
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_KEYSPACE,
            Self::KEYSPACE_SYSTEM_SCHEMA,
            cdrs_tokio::query_values!(keyspace),
        )
        .await
        .and_then(ResponseBody::into_rows)
        .is_some_and(|rows| !rows.is_empty())
    }

    const CQL_TEMPLATE_SELECT_TABLE_NAME: &'static str = "
        SELECT table_name FROM tables
        WHERE keyspace_name = ? AND table_name = ?
        ;";

    /// Return `true` if the table exists in the keyspace.
    pub async fn keyspace_table_exists(
        cs: &CassandraSession,
        keyspace: &str,
        table_name: &str,
    ) -> bool {
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_TABLE_NAME,
            Self::KEYSPACE_SYSTEM_SCHEMA,
            cdrs_tokio::query_values!(keyspace, table_name.to_owned()),
        )
        .await
        .and_then(ResponseBody::into_rows)
        .is_some_and(|rows| !rows.is_empty())
    }

    const CQL_TEMPLATE_SELECT_INDEX: &'static str = "
        SELECT index_name FROM indexes
        WHERE keyspace_name = ? AND table_name = ? AND index_name = ?
        ;";

    /// Return `true` if the table index exists in the keyspace.
    pub async fn keyspace_table_index_exists(
        cs: &CassandraSession,
        keyspace: &str,
        table_name: &str,
        index_name: &str,
    ) -> bool {
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_INDEX,
            Self::KEYSPACE_SYSTEM_SCHEMA,
            cdrs_tokio::query_values!(keyspace, table_name, index_name),
        )
        .await
        .and_then(|response_body| response_body.into_rows())
        .is_some_and(|rows| !rows.is_empty())
    }

    const CQL_TEMPLATE_SELECT_COLUMN_NAMES: &'static str = "
        SELECT column_name FROM columns
        WHERE keyspace_name = ? AND table_name = ?
        ;";

    /// Return all table's column names in the keyspace.
    pub async fn column_names_by_keyspace_and_table(
        cs: &CassandraSession,
        keyspace_name: &str,
        table_name: &str,
    ) -> Vec<String> {
        let values = cdrs_tokio::query_values!(
        "keyspace_name" => keyspace_name.to_owned(),
        "table_name" => table_name.to_owned()
        );
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_COLUMN_NAMES,
            Self::KEYSPACE_SYSTEM_SCHEMA,
            values,
        )
        .await
        .map(CassandraResultMapper::into_string_vec)
        .unwrap_or_default()
    }

    const CQL_TEMPLATE_SELECT_INDEX_NAMES: &'static str = "
        SELECT index_name FROM indexes
        WHERE keyspace_name = ? AND table_name = ?
        ;";

    /// Return all table's index names in the keyspace.
    pub async fn index_names_by_keyspace_and_table(
        cs: &CassandraSession,
        keyspace_name: &str,
        table_name: &str,
    ) -> Vec<String> {
        let values = cdrs_tokio::query_values!(
        "keyspace_name" => keyspace_name.to_owned(),
        "table_name" => table_name.to_owned()
        );
        cs.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_INDEX_NAMES,
            Self::KEYSPACE_SYSTEM_SCHEMA,
            values,
        )
        .await
        .map(CassandraResultMapper::into_string_vec)
        .unwrap_or_default()
    }

    const CQL_TEMPLATE_ALTER_TABLE_ADD_COLUMN: &'static str = "
        ALTER TABLE {{ keyspace }}.{{ table_name }}
        ADD IF NOT EXISTS {{ column_name }} {{ cql_type }}
        ;";

    /// Add a column to the table in the keyspace.
    pub async fn alter_table_add_column(
        cs: &CassandraSession,
        keyspace: &str,
        table_name: &str,
        column_name: &str,
        cql_type: &str,
    ) {
        let query_template = Self::CQL_TEMPLATE_ALTER_TABLE_ADD_COLUMN
            .replace("{{ table_name }}", table_name)
            .replace("{{ column_name }}", column_name)
            .replace("{{ cql_type }}", cql_type);
        cs.query_raw(&query_template, keyspace).await;
    }

    const CQL_TEMPLATE_ALTER_TABLE_ADD_INDEX: &'static str = "
        CREATE CUSTOM INDEX IF NOT EXISTS {{ index_name }}
        ON {{ keyspace }}.{{ table_name }} ({{ column_name }})
        USING 'StorageAttachedIndex'
        ;";

    /// Add an index to the table in the keyspace.
    pub async fn alter_table_add_index(
        cs: &CassandraSession,
        keyspace: &str,
        table_name: &str,
        column_name: &str,
        index_name: &str,
    ) {
        let query_template = Self::CQL_TEMPLATE_ALTER_TABLE_ADD_INDEX
            .replace("{{ table_name }}", table_name)
            .replace("{{ column_name }}", column_name)
            .replace("{{ index_name }}", index_name);
        cs.query_raw(&query_template, keyspace).await;
    }
}
