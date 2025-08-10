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

//! Cassandra query result mapping.

use cdrs_tokio::frame::message_response::ResponseBody;
use cdrs_tokio::types::{ByName, IntoRustByIndex};

/// Cassandra query result mapper.
pub struct CassandraResultMapper {}

impl CassandraResultMapper {
    /// Map rows into entities.
    pub fn into_entities<T: cdrs_tokio::frame::TryFromRow>(response_body: ResponseBody) -> Vec<T> {
        response_body
            .into_rows()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                T::try_from_row(row)
                    .map_err(|e| {
                        log::debug!("try_from_row: {e}");
                    })
                    .ok()
            })
            .collect()
    }

    /// Map first column of each rows into a String.
    pub fn into_string_vec(response_body: ResponseBody) -> Vec<String> {
        response_body
            .into_rows()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                row.get_by_index(0)
                    .map_err(|e| {
                        log::debug!("get_by_index(0): {e}");
                    })
                    .ok()
                    .and_then(|column_opt| column_opt)
            })
            .collect()
    }

    /// Map first column of each rows into a Uuid-tuplet.
    pub fn into_uuid_tuplet_vec(response_body: ResponseBody) -> Vec<(uuid::Uuid, uuid::Uuid)> {
        response_body
            .into_rows()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let uuid0 = row
                    .get_by_index(0)
                    .map_err(|e| {
                        log::debug!("get_by_index(0): {e}");
                    })
                    .ok()
                    .and_then(|column_opt| column_opt);
                let uuid1 = row
                    .get_by_index(1)
                    .map_err(|e| {
                        log::debug!("get_by_index(1): {e}");
                    })
                    .ok()
                    .and_then(|column_opt| column_opt);
                uuid0.and_then(|uuid0| uuid1.map(|uuid1| (uuid0, uuid1)))
            })
            .collect()
    }

    /// Map first column of each rows into a Uuid-tuplet.
    pub fn into_string_u64_tuplet_vec(response_body: ResponseBody) -> Vec<(String, u64)> {
        response_body
            .into_rows()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let string = row
                    .get_by_index(0)
                    .map_err(|e| {
                        log::debug!("get_by_index(0): {e}");
                    })
                    .ok()
                    .and_then(|column_opt| column_opt);
                let number: Option<i64> = row
                    .get_by_index(1)
                    .map_err(|e| {
                        log::debug!("get_by_index(1): {e}");
                    })
                    .ok()
                    .and_then(|column_opt| column_opt);
                string.and_then(|string| {
                    number.map(|number| (string, u64::try_from(number).unwrap_or_default()))
                })
            })
            .collect()
    }

    /// Conditional statement have a special result named `[applied]`.
    ///
    /// This will return `true` if the `[applied]` result is missing which
    /// happens if the statement was not conditional.
    pub fn into_applied(response_body: ResponseBody) -> bool {
        response_body
            .into_rows()
            .unwrap_or_default()
            .first()
            .and_then(|row| row.by_name::<bool>("[applied]").unwrap_or(Some(true)))
            .unwrap_or(true)
    }
}
