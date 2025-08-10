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

//! Event subcription query parameters.

use actix_web::Error;
use actix_web::error::ErrorBadRequest;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use serde::Deserialize;
use std::num::ParseIntError;

/// Starting point and acceptable event descriptor version for getting messages.
#[derive(Debug, Deserialize)]
pub struct NextQueryParams {
    /// Only consider events newer than this in epoch milliseconds.
    #[serde(rename = "from")]
    from_epoch_millis: Option<u64>,
    /// Event Descriptor SemVer that the client prefers.
    #[serde(rename = "version")]
    event_descriptor_semver: Option<String>,
}

impl NextQueryParams {
    /// Get the earliest point in time that events should be delivered from in
    /// epoch microseconds.
    pub fn get_from_epoch_micros(&self) -> Option<u64> {
        self.from_epoch_millis.map(|ms| ms * 1000)
    }

    /// Parse event descriptor version String, if present.
    ///
    /// Errors out with HTTP 400 Bad Request if parameters is not in the form
    /// `[number[.number]]`
    pub fn as_descriptor_version(
        event_descriptor_semver: &Option<String>,
    ) -> Result<Option<DescriptorVersion>, Error> {
        Self::as_descriptor_version_internal(event_descriptor_semver).map_err(|e| {
            ErrorBadRequest(
                format!(
                    "Invalid format of 'version' query parameter. Use 'major.minor'. Error was: {e}"
                )
                .to_string(),
            )
        })
    }

    /// Respect consumers version support to avoid (too new) incompatibel
    /// messages.
    fn as_descriptor_version_internal(
        event_descriptor_semver: &Option<String>,
    ) -> Result<Option<DescriptorVersion>, ParseIntError> {
        if let Some(input) = event_descriptor_semver {
            let mut split = input.trim().split('.');
            if let Some(major_str) = split.next() {
                let major = major_str.parse::<u16>()?;
                if let Some(minor_str) = split.next() {
                    let minor = minor_str.parse::<u16>()?;
                    return Ok(Some(DescriptorVersion::from_major_and_minor(major, minor)));
                } else {
                    return Ok(Some(DescriptorVersion::from_major(major)));
                }
            }
        }
        Ok(None)
    }

    /// Respect consumers version support to avoid (too new) incompatibel
    /// messages.
    ///
    /// Errors out with HTTP 400 Bad Request if parameters is not in the form
    /// `[number[.number]]`
    pub fn get_descriptor_version(&self) -> Result<Option<DescriptorVersion>, Error> {
        Self::as_descriptor_version(&self.event_descriptor_semver)
    }
}
