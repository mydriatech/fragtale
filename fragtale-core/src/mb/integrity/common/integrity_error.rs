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

//! Data integrity failures.

use std::error::Error;
use std::fmt;

/// Cause of certificate validation error.
#[derive(Debug)]
pub enum IntegrityErrorKind {
    /// Data is not well formed.
    Malformed,
    /// Data integrity is not verifyable by this proof.
    InvalidProof,
    /// Failed to validate GenericDataProtection
    ValidationFailure,
}

#[allow(dead_code)]
impl IntegrityErrorKind {
    /// Create a new instance with an error message.
    pub fn error_with_msg<S: AsRef<str>>(self, msg: S) -> IntegrityError {
        IntegrityError {
            kind: self,
            msg: Some(msg.as_ref().to_string()),
        }
    }

    /// Create a new instance without an error message.
    pub fn error(self) -> IntegrityError {
        IntegrityError {
            kind: self,
            msg: None,
        }
    }
}

impl fmt::Display for IntegrityErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/** Data integrity error.

Create a new instance via [IntegrityErrorKind].
*/
#[derive(Debug)]
pub struct IntegrityError {
    kind: IntegrityErrorKind,
    msg: Option<String>,
}

#[allow(dead_code)]
impl IntegrityError {
    /// Return the [IntegrityErrorKind] type of this error.
    pub fn kind(&self) -> &IntegrityErrorKind {
        &self.kind
    }
}

impl fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(msg) = &self.msg {
            write!(f, "{} {}", self.kind, msg)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl Error for IntegrityError {}
