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

//! Message broker errors.

use std::error::Error;
use std::fmt;

/// Cause of error.
#[derive(Debug)]
pub enum MessageBrokerErrorKind {
    /// General failure. See message for details.
    Unspecified,
    /// Malformed identifier. E.g. topic_id or consumer_id.
    MalformedIdentifier,
    /// Event descriptor error.
    EvenDescriptorError,
    /// Time could be trusted.
    TrustedTimeError,
    /// Failure during processing before storing event, like schema validation
    /// or index column extraction.
    PreStorageProcessorError,
    /// Failue related to integrity protection.
    IntegrityProtectionError,
    /// Authentication failed.
    AuthenticationFailure,
    /// Unauthorized.
    Unauthorized,
}

impl MessageBrokerErrorKind {
    /// Create a new instance with an error message.
    pub fn error_with_msg<S: AsRef<str>>(self, msg: S) -> MessageBrokerError {
        MessageBrokerError {
            kind: self,
            msg: Some(msg.as_ref().to_string()),
        }
    }

    /// Create a new instance without an error message.
    pub fn error(self) -> MessageBrokerError {
        MessageBrokerError {
            kind: self,
            msg: None,
        }
    }
}

impl fmt::Display for MessageBrokerErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/** Message broker error.

Create a new instance via [MessageBrokerErrorKind].
*/
#[derive(Debug)]
pub struct MessageBrokerError {
    kind: MessageBrokerErrorKind,
    msg: Option<String>,
}

impl MessageBrokerError {
    /// Return the type of error.
    pub fn kind(&self) -> &MessageBrokerErrorKind {
        &self.kind
    }
}

impl fmt::Display for MessageBrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(msg) = &self.msg {
            write!(f, "{} {}", self.kind, msg)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl AsRef<MessageBrokerError> for MessageBrokerError {
    fn as_ref(&self) -> &MessageBrokerError {
        self
    }
}

impl Error for MessageBrokerError {}
