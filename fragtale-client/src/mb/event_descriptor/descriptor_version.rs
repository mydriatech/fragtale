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

//! Versioning of event descriptor.

/** Versioning of event descriptor (schema and extractors) for a topic.

The versioning uses the terms major, minor and patch similar to
[Semantic Versioning 2.0.0](https://semver.org/).

As long as the individual components of the version don't overflow 16-bit
unsigned, the encoded version can be used to compare ordering.
*/
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DescriptorVersion(u64);

impl DescriptorVersion {
    //const MAJOR_VERSION_MAX: u16 = 0xffff;
    const MINOR_VERSION_MAX: u16 = 0xffff;
    const PATCH_VERSION_MAX: u16 = 0xffff;

    /// Return a new instance from a fully specified version.
    pub fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self((u64::from(major) << 32) | (u64::from(minor) << 16) | u64::from(patch))
    }

    /// Return the encoded version.
    pub fn as_encoded(&self) -> u64 {
        self.0
    }

    /// Return a new instance from the encoded version.
    pub fn from_encoded(encoded: u64) -> Self {
        Self(encoded)
    }

    /// Return a new instance from just the major version.
    pub fn from_major(major: u16) -> Self {
        Self::new(major, Self::MINOR_VERSION_MAX, Self::PATCH_VERSION_MAX)
    }

    /// Return a new instance from the major and minor version.
    pub fn from_major_and_minor(major: u16, minor: u16) -> Self {
        Self::new(major, minor, Self::PATCH_VERSION_MAX)
    }

    /// Return the major version.
    ///
    /// An increment implies that incompatible changes has been made.
    pub fn get_major(&self) -> u16 {
        u16::try_from((self.0 >> 32) & 0xffff).unwrap()
    }

    /// Return the minor version.
    ///
    /// An increment implies that incompatible changes are backwards compatible.
    pub fn get_minor(&self) -> u16 {
        u16::try_from((self.0 >> 16) & 0xffff).unwrap()
    }

    /// Return the patch version.
    ///
    /// An increment implies that backwards compatible bug fixes.
    pub fn get_patch(&self) -> u16 {
        u16::try_from(self.0 & 0xffff).unwrap()
    }
}
