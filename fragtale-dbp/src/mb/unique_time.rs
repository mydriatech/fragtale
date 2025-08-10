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

//! Cluster-wide unique timestamps

/**
   Timestamp representation with microsecond granularity that is unique
   accross all instances of a cluster with a shared database.

   Encoding to u64:

   ```text
   MSB
   63  Reserved/unused to allow for conversion to positive i64
   62
   ...  8 bits: "Shelf". Roughly corresponds to a year.
   55
   54
   ... 25 bits: Roughly corresponds to seconds. Forms 33 bit "Bucket" with Shelf.
   30
   29
   ... 20 bits: Roughly corresponds to microseconds
   10
   9
   ... 10 bits: Instance identity assignment
   0
   LSB
   ```

   Bit 10..=62 (53 bits) corresonds to epoch micros which can represent 285.4 years from 1970.

   3,1536×10^13 microseconds per year, 2^45 ≃ 3,5×10^13
*/
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UniqueTime(u64);

impl From<i64> for UniqueTime {
    fn from(value: i64) -> Self {
        Self(u64::try_from(value).unwrap_or(0))
    }
}

impl From<&i64> for UniqueTime {
    fn from(value: &i64) -> Self {
        Self::from(*value)
    }
}

impl From<u64> for UniqueTime {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<&u64> for UniqueTime {
    fn from(value: &u64) -> Self {
        Self::from(*value)
    }
}

impl From<UniqueTime> for i64 {
    fn from(value: UniqueTime) -> i64 {
        i64::try_from(value.0).unwrap_or(i64::MAX)
    }
}

impl From<&UniqueTime> for i64 {
    fn from(value: &UniqueTime) -> i64 {
        i64::from(*value)
    }
}

impl From<UniqueTime> for u64 {
    fn from(value: UniqueTime) -> u64 {
        value.0
    }
}

impl From<&UniqueTime> for u64 {
    fn from(value: &UniqueTime) -> u64 {
        u64::from(*value)
    }
}
#[allow(dead_code)]
impl UniqueTime {
    const BITMASK_53_BITS: u64 = 0x001f_ffff_ffff_ffff;
    const BITMASK_33_BITS: u64 = 0x0000_0001_ffff_ffff;
    const BITMASK_30_BITS: u64 = 0x0000_0000_3fff_ffff;
    const BITMASK_10_BITS: u64 = 0x0000_0000_0000_03ff;
    const BITMASK_08_BITS: u64 = 0x0000_0000_0000_00ff;

    /// The highest allowed instance identifier.
    pub const MAX_INSTANCE_ID: u16 = 0x03ff;

    /// Return a new instance.
    pub fn new(micros_since_epoch: u64, instance_id: u16) -> Self {
        Self::from(
            ((micros_since_epoch & Self::BITMASK_53_BITS) << 10)
                | (u64::from(instance_id) & Self::BITMASK_10_BITS),
        )
    }

    /// Return `Self` in `i64` encoded form.
    pub fn as_encoded_i64(&self) -> i64 {
        i64::from(self)
    }

    /// Return `Self` in `u64` encoded form.
    pub fn as_encoded(&self) -> u64 {
        u64::from(self)
    }

    /// Return `Self` in byte encoded form.
    pub fn as_bytes(&self) -> [u8; 8] {
        u64::from(self).to_be_bytes()
    }

    /// Get unix epoch timestamp part in microseconds.
    pub fn get_time_micros(&self) -> u64 {
        (self.0 >> 10) & Self::BITMASK_53_BITS
    }

    /// Get bucket part as `i64`.
    pub fn get_bucket_i64(&self) -> i64 {
        i64::try_from((self.0 >> 30) & Self::BITMASK_33_BITS).unwrap_or(0)
    }

    /// Get bucket part as `u64`.
    pub fn get_bucket(&self) -> u64 {
        (self.0 >> 30) & Self::BITMASK_33_BITS
    }

    /// Get shelf part as `i16`.
    pub fn get_shelf_i16(&self) -> i16 {
        i16::try_from((self.0 >> 55) & Self::BITMASK_08_BITS).unwrap_or(0)
    }

    /// Get shelf part as `u16`.
    pub fn get_shelf(&self) -> u16 {
        u16::try_from((self.0 >> 55) & Self::BITMASK_08_BITS).unwrap_or(0)
    }

    /// Get instance identifier part as `u16`.
    pub fn get_instance_id(&self) -> u16 {
        u16::try_from(self.0 & Self::BITMASK_10_BITS).unwrap()
    }

    /// Return largest value in bucket.
    pub fn max_encoded_in_bucket(bucket: u64) -> u64 {
        ((bucket & Self::BITMASK_33_BITS) << 30) | Self::BITMASK_30_BITS
    }

    /// Return smallest value for timestamp.
    pub fn min_encoded_for_micros(micros: u64) -> u64 {
        (micros & Self::BITMASK_53_BITS) << 10
    }
}
