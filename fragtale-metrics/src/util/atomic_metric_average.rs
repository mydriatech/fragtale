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

//! Atomic average calculation.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/** Atomic average calculations tailored for metrics.

[Self] uses a single [AtomicU64] where the most significant bits are
used to track the number of items.

```text
63-40   (24 bits)   Number of items.    2^24 ≃ > 15 M items
39- 0   (40 bits)   Sum of all items.   2^40 = > 10^12
```

The averaged values needs to be below `2^40 / 2^24 = 65_536`.

Intended use-case:

* Get average of duration in range 0-60_000 milliseconds.
* 1M ops/s and [AtomicMetricAverage] is scraped every 15 seconds.
* OR 0.25M ops/s and [AtomicMetricAverage] is scraped every 60 seconds.
*/
#[derive(Default)]
pub struct AtomicMetricAverage {
    value: AtomicU64,
}
impl AtomicMetricAverage {
    // 65_536
    const MAX_VALUE: u64 = 0x0000_0000_0000_ffff;

    /// Append value in the range (0-65535) to average calc.
    ///
    /// If the value is larger than the allow range the max will used.
    pub fn append_with_cap(&self, value: u64) {
        self.value.fetch_add(
            std::cmp::min(Self::MAX_VALUE, value) | 0x0000_0100_0000_0000,
            Ordering::Relaxed,
        );
    }

    /// Get the average and reset the calculation.
    pub fn get_and_reset(&self) -> u64 {
        let value = self.value.swap(0, Ordering::Relaxed);
        let items = (value & 0xffff_ff00_0000_0000) >> 40;
        if items == 0 {
            return 0;
        }
        let sum = value & 0x0000_00ff_ffff_ffff;
        sum / items
    }
}
