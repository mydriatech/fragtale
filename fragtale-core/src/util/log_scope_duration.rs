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

//! Utility for measuring duration of a scope.

/// Leverage `Drop` trait to log when the struct goes out of scope.
pub struct LogScopeDuration<'a> {
    level: log::Level,
    module_path: &'a str,
    scope_name: &'a str,
    start_ts_micros: u64,
    min_micros_to_log: u64,
}

impl<'a> LogScopeDuration<'a> {
    /// Return a new instance.
    ///
    /// You can retrieve the module path of the caller using the `module_path!()` macro.
    pub fn new(
        level: log::Level,
        module_path: &'a str,
        scope_name: &'a str,
        min_micros_to_log: u64,
    ) -> Option<Self> {
        log::log_enabled!(target: module_path, level).then(|| Self {
            module_path,
            scope_name,
            start_ts_micros: Self::now_in_epoch_micros(),
            level,
            min_micros_to_log,
        })
    }

    /// Return the time this instance was created in epoch micros.
    pub fn start_ts_micros(&self) -> u64 {
        self.start_ts_micros
    }

    #[inline]
    fn now_in_epoch_micros() -> u64 {
        fragtale_client::time::get_timestamp_micros()
    }
}

impl Drop for LogScopeDuration<'_> {
    fn drop(&mut self) {
        let duration_micros = Self::now_in_epoch_micros() - self.start_ts_micros;
        if duration_micros >= self.min_micros_to_log {
            log::log!(target: self.module_path, self.level, "'{}' took {} µs.", self.scope_name, Self::now_in_epoch_micros() - self.start_ts_micros);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Initialize logging.
    pub fn init_logger() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init();
    }

    #[test]
    fn test_scoped_timer() {
        init_logger();
        let _ = LogScopeDuration::new(log::Level::Debug, module_path!(), "test_scoped_timer", 0);
        let _ = LogScopeDuration::new(
            log::Level::Trace,
            module_path!(),
            "filtered by log level",
            0,
        );
    }
}
