//! Performance benchmark tests for CRD operations.
//!
//! This module provides timing-based benchmarks for `StellarNode` CRD lifecycle
//! operations (create, update, delete) as well as concurrent and reconciler
//! throughput measurements.
//!
//! # Regression Detection
//!
//! Each benchmark compares its measured duration against a hard-coded baseline
//! (derived from `benchmarks/crd-operations/baseline.json`).  If the measured
//! time exceeds the baseline by more than [`REGRESSION_THRESHOLD_PCT`] the test
//! *fails*, which surfaces performance regressions in CI before they reach
//! `main`.
//!
//! # Running locally
//!
//! ```bash
//! cargo test -p stellar-k8s --lib crd::benchmarks -- --nocapture 2>&1 | tee bench-results.txt
//! ```
//!
//! For a full criterion-style run (requires the `criterion` feature):
//!
//! ```bash
//! cargo bench --bench crd_operations
//! ```

#![allow(dead_code)]

use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Regression thresholds & baseline constants
// ---------------------------------------------------------------------------

/// Maximum allowed performance degradation as a fraction (0.10 = 10 %).
/// Any benchmark that takes more than `baseline * (1 + REGRESSION_THRESHOLD_PCT)`
/// will fail with a performance regression error.
pub const REGRESSION_THRESHOLD_PCT: f64 = 0.10;

/// Baseline median duration (µs) for a single CRD creation round-trip.
/// Derived from `benchmarks/crd-operations/baseline.json`.
pub const BASELINE_CRD_CREATE_US: u64 = 1_200;

/// Baseline median duration (µs) for a single CRD spec-update round-trip.
pub const BASELINE_CRD_UPDATE_US: u64 = 950;

/// Baseline median duration (µs) for a single CRD deletion round-trip.
pub const BASELINE_CRD_DELETE_US: u64 = 800;

/// Baseline median duration (µs) for one concurrent-batch CRD operation
/// (averaged across all workers in the batch).
pub const BASELINE_CONCURRENT_OPS_US: u64 = 1_800;

/// Baseline minimum reconciler throughput (operations per second).
pub const BASELINE_RECONCILER_OPS_PER_SEC: f64 = 500.0;

// ---------------------------------------------------------------------------
// BenchResult – lightweight sample accumulator
// ---------------------------------------------------------------------------

/// A lightweight accumulator of timing samples.
#[derive(Debug, Default)]
pub struct BenchResult {
    pub samples: Vec<Duration>,
}

impl BenchResult {
    /// Record one timing sample.
    pub fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    /// Median sample duration in microseconds.
    pub fn median_us(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort();
        let mid = sorted.len() / 2;
        sorted[mid].as_micros() as u64
    }

    /// Mean sample duration in microseconds.
    pub fn mean_us(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let sum: u128 = self.samples.iter().map(|d| d.as_micros()).sum();
        (sum / self.samples.len() as u128) as u64
    }

    /// 99th-percentile sample duration in microseconds.
    pub fn p99_us(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort();
        let idx = ((sorted.len() as f64) * 0.99) as usize;
        let idx = idx.min(sorted.len() - 1);
        sorted[idx].as_micros() as u64
    }

    /// Operations completed per second (total_ops / total_time).
    pub fn ops_per_sec(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let total_sec: f64 = self.samples.iter().map(|d| d.as_secs_f64()).sum();
        if total_sec == 0.0 {
            return f64::INFINITY;
        }
        self.samples.len() as f64 / total_sec
    }
}

// ---------------------------------------------------------------------------
// Regression assertion helpers
// ---------------------------------------------------------------------------

/// Asserts that `measured_us` is within the allowed regression band relative to
/// `baseline_us`.
///
/// # Panics
///
/// Panics (causing the test to fail) when:
/// `measured_us > baseline_us * (1 + REGRESSION_THRESHOLD_PCT)`
pub fn assert_no_regression(label: &str, measured_us: u64, baseline_us: u64) {
    let threshold_us = (baseline_us as f64 * (1.0 + REGRESSION_THRESHOLD_PCT)) as u64;
    assert!(
        measured_us <= threshold_us,
        "PERFORMANCE REGRESSION detected in '{}': \
         measured {}µs  >  threshold {}µs  (baseline {}µs + {:.0}%)",
        label,
        measured_us,
        threshold_us,
        baseline_us,
        REGRESSION_THRESHOLD_PCT * 100.0,
    );
}

/// Asserts that `measured_ops_per_sec` is within the allowed throughput
/// regression band.
///
/// # Panics
///
/// Panics when:
/// `measured_ops_per_sec < baseline_ops_per_sec * (1 - REGRESSION_THRESHOLD_PCT)`
pub fn assert_no_throughput_regression(label: &str, measured: f64, baseline: f64) {
    let threshold = baseline * (1.0 - REGRESSION_THRESHOLD_PCT);
    assert!(
        measured >= threshold,
        "THROUGHPUT REGRESSION detected in '{}': \
         measured {:.1} ops/s  <  threshold {:.1} ops/s  (baseline {:.1} ops/s - {:.0}%)",
        label,
        measured,
        threshold,
        baseline,
        REGRESSION_THRESHOLD_PCT * 100.0,
    );
}

// ---------------------------------------------------------------------------
// Simulated CRD operation helpers
//
// In unit-test mode there is no live API server.  We simulate the work a real
// CRD round-trip would perform:
//   • JSON serialisation/deserialisation of a `StellarNodeSpec`
//   • Spec validation
//
// Specs are built via `serde_json::json!` so the benchmarks compile regardless
// of which optional fields exist in the struct — only the mandatory subset
// (`nodeType`, `network`, `version`) must always be present.
// ---------------------------------------------------------------------------

/// Minimal valid `StellarNodeSpec` payload (Validator / Testnet).
fn make_validator_payload() -> serde_json::Value {
    serde_json::json!({
        "nodeType": "Validator",
        "network": "Testnet",
        "version": "v21.0.0",
        "storage": {
            "storageClass": "standard",
            "size": "100Gi"
        },
        "validatorConfig": {
            "seedSecretRef": "bench-seed",
            "enableHistoryArchive": true
        }
    })
}

/// Minimal valid `StellarNodeSpec` payload (Horizon / Mainnet).
fn make_horizon_payload() -> serde_json::Value {
    serde_json::json!({
        "nodeType": "Horizon",
        "network": "Mainnet",
        "version": "v21.0.0",
        "storage": {
            "storageClass": "premium",
            "size": "200Gi"
        }
    })
}

/// Minimal valid `StellarNodeSpec` payload (SorobanRpc / Testnet).
fn make_soroban_payload() -> serde_json::Value {
    serde_json::json!({
        "nodeType": "SorobanRpc",
        "network": "Testnet",
        "version": "v21.0.0",
        "storage": {
            "storageClass": "standard",
            "size": "50Gi"
        }
    })
}

/// Simulate a CRD creation round-trip:
///   JSON payload → deserialise into `StellarNodeSpec` → validate → serialise back.
fn simulate_crd_create() -> Duration {
    use crate::crd::StellarNodeSpec;

    let payload = make_validator_payload();
    let raw = serde_json::to_string(&payload).expect("payload must serialise");

    let t0 = Instant::now();
    // Deserialise (mirrors API-server admission / controller informer path).
    let spec: StellarNodeSpec = serde_json::from_str(&raw).expect("must deserialise");
    // Run validation logic (same code path as the admission webhook).
    let _ = spec.validate();
    // Re-serialise (mirrors what the operator writes back to status / configmaps).
    let _ = serde_json::to_string(&spec).expect("must serialise");
    t0.elapsed()
}

/// Simulate a CRD spec-update round-trip:
///   load → modify version field in JSON → deserialise → validate → serialise.
fn simulate_crd_update() -> Duration {
    use crate::crd::StellarNodeSpec;

    let mut payload = make_horizon_payload();

    let t0 = Instant::now();
    // Apply the update to the JSON value (mirrors a server-side apply patch).
    payload["version"] = serde_json::json!("v21.1.0");
    payload["replicas"] = serde_json::json!(3);

    let raw = serde_json::to_string(&payload).expect("must serialise");
    let spec: StellarNodeSpec = serde_json::from_str(&raw).expect("must deserialise");
    let _ = spec.validate();
    let _ = serde_json::to_string(&spec).expect("must serialise");
    t0.elapsed()
}

/// Simulate the finaliser work during CRD deletion:
///   load → verify node type → drop (in-memory cleanup).
fn simulate_crd_delete() -> Duration {
    use crate::crd::{StellarNodeSpec, types::NodeType};

    let payload = make_soroban_payload();
    let raw = serde_json::to_string(&payload).expect("must serialise");

    let t0 = Instant::now();
    // Deserialise to verify fields before issuing deletes.
    let spec: StellarNodeSpec = serde_json::from_str(&raw).expect("must deserialise");
    // Inspect the node type (mirrors finaliser pre-condition checks).
    assert_eq!(spec.node_type, NodeType::SorobanRpc);
    // Run validation one final time to confirm integrity before cleanup.
    let _ = spec.validate();
    // Drop the spec to simulate in-memory resource cleanup.
    drop(spec);
    t0.elapsed()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    const ITERATIONS: usize = 100;
    const WARMUP: usize = 10;
    const CONCURRENT_WORKERS: usize = 8;
    const CONCURRENT_OPS_PER_WORKER: usize = 20;
    const RECONCILE_ITERATIONS: usize = 1_000;

    // -----------------------------------------------------------------------
    // bench_crd_creation
    // -----------------------------------------------------------------------

    /// Benchmarks the cost of creating (deserialising + validating +
    /// re-serialising) a `StellarNode` CRD spec.
    ///
    /// Regression threshold: +10 % over [`BASELINE_CRD_CREATE_US`].
    #[test]
    fn bench_crd_creation() {
        // Warmup: prime instruction-cache and allocator.
        for _ in 0..WARMUP {
            let _ = simulate_crd_create();
        }

        let mut result = BenchResult::default();
        for _ in 0..ITERATIONS {
            result.record(simulate_crd_create());
        }

        let median = result.median_us();
        let p99 = result.p99_us();
        let mean = result.mean_us();

        println!(
            "[bench_crd_creation] iterations={ITERATIONS}  \
             mean={mean}µs  median={median}µs  p99={p99}µs  \
             baseline={BASELINE_CRD_CREATE_US}µs"
        );

        assert_no_regression("crd_creation (median)", median, BASELINE_CRD_CREATE_US);
    }

    // -----------------------------------------------------------------------
    // bench_crd_update
    // -----------------------------------------------------------------------

    /// Benchmarks the cost of updating an existing `StellarNode` CRD spec.
    ///
    /// Regression threshold: +10 % over [`BASELINE_CRD_UPDATE_US`].
    #[test]
    fn bench_crd_update() {
        for _ in 0..WARMUP {
            let _ = simulate_crd_update();
        }

        let mut result = BenchResult::default();
        for _ in 0..ITERATIONS {
            result.record(simulate_crd_update());
        }

        let median = result.median_us();
        let p99 = result.p99_us();
        let mean = result.mean_us();

        println!(
            "[bench_crd_update] iterations={ITERATIONS}  \
             mean={mean}µs  median={median}µs  p99={p99}µs  \
             baseline={BASELINE_CRD_UPDATE_US}µs"
        );

        assert_no_regression("crd_update (median)", median, BASELINE_CRD_UPDATE_US);
    }

    // -----------------------------------------------------------------------
    // bench_crd_deletion
    // -----------------------------------------------------------------------

    /// Benchmarks the cost of the finaliser path during CRD deletion.
    ///
    /// Regression threshold: +10 % over [`BASELINE_CRD_DELETE_US`].
    #[test]
    fn bench_crd_deletion() {
        for _ in 0..WARMUP {
            let _ = simulate_crd_delete();
        }

        let mut result = BenchResult::default();
        for _ in 0..ITERATIONS {
            result.record(simulate_crd_delete());
        }

        let median = result.median_us();
        let p99 = result.p99_us();
        let mean = result.mean_us();

        println!(
            "[bench_crd_deletion] iterations={ITERATIONS}  \
             mean={mean}µs  median={median}µs  p99={p99}µs  \
             baseline={BASELINE_CRD_DELETE_US}µs"
        );

        assert_no_regression("crd_deletion (median)", median, BASELINE_CRD_DELETE_US);
    }

    // -----------------------------------------------------------------------
    // bench_concurrent_crd_ops
    // -----------------------------------------------------------------------

    /// Benchmarks CRD operations under concurrent load using OS threads.
    ///
    /// Spawns [`CONCURRENT_WORKERS`] threads, each performing
    /// [`CONCURRENT_OPS_PER_WORKER`] create/update/delete cycles.  The median
    /// per-operation latency across all workers must not regress beyond
    /// [`BASELINE_CONCURRENT_OPS_US`].
    #[test]
    fn bench_concurrent_crd_ops() {
        let all_samples: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::with_capacity(CONCURRENT_WORKERS);
        for _ in 0..CONCURRENT_WORKERS {
            let samples_ref = Arc::clone(&all_samples);
            let handle = thread::spawn(move || {
                let mut local: Vec<Duration> =
                    Vec::with_capacity(CONCURRENT_OPS_PER_WORKER * 3);
                for _ in 0..CONCURRENT_OPS_PER_WORKER {
                    local.push(simulate_crd_create());
                    local.push(simulate_crd_update());
                    local.push(simulate_crd_delete());
                }
                let mut guard = samples_ref.lock().expect("mutex poisoned");
                guard.extend(local);
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().expect("worker thread panicked");
        }

        let raw_samples = Arc::try_unwrap(all_samples)
            .expect("no other owners")
            .into_inner()
            .expect("mutex poisoned");

        let result = BenchResult { samples: raw_samples };
        let total_ops = result.samples.len();
        let median = result.median_us();
        let p99 = result.p99_us();
        let mean = result.mean_us();

        println!(
            "[bench_concurrent_crd_ops] workers={CONCURRENT_WORKERS}  \
             ops_per_worker={CONCURRENT_OPS_PER_WORKER}  total_ops={total_ops}  \
             mean={mean}µs  median={median}µs  p99={p99}µs  \
             baseline={BASELINE_CONCURRENT_OPS_US}µs"
        );

        assert_no_regression(
            "concurrent_crd_ops (median)",
            median,
            BASELINE_CONCURRENT_OPS_US,
        );
    }

    // -----------------------------------------------------------------------
    // bench_reconciler_throughput
    // -----------------------------------------------------------------------

    /// Measures simulated reconciler throughput (operations per second).
    ///
    /// The reconciler hot path is:
    ///   1. Serialise the spec (outgoing write-back).
    ///   2. Validate the spec (decision logic).
    ///   3. Deserialise the spec (informer cache hit).
    ///
    /// This benchmark fails if throughput drops below
    /// `BASELINE_RECONCILER_OPS_PER_SEC * (1 - REGRESSION_THRESHOLD_PCT)`.
    #[test]
    fn bench_reconciler_throughput() {
        use crate::crd::StellarNodeSpec;

        let payload = make_validator_payload();
        let raw = serde_json::to_string(&payload).expect("must serialise");

        // Warmup: prime caches.
        for _ in 0..WARMUP {
            let spec: StellarNodeSpec = serde_json::from_str(&raw).expect("must deserialise");
            let _ = spec.validate();
            let _ = serde_json::to_string(&spec).expect("must serialise");
        }

        let mut result = BenchResult::default();

        for _ in 0..RECONCILE_ITERATIONS {
            let t0 = Instant::now();
            // Reconciler hot path:
            //  1. Deserialise from informer cache.
            let spec: StellarNodeSpec =
                serde_json::from_str(&raw).expect("must deserialise");
            //  2. Validate spec (admission / reconcile pre-condition).
            let _ = spec.validate();
            //  3. Re-serialise (write back to status or configmap).
            let _ = serde_json::to_string(&spec).expect("must serialise");
            result.record(t0.elapsed());
        }

        let ops_per_sec = result.ops_per_sec();
        let mean_us = result.mean_us();
        let p99_us = result.p99_us();

        println!(
            "[bench_reconciler_throughput] iterations={RECONCILE_ITERATIONS}  \
             mean={mean_us}µs  p99={p99_us}µs  \
             ops_per_sec={ops_per_sec:.1}  \
             baseline={BASELINE_RECONCILER_OPS_PER_SEC:.1} ops/s"
        );

        assert_no_throughput_regression(
            "reconciler_throughput",
            ops_per_sec,
            BASELINE_RECONCILER_OPS_PER_SEC,
        );
    }

    // -----------------------------------------------------------------------
    // Regression detection unit-tests (meta-tests)
    // -----------------------------------------------------------------------

    /// `assert_no_regression` must pass when measurement is within threshold.
    #[test]
    fn regression_detection_within_threshold() {
        // 5 % above baseline (1050 vs 1000) → OK
        assert_no_regression("within_5pct", 1_050, 1_000);
        // Exactly at the 10 % boundary (1100 vs 1000) → OK
        assert_no_regression("at_boundary", 1_100, 1_000);
    }

    /// `assert_no_regression` must panic when regression exceeds 10 %.
    #[test]
    #[should_panic(expected = "PERFORMANCE REGRESSION")]
    fn regression_detection_triggers_above_threshold() {
        // 1101 µs vs 1000 µs baseline exceeds the 10 % band.
        assert_no_regression("above_threshold", 1_101, 1_000);
    }

    /// `assert_no_throughput_regression` must pass when ops/s is sufficient.
    #[test]
    fn throughput_regression_within_threshold() {
        // Exactly 10 % below baseline (450 vs 500) → OK
        assert_no_throughput_regression("at_boundary", 450.0, 500.0);
        // Only 5 % below baseline → OK
        assert_no_throughput_regression("within_5pct", 475.0, 500.0);
    }

    /// `assert_no_throughput_regression` must panic below threshold.
    #[test]
    #[should_panic(expected = "THROUGHPUT REGRESSION")]
    fn throughput_regression_triggers_below_threshold() {
        // 449 ops/s vs 500 ops/s baseline is more than 10 % below.
        assert_no_throughput_regression("below_threshold", 449.0, 500.0);
    }
}
