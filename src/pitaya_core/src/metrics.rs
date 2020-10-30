use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid metric: {0}")]
    InvalidMetric(String),

    #[error("failed to start metrics server: {0}")]
    FailedToStartServer(String),
}

#[derive(Debug, PartialEq)]
pub enum MetricKind {
    Histogram,
    Gauge,
    Counter,
}

/// The options required for a metric.
pub struct Opts {
    pub kind: MetricKind,
    pub namespace: String,
    pub subsystem: String,
    /// The name that the metric is going to have.
    /// This name has to be unique.
    pub name: String,
    /// A help string for the metric.
    pub help: String,
    /// The labels that are used for this metric.
    pub variable_labels: Vec<String>,
    /// The buckets for a histogram. This field is ignored for other metric kinds.
    pub buckets: Vec<f64>,
}

/// Represents a reporter that can be used across multiple threads.
pub type ThreadSafeReporter = Arc<RwLock<Box<dyn Reporter + Send + Sync + 'static>>>;

/// A Reporter is a trait for any type that can be used for reporting metrics.
/// Common implementations of this trait could be Prometheus or DogStatsD, for example.
#[async_trait]
pub trait Reporter {
    fn register_counter(&mut self, opts: Opts) -> Result<(), Error>;
    fn register_histogram(&mut self, opts: Opts) -> Result<(), Error>;
    fn register_gauge(&mut self, opts: Opts) -> Result<(), Error>;

    async fn start(&mut self) -> Result<(), Error>;
    async fn shutdown(&mut self) -> Result<(), Error>;

    fn inc_counter(&self, name: &str, labels: &[&str]) -> Result<(), Error>;
    fn observe_hist(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error>;
    fn set_gauge(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error>;
    fn add_gauge(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error>;
}

/// A reporter implementation that does nothing for all methods.
pub struct DummyReporter {}

#[async_trait]
impl Reporter for DummyReporter {
    fn register_counter(&mut self, _opts: Opts) -> Result<(), Error> {
        Ok(())
    }

    fn register_histogram(&mut self, _opts: Opts) -> Result<(), Error> {
        Ok(())
    }

    fn register_gauge(&mut self, _opts: Opts) -> Result<(), Error> {
        Ok(())
    }

    async fn start(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn inc_counter(&self, _name: &str, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }

    fn observe_hist(&self, _name: &str, _value: f64, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }

    fn set_gauge(&self, _name: &str, _value: f64, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }

    fn add_gauge(&self, _name: &str, _value: f64, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }
}

/// Creates buckets that are incremented exponentially.
///
/// # Examples
/// ```
/// let buckets = pitaya_core::metrics::exponential_buckets(1.0, 2.5, 5);
/// assert_eq!(buckets, vec![1.0, 2.5, 6.25, 15.625, 39.0625]);
/// ```
///
/// # Panics
/// This function panics if count is zero, start is less than or equal to zero
/// or factor is smaller than one.
///
/// ```rust,should_panic
/// let _ = pitaya_core::metrics::exponential_buckets(1.0, 2.5, 0);
/// ```
/// ```rust,should_panic
/// let _ = pitaya_core::metrics::exponential_buckets(-1.0, 2.5, 5);
/// ```
/// ```rust,should_panic
/// let _ = pitaya_core::metrics::exponential_buckets(1.0, 0.5, 5);
/// ```
pub fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    assert!(count >= 1);
    assert!(start > 0.0);
    assert!(factor > 1.0);

    let mut next = start;
    let mut buckets = Vec::with_capacity(count);
    for _ in 0..count {
        buckets.push(next);
        next *= factor;
    }

    buckets
}

pub async fn record_histogram_duration<'a>(
    logger: slog::Logger,
    reporter: ThreadSafeReporter,
    name: &'a str,
    start: std::time::Instant,
    labels: &'a [&'a str],
) {
    let elapsed = std::time::Instant::now() - start;
    if let Err(e) = reporter
        .read()
        .await
        .observe_hist(name, elapsed.as_secs_f64(), labels)
    {
        slog::warn!(logger, "observe_hist failed"; "err" => %e);
    }
}

pub async fn add_to_gauge<'a>(
    logger: slog::Logger,
    reporter: ThreadSafeReporter,
    name: &'a str,
    value: f64,
    labels: &'a [&'a str],
) {
    if let Err(e) = reporter.read().await.add_gauge(name, value, labels) {
        slog::warn!(logger, "add_gauge failed"; "err" => %e);
    }
}
