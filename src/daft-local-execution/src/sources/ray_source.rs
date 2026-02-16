//! RaySourceNode - Streaming source for Ray shuffle data.
//!
//! This module implements a Rust-side source node that uses PyO3 to invoke
//! a Python iterator for streaming shuffle data from Ray. This follows the
//! design specification for "Streaming Shuffle Fetching (Python <-> Rust)".
//!
//! Key features:
//! - Uses PyO3 to call Python iterator that invokes ray.get on individual chunks
//! - Small prefetch buffer (1-2 morsels) to hide latency without causing OOM
//! - Integrates with the local execution pipeline

use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;

use super::source::Source;
use crate::{pipeline::NodeName, sources::source::SourceStream};

/// Configuration for the RaySourceNode
#[derive(Clone, Debug)]
pub struct RaySourceConfig {
    /// Number of morsels to prefetch (1-2 recommended)
    pub prefetch_buffer_size: usize,
}

impl Default for RaySourceConfig {
    fn default() -> Self {
        Self {
            prefetch_buffer_size: 2,
        }
    }
}

/// A source node that streams data from Ray ObjectRefs via Python callbacks.
///
/// This node implements the streaming bridge between Python (Ray) and Rust
/// (local execution pipeline). Instead of calling ray.get() on all partitions
/// at once, it uses a Python iterator to fetch chunks on-demand.
///
/// # Design
///
/// The RaySourceNode works by:
/// 1. Receiving a Python iterator (via PyO3) that yields MicroPartitions
/// 2. Using a small prefetch buffer to hide latency
/// 3. Yielding morsels to the Rust pipeline as they become available
///
/// This prevents OOM by avoiding materializing all shuffle data at once.
#[cfg(feature = "python")]
pub struct RaySourceNode {
    /// Schema of the data
    schema: SchemaRef,
    /// Configuration
    config: RaySourceConfig,
    /// Python iterator reference (stored as PyObject)
    py_iterator: Option<pyo3::Py<pyo3::types::PyAny>>,
}

#[cfg(feature = "python")]
impl RaySourceNode {
    /// Create a new RaySourceNode with a Python iterator.
    ///
    /// # Arguments
    /// * `schema` - Schema of the data being streamed
    /// * `py_iterator` - Python iterator that yields MicroPartitions
    /// * `config` - Configuration for prefetching
    pub fn new(
        schema: SchemaRef,
        py_iterator: pyo3::Py<pyo3::types::PyAny>,
        config: RaySourceConfig,
    ) -> Self {
        Self {
            schema,
            config,
            py_iterator: Some(py_iterator),
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[cfg(feature = "python")]
#[async_trait]
impl Source for RaySourceNode {
    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "RaySource: BufferSize={}",
            self.config.prefetch_buffer_size
        )];
        lines.push(format!("Schema = {}", self.schema));
        lines
    }

    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        use common_error::DaftError;
        use daft_micropartition::{MicroPartition, python::PyMicroPartition};
        use pyo3::prelude::*;
        use tokio::sync::mpsc;

        let py_iterator =
            Python::with_gil(|py| self.py_iterator.as_ref().map(|obj| obj.clone_ref(py)))
                .ok_or_else(|| {
                    DaftError::ComputeError("RaySourceNode: Python iterator not set".to_string())
                })?;

        let prefetch_size = self.config.prefetch_buffer_size;
        let (tx, rx) = mpsc::channel::<DaftResult<Arc<MicroPartition>>>(prefetch_size);

        // Spawn a blocking task to iterate over the Python iterator
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let iterator = py_iterator.bind(py);

                // Iterate over the Python iterator
                loop {
                    match iterator.call_method0("__next__") {
                        Ok(result) => {
                            // Extract MicroPartition from Python object
                            match result.extract::<PyMicroPartition>() {
                                Ok(py_mp) => {
                                    let result = Ok(py_mp.inner.clone());
                                    // Release GIL while sending to channel to avoid blocking other Python threads
                                    // if the channel is full (backpressure)
                                    if py.allow_threads(|| tx.blocking_send(result)).is_err() {
                                        // Receiver dropped, stop iteration
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let error = Err(DaftError::ComputeError(format!(
                                        "Failed to extract MicroPartition: {}",
                                        e
                                    )));
                                    let _ = py.allow_threads(|| tx.blocking_send(error));
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            // Check if it's StopIteration (normal end of iteration)
                            if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                                // Normal end of iteration
                                break;
                            } else {
                                // Actual error
                                let error =
                                    Err(DaftError::ComputeError(format!("Python iterator error: {}", e)));
                                let _ = py.allow_threads(|| tx.blocking_send(error));
                                break;
                            }
                        }
                    }
                }
            });
        });

        // Convert the receiver into a stream
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }

    fn name(&self) -> NodeName {
        "RaySource".into()
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

/// Non-Python stub for RaySourceNode when Python feature is not enabled
#[cfg(not(feature = "python"))]
pub struct RaySourceNode {
    schema: SchemaRef,
}

#[cfg(not(feature = "python"))]
impl RaySourceNode {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[cfg(not(feature = "python"))]
#[async_trait]
impl Source for RaySourceNode {
    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["RaySource (No Python Support)".to_string()]
    }

    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        Err(common_error::DaftError::ComputeError(
            "RaySourceNode requires Python feature".to_string(),
        ))
    }

    fn name(&self) -> NodeName {
        "RaySource".into()
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ray_source_config_default() {
        let config = RaySourceConfig::default();
        assert_eq!(config.prefetch_buffer_size, 2);
    }

    #[test]
    fn test_ray_source_config_custom() {
        let config = RaySourceConfig {
            prefetch_buffer_size: 4,
        };
        assert_eq!(config.prefetch_buffer_size, 4);
    }

    #[test]
    fn test_ray_source_config_clone() {
        let config = RaySourceConfig {
            prefetch_buffer_size: 8,
        };
        let cloned = config.clone();
        assert_eq!(cloned.prefetch_buffer_size, 8);
    }
}
