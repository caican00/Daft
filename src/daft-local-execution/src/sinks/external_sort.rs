//! External Sorter for memory-bounded sorting operations.

use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;

use super::{
    merge_iterator::MergeIterator,
    spill::{SortRun, SpillConfig, SpillManager},
};

/// Configuration for the ExternalSorter
#[derive(Clone, Debug)]
pub struct ExternalSorterConfig {
    /// Memory threshold in bytes before spilling a sorted run to disk.
    pub spill_threshold: usize,
    /// Directory for spill files. `None` means spilling is disabled.
    pub spill_dir: Option<String>,
    /// Maximum number of rows per output batch during the merge phase.
    pub output_batch_size: usize,
}

impl Default for ExternalSorterConfig {
    fn default() -> Self {
        Self {
            spill_threshold: 512 * 1024 * 1024, // 512 MiB
            spill_dir: None,
            output_batch_size: 128 * 1024, // 131072 rows
        }
    }
}

/// Parameters for sorting
#[derive(Clone)]
pub struct SortParams {
    pub sort_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
}

/// External sorter that can spill to disk
pub struct ExternalSorter {
    params: SortParams,
    config: ExternalSorterConfig,
    runs: Vec<SortRun>,
    buffer: Vec<Arc<MicroPartition>>,
    buffer_size: usize,
    spill_manager: Option<SpillManager>,
    schema: Option<SchemaRef>,
}

impl ExternalSorter {
    pub fn new(params: SortParams, config: ExternalSorterConfig) -> DaftResult<Self> {
        let spill_manager = if let Some(ref spill_dir) = config.spill_dir {
            let spill_config = SpillConfig::new(spill_dir).with_prefix("daft-sort-spill");
            Some(SpillManager::new(spill_config)?)
        } else {
            None
        };

        Ok(Self {
            params,
            config,
            runs: Vec::new(),
            buffer: Vec::new(),
            buffer_size: 0,
            spill_manager,
            schema: None,
        })
    }

    pub fn add(&mut self, partition: Arc<MicroPartition>) -> DaftResult<()> {
        if self.schema.is_none() {
            self.schema = Some(partition.schema());
        }
        self.buffer_size += partition.size_bytes();
        self.buffer.push(partition);

        if self.buffer_size >= self.config.spill_threshold {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Merges another ExternalSorter into this one by absorbing its runs
    pub fn merge_from(&mut self, mut other: Self) -> DaftResult<()> {
        // Flush buffer of other to ensure all data is in runs
        other.flush_buffer()?;

        // Take runs from other
        self.runs.append(&mut other.runs);

        // Check if we have schema
        if self.schema.is_none() {
            self.schema = other.schema;
        }

        Ok(())
    }

    fn flush_buffer(&mut self) -> DaftResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Sort each buffered partition independently to avoid a large concat
        // that would triple peak memory. Each becomes its own sorted run;
        // MergeIterator handles the K-way merge later.
        for partition in self.buffer.drain(..) {
            let sorted = Arc::new(partition.sort(
                &self.params.sort_by,
                &self.params.descending,
                &self.params.nulls_first,
            )?);

            let run = if let Some(ref spill_manager) = self.spill_manager {
                SortRun::Spilled(spill_manager.spill(&sorted)?)
            } else {
                SortRun::InMemory(sorted)
            };

            self.runs.push(run);
        }

        self.buffer_size = 0;
        Ok(())
    }

    /// Return a streaming merge iterator over all sorted runs.
    /// Each call to `next_batch()` yields one output morsel, keeping
    /// memory bounded to roughly `k * batch_size + output_batch_size`.
    pub fn finish_streaming(mut self) -> DaftResult<SortMergeIterator> {
        self.flush_buffer()?;

        let schema = match self.schema.take() {
            Some(s) => s,
            None => {
                return Ok(SortMergeIterator {
                    inner: SortMergeInner::Empty,
                });
            }
        };

        if self.runs.is_empty() {
            return Ok(SortMergeIterator {
                inner: SortMergeInner::Single(Some(Arc::new(MicroPartition::empty(Some(schema))))),
            });
        }

        if self.runs.len() == 1 {
            let mp = self.runs.pop().unwrap().read()?;
            return Ok(SortMergeIterator {
                inner: SortMergeInner::Single(Some(mp)),
            });
        }

        let iter = self.merge_iter(schema)?;
        Ok(SortMergeIterator {
            inner: SortMergeInner::Merge(iter),
        })
    }

    fn merge_iter(&mut self, schema: SchemaRef) -> DaftResult<MergeIterator> {
        let mut iter = MergeIterator::new(
            schema,
            &self.params.sort_by,
            &self.params.descending,
            &self.params.nulls_first,
            self.config.output_batch_size,
        )?;

        // Add each run as a streaming source
        for run in self.runs.drain(..) {
            let batch_iter = run.stream_batches()?;
            iter.add_streaming_run(batch_iter);
        }

        Ok(iter)
    }
}

/// Streaming iterator over sorted merge output.
/// Wraps the different cases (empty, single run, multi-run merge)
/// behind a uniform `next_batch()` API.
pub struct SortMergeIterator {
    inner: SortMergeInner,
}

enum SortMergeInner {
    Empty,
    Single(Option<Arc<MicroPartition>>),
    Merge(MergeIterator),
}

impl SortMergeIterator {
    /// Yield the next output morsel, or `None` when exhausted.
    pub fn next_batch(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        match &mut self.inner {
            SortMergeInner::Empty => Ok(None),
            SortMergeInner::Single(mp) => Ok(mp.take()),
            SortMergeInner::Merge(iter) => iter.next_merged_batch(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_external_sorter_config_default() {
        let config = ExternalSorterConfig::default();
        assert_eq!(config.spill_threshold, 512 * 1024 * 1024);
        assert!(config.spill_dir.is_none());
        assert_eq!(config.output_batch_size, 128 * 1024);
    }

    #[test]
    fn test_external_sorter_config_custom() {
        let config = ExternalSorterConfig {
            spill_threshold: 1024,
            spill_dir: Some("/tmp/spill".to_string()),
            output_batch_size: 500,
        };
        assert_eq!(config.spill_threshold, 1024);
        assert_eq!(config.spill_dir, Some("/tmp/spill".to_string()));
        assert_eq!(config.output_batch_size, 500);
    }

    #[test]
    fn test_sort_params_clone() {
        let params = SortParams {
            sort_by: vec![],
            descending: vec![true, false],
            nulls_first: vec![false, true],
        };
        let cloned = params.clone();
        assert_eq!(cloned.descending, vec![true, false]);
        assert_eq!(cloned.nulls_first, vec![false, true]);
    }
}
