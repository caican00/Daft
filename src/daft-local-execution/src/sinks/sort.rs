use std::sync::{Arc, Mutex};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult,
        BlockingSinkSinkResult,
    },
    external_sort::{
        ExternalSorter, ExternalSorterConfig, SortMergeIterator,
        SortParams as ExtSortParams,
    },
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) enum SortState {
    Building(ExternalSorter),
    /// Streaming merge phase — yields one morsel per `finalize` call.
    Draining(Arc<Mutex<SortMergeIterator>>),
    Done,
}

impl SortState {
    fn push(&mut self, part: Arc<MicroPartition>) -> DaftResult<()> {
        if let Self::Building(sorter) = self {
            sorter.add(part)
        } else {
            panic!("SortSink should be in Building state");
        }
    }
}

struct SortParams {
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
}

/// Configuration for sort spilling
#[derive(Clone, Debug)]
pub struct SortSpillConfig {
    /// Memory threshold in bytes before spilling
    pub spill_threshold: Option<usize>,
    /// Directory for spill files (None = spilling disabled)
    pub spill_dir: Option<String>,
}

impl Default for SortSpillConfig {
    fn default() -> Self {
        Self {
            spill_threshold: None,
            spill_dir: None,
        }
    }
}

pub struct SortSink {
    params: Arc<SortParams>,
    spill_config: SortSpillConfig,
}

impl SortSink {
    pub fn new(sort_by: Vec<BoundExpr>, descending: Vec<bool>, nulls_first: Vec<bool>) -> Self {
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
                nulls_first,
            }),
            spill_config: SortSpillConfig::default(),
        }
    }

    pub fn with_spill_config(mut self, spill_config: SortSpillConfig) -> Self {
        self.spill_config = spill_config;
        self
    }

    /// Check if spilling is enabled
    fn spilling_enabled(&self) -> bool {
        self.spill_config.spill_threshold.is_some() && self.spill_config.spill_dir.is_some()
    }
}

impl BlockingSink for SortSink {
    type State = SortState;

    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        if let Err(e) = state.push(input) {
            return Err(e).into();
        }
        Ok(state).into()
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        spawner
            .spawn(
                async move {
                    let mut merge_iter: Option<Arc<Mutex<SortMergeIterator>>> = None;
                    let mut main_sorter: Option<ExternalSorter> = None;

                    for state in states {
                        match state {
                            SortState::Draining(iter_mutex) => {
                                if merge_iter.is_some() {
                                    return Err(DaftError::InternalError(
                                        "SortSink finalize: multiple Draining states".into(),
                                    ));
                                }
                                merge_iter = Some(iter_mutex);
                            }
                            SortState::Building(sorter) => {
                                if let Some(main) = &mut main_sorter {
                                    main.merge_from(sorter)?;
                                } else {
                                    main_sorter = Some(sorter);
                                }
                            }
                            SortState::Done => {}
                        }
                    }

                    // First call: merge all sorters into a streaming iterator
                    if merge_iter.is_none() {
                        match main_sorter {
                            Some(sorter) => {
                                merge_iter = Some(Arc::new(Mutex::new(sorter.finish_streaming()?)));
                            }
                            None => {
                                return Ok(BlockingSinkFinalizeOutput::Finished(vec![]));
                            }
                        }
                    }

                    let iter_mutex = merge_iter.unwrap();
                    let mut iter = iter_mutex.lock().unwrap_or_else(|e| e.into_inner());

                    // Drain one batch from the iterator
                    match iter.next_batch()? {
                        Some(batch) => {
                            drop(iter);
                            Ok(BlockingSinkFinalizeOutput::HasMoreOutput {
                                states: vec![SortState::Draining(iter_mutex)],
                                output: vec![batch],
                            })
                        }
                        None => {
                            Ok(BlockingSinkFinalizeOutput::Finished(vec![]))
                        }
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Sort".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Sort
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        assert!(!self.params.sort_by.is_empty());
        let pairs = self
            .params
            .sort_by
            .iter()
            .zip(self.params.descending.iter())
            .zip(self.params.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");
        lines.push(format!("Sort: Sort by = {}", pairs));
        if self.spilling_enabled() {
            lines.push(format!(
                "Spill: threshold={:?}, dir={:?}",
                self.spill_config.spill_threshold, self.spill_config.spill_dir
            ));
        }
        lines
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let spill_config = &self.spill_config;
        let ext_config = ExternalSorterConfig {
            spill_threshold: spill_config
                .spill_threshold
                .unwrap_or(512 * 1024 * 1024), // bytes
            spill_dir: spill_config.spill_dir.clone(),
            output_batch_size: 128 * 1024, // rows
        };
        let ext_params = ExtSortParams {
            sort_by: self.params.sort_by.clone(),
            descending: self.params.descending.clone(),
            nulls_first: self.params.nulls_first.clone(),
        };

        let sorter = ExternalSorter::new(ext_params, ext_config)?;
        Ok(SortState::Building(sorter))
    }
}
