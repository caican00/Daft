//! K-Way merge iterator for sorted runs.
//!
//! Uses arrow-row RowConverter for binary-comparable sort keys and a BinaryHeap
//! to merge K sorted runs (in-memory or spilled) into a single sorted stream.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use arrow_row::{RowConverter, SortField};
use arrow_schema::SortOptions;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

struct RunCursor {
    batch: RecordBatch,
    row_idx: usize,
    /// Pre-encoded sort keys for all rows in `batch`.
    encoded_keys: arrow_row::Rows,
}

impl RunCursor {
    fn new(
        batch: RecordBatch,
        converter: &mut RowConverter,
        sort_key_indices: &SortKeyIndices,
    ) -> DaftResult<Self> {
        let encoded_keys = encode_batch_keys(converter, sort_key_indices, &batch)?;
        Ok(Self {
            batch,
            row_idx: 0,
            encoded_keys,
        })
    }

    fn is_exhausted(&self) -> bool {
        self.row_idx >= self.batch.len()
    }

    fn advance(&mut self) {
        self.row_idx += 1;
    }

    fn current_sort_key(&self) -> Box<[u8]> {
        debug_assert!(!self.is_exhausted(), "current_sort_key called on exhausted cursor");
        self.encoded_keys.row(self.row_idx).as_ref().into()
    }
}

struct HeapEntry {
    sort_key: Box<[u8]>,
    run_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}
impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.sort_key.cmp(&self.sort_key)
    }
}

// ---------------------------------------------------------------------------
// SortedRunSource
// ---------------------------------------------------------------------------

enum SortedRunSource {
    InMemory {
        batches: Vec<RecordBatch>,
        next_idx: usize,
    },
    Streaming {
        iter: Box<dyn Iterator<Item = DaftResult<RecordBatch>> + Send>,
    },
}

impl SortedRunSource {
    fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>> {
        match self {
            Self::InMemory { batches, next_idx } => {
                if *next_idx < batches.len() {
                    let batch = batches[*next_idx].clone();
                    *next_idx += 1;
                    Ok(Some(batch))
                } else {
                    Ok(None)
                }
            }
            Self::Streaming { iter } => match iter.next() {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// MergeIterator
// ---------------------------------------------------------------------------

struct SortKeyIndices(Vec<usize>);

pub struct MergeIterator {
    schema: SchemaRef,
    sort_key_indices: SortKeyIndices,
    converter: RowConverter,
    runs: Vec<SortedRunSource>,
    cursors: Vec<Option<RunCursor>>,
    heap: BinaryHeap<HeapEntry>,
    output_batch_size: usize,
    initialized: bool,
}

impl MergeIterator {
    pub fn new(
        schema: SchemaRef,
        sort_by: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        output_batch_size: usize,
    ) -> DaftResult<Self> {
        let mut key_indices = Vec::with_capacity(sort_by.len());
        for expr in sort_by {
            let name = expr.as_ref().name();
            let idx = schema
                .fields()
                .iter()
                .position(|f| f.name == name)
                .ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "Sort key column '{}' not found in schema",
                        name
                    ))
                })?;
            key_indices.push(idx);
        }

        let sort_fields: Vec<SortField> = key_indices
            .iter()
            .zip(descending.iter())
            .zip(nulls_first.iter())
            .map(|((&col_idx, &desc), &nf)| {
                let arrow_dt = schema.fields()[col_idx].dtype.to_arrow().unwrap();
                SortField::new_with_options(
                    arrow_dt,
                    SortOptions {
                        descending: desc,
                        nulls_first: nf,
                    },
                )
            })
            .collect();

        let converter = RowConverter::new(sort_fields)
            .map_err(|e| DaftError::ComputeError(format!("Failed to create RowConverter: {e}")))?;

        Ok(Self {
            schema,
            sort_key_indices: SortKeyIndices(key_indices),
            converter,
            runs: Vec::new(),
            cursors: Vec::new(),
            heap: BinaryHeap::new(),
            output_batch_size,
            initialized: false,
        })
    }

    pub fn add_in_memory_run(&mut self, batches: Vec<RecordBatch>) {
        self.runs.push(SortedRunSource::InMemory {
            batches,
            next_idx: 0,
        });
    }

    pub fn add_streaming_run(
        &mut self,
        iter: Box<dyn Iterator<Item = DaftResult<RecordBatch>> + Send>,
    ) {
        self.runs.push(SortedRunSource::Streaming { iter });
    }

    /// Initialize: load first batch from each run and seed the heap.
    fn initialize(&mut self) -> DaftResult<()> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        let num_runs = self.runs.len();
        self.cursors = (0..num_runs).map(|_| None).collect();

        for run_idx in 0..num_runs {
            if let Some(batch) = self.runs[run_idx].next_batch()? {
                if batch.len() > 0 {
                    let cursor = RunCursor::new(
                        batch,
                        &mut self.converter,
                        &self.sort_key_indices,
                    )?;
                    self.cursors[run_idx] = Some(cursor);
                }
            }
        }

        // Seed heap with first row from each active cursor
        for run_idx in 0..num_runs {
            if let Some(cursor) = &self.cursors[run_idx] {
                self.heap.push(HeapEntry {
                    sort_key: cursor.current_sort_key(),
                    run_idx,
                });
            }
        }

        Ok(())
    }

    /// Try to refill a cursor from its run source. Returns true if refilled.
    fn refill_cursor(&mut self, run_idx: usize) -> DaftResult<bool> {
        if let Some(batch) = self.runs[run_idx].next_batch()? {
            if batch.len() > 0 {
                let cursor = RunCursor::new(
                    batch,
                    &mut self.converter,
                    &self.sort_key_indices,
                )?;
                self.cursors[run_idx] = Some(cursor);
                return Ok(true);
            }
        }
        self.cursors[run_idx] = None;
        Ok(false)
    }

    /// Produce the next merged output batch (up to output_batch_size rows).
    pub fn next_merged_batch(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        self.initialize()?;

        if self.heap.is_empty() {
            return Ok(None);
        }

        // Collect (batch_list_index, row_idx) pairs for interleave.
        // Each run's current batch is tracked; when a cursor advances to a new
        // batch (after refill), a new entry is added to `batches`.
        let mut batches: Vec<RecordBatch> = Vec::new();
        // Maps run_idx -> current batch index in `batches`
        let mut run_batch_idx: HashMap<usize, usize> = HashMap::new();
        let mut indices: Vec<(usize, usize)> = Vec::new();
        let mut count = 0usize;

        while count < self.output_batch_size {
            let entry = match self.heap.pop() {
                Some(e) => e,
                None => break,
            };

            let run_idx = entry.run_idx;

            // Take the cursor out temporarily
            let mut cursor = match self.cursors[run_idx].take() {
                Some(c) => c,
                None => continue,
            };

            // Get or register this run's current batch in the batch list
            let batch_idx = *run_batch_idx.entry(run_idx).or_insert_with(|| {
                let idx = batches.len();
                batches.push(cursor.batch.clone());
                idx
            });
            indices.push((batch_idx, cursor.row_idx));
            count += 1;

            cursor.advance();

            if cursor.is_exhausted() {
                // Try to load next batch from this run
                if self.refill_cursor(run_idx)? {
                    if let Some(new_cursor) = &self.cursors[run_idx] {
                        // Register the new batch
                        let new_idx = batches.len();
                        batches.push(new_cursor.batch.clone());
                        run_batch_idx.insert(run_idx, new_idx);

                        self.heap.push(HeapEntry {
                            sort_key: new_cursor.current_sort_key(),
                            run_idx,
                        });
                    }
                }
            } else {
                // Cursor still has rows; push back with current sort key
                let key = cursor.current_sort_key();
                self.cursors[run_idx] = Some(cursor);
                self.heap.push(HeapEntry {
                    sort_key: key,
                    run_idx,
                });
            }
        }

        if indices.is_empty() {
            return Ok(None);
        }

        // Convert Daft RecordBatches to Arrow RecordBatches for interleave
        let arrow_batches: Vec<arrow_array::RecordBatch> = batches
            .into_iter()
            .map(|rb| rb.try_into())
            .collect::<DaftResult<Vec<_>>>()?;
        let arrow_refs: Vec<&arrow_array::RecordBatch> = arrow_batches.iter().collect();

        let merged_arrow = arrow_select::interleave::interleave_record_batch(&arrow_refs, &indices)
            .map_err(|e| DaftError::ComputeError(format!("interleave error: {e}")))?;

        // Convert back to Daft RecordBatch
        let arrow_arrays: Vec<arrow_array::ArrayRef> = merged_arrow.columns().to_vec();
        let merged = RecordBatch::from_arrow(self.schema.clone(), arrow_arrays)?;
        let mp = MicroPartition::new_loaded(
            self.schema.clone(),
            Arc::new(vec![merged]),
            None,
        );
        Ok(Some(Arc::new(mp)))
    }
}

/// Encode sort keys for all rows in a batch at once.
/// Returns a `Rows` object that supports O(1) row-level access.
fn encode_batch_keys(
    converter: &mut RowConverter,
    sort_key_indices: &SortKeyIndices,
    batch: &RecordBatch,
) -> DaftResult<arrow_row::Rows> {
    let key_arrays: Vec<arrow_array::ArrayRef> = sort_key_indices
        .0
        .iter()
        .map(|&col_idx| {
            let series = batch.get_column(col_idx);
            series.to_arrow()
        })
        .collect::<DaftResult<Vec<_>>>()?;

    converter
        .convert_columns(&key_arrays)
        .map_err(|e| DaftError::ComputeError(format!("RowConverter error: {e}")))
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};

    use super::*;

    /// Helper: build a single-column RecordBatch of Int64 values.
    fn int64_batch(name: &str, values: &[i64]) -> RecordBatch {
        let series = Int64Array::from_vec(name, values.to_vec()).into_series();
        RecordBatch::from_nonempty_columns(vec![series]).unwrap()
    }

    /// Helper: build a BoundExpr for a column by name against a schema.
    fn bound_sort_expr(schema: &Schema, col_name: &str) -> BoundExpr {
        BoundExpr::try_new(resolved_col(col_name), schema).unwrap()
    }

    /// Helper: create a MergeIterator with ascending sort on a single column.
    fn make_merge_iter(
        schema: SchemaRef,
        sort_col: &str,
        output_batch_size: usize,
    ) -> MergeIterator {
        let sort_expr = bound_sort_expr(&schema, sort_col);
        MergeIterator::new(
            schema,
            &[sort_expr],
            &[false], // ascending
            &[false], // nulls last
            output_batch_size,
        )
        .unwrap()
    }

    /// Drain all batches from a MergeIterator and collect the sort column values.
    fn drain_int64_values(iter: &mut MergeIterator, col_idx: usize) -> Vec<i64> {
        let mut result = Vec::new();
        while let Some(mp) = iter.next_merged_batch().unwrap() {
            if let Some(batch) = mp.concat_or_get().unwrap() {
                let series = batch.get_column(col_idx);
                let arr = series.i64().unwrap();
                for i in 0..arr.len() {
                    result.push(arr.get(i).unwrap());
                }
            }
        }
        result
    }

    #[test]
    fn test_merge_two_runs_ascending() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let mut iter = make_merge_iter(schema, "x", 1024);

        // Run 0: [1, 3, 5]
        iter.add_in_memory_run(vec![int64_batch("x", &[1, 3, 5])]);
        // Run 1: [2, 4, 6]
        iter.add_in_memory_run(vec![int64_batch("x", &[2, 4, 6])]);

        let values = drain_int64_values(&mut iter, 0);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_merge_single_run() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let mut iter = make_merge_iter(schema, "x", 1024);

        iter.add_in_memory_run(vec![int64_batch("x", &[10, 20, 30])]);

        let values = drain_int64_values(&mut iter, 0);
        assert_eq!(values, vec![10, 20, 30]);
    }

    #[test]
    fn test_merge_three_runs() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let mut iter = make_merge_iter(schema, "x", 1024);

        iter.add_in_memory_run(vec![int64_batch("x", &[1, 4, 7])]);
        iter.add_in_memory_run(vec![int64_batch("x", &[2, 5, 8])]);
        iter.add_in_memory_run(vec![int64_batch("x", &[3, 6, 9])]);

        let values = drain_int64_values(&mut iter, 0);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_merge_respects_output_batch_size() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        // output_batch_size = 2, so each morsel should have at most 2 rows
        let mut iter = make_merge_iter(schema, "x", 2);

        iter.add_in_memory_run(vec![int64_batch("x", &[1, 3, 5])]);
        iter.add_in_memory_run(vec![int64_batch("x", &[2, 4, 6])]);

        let mut batch_sizes = Vec::new();
        let mut all_values = Vec::new();
        while let Some(mp) = iter.next_merged_batch().unwrap() {
            if let Some(rb) = mp.concat_or_get().unwrap() {
                batch_sizes.push(rb.len());
                let arr = rb.get_column(0).i64().unwrap();
                for i in 0..arr.len() {
                    all_values.push(arr.get(i).unwrap());
                }
            }
        }
        assert_eq!(all_values, vec![1, 2, 3, 4, 5, 6]);
        // Each batch should have at most 2 rows
        for &sz in &batch_sizes {
            assert!(sz <= 2, "batch size {sz} exceeds output_batch_size 2");
        }
    }

    #[test]
    fn test_merge_multi_batch_run() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let mut iter = make_merge_iter(schema, "x", 1024);

        // Run 0 has two batches: [1, 3] and [5, 7]
        iter.add_in_memory_run(vec![
            int64_batch("x", &[1, 3]),
            int64_batch("x", &[5, 7]),
        ]);
        // Run 1 has one batch: [2, 4, 6]
        iter.add_in_memory_run(vec![int64_batch("x", &[2, 4, 6])]);

        let values = drain_int64_values(&mut iter, 0);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_merge_descending() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let sort_expr = bound_sort_expr(&schema, "x");
        let mut iter = MergeIterator::new(
            schema,
            &[sort_expr],
            &[true],  // descending
            &[false], // nulls last
            1024,
        )
        .unwrap();

        // Runs are sorted in descending order
        iter.add_in_memory_run(vec![int64_batch("x", &[6, 4, 2])]);
        iter.add_in_memory_run(vec![int64_batch("x", &[5, 3, 1])]);

        let values = drain_int64_values(&mut iter, 0);
        assert_eq!(values, vec![6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_merge_no_runs_returns_none() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let mut iter = make_merge_iter(schema, "x", 1024);

        // No runs added
        let result = iter.next_merged_batch().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_streaming_run() {
        let schema: SchemaRef = Schema::new(vec![Field::new("x", DataType::Int64)]).into();
        let mut iter = make_merge_iter(schema, "x", 1024);

        // Add a streaming run via an iterator
        let batches = vec![
            Ok(int64_batch("x", &[1, 4])),
            Ok(int64_batch("x", &[7, 10])),
        ];
        iter.add_streaming_run(Box::new(batches.into_iter()));

        // Add an in-memory run
        iter.add_in_memory_run(vec![int64_batch("x", &[2, 5, 8])]);

        let values = drain_int64_values(&mut iter, 0);
        assert_eq!(values, vec![1, 2, 4, 5, 7, 8, 10]);
    }
}
