//! Spill Manager for external sort and other memory-bounded operations.
//!
//! This module provides infrastructure for spilling data to disk when memory
//! limits are exceeded during shuffle reduce operations.
//!
//! Key features:
//! - Uses `tempfile` crate for secure temporary file creation
//! - File naming: `daft-spill-{plan_id}-{node_id}-{uuid}.ipc`
//! - Arrow IPC format for efficient serialization
//! - RAII cleanup via `SpillHandle::Drop`
//! - `ResourceRegistry` as a fail-safe to prevent disk leakage on panics

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tempfile::NamedTempFile;
use uuid::Uuid;

/// A query-level registry that tracks all spill file paths as a fail-safe.
///
/// Even if `SpillHandle::Drop` is bypassed (e.g., due to `std::process::abort`
/// or a panic that unwinds past the handle), the registry can be used to clean
/// up all remaining spill files.
///
/// Usage:
/// - Create one `ResourceRegistry` per query execution.
/// - Pass it to `SpillManager` so every spill file is registered.
/// - Call `cleanup_all()` at query teardown (or rely on `Drop`).
pub struct ResourceRegistry {
    paths: Mutex<HashSet<PathBuf>>,
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self {
            paths: Mutex::new(HashSet::new()),
        }
    }

    /// Register a spill file path.
    pub fn register(&self, path: PathBuf) {
        self.paths.lock().expect("ResourceRegistry lock poisoned").insert(path);
    }

    /// Unregister a spill file path (called when SpillHandle is dropped normally).
    pub fn unregister(&self, path: &Path) {
        self.paths.lock().expect("ResourceRegistry lock poisoned").remove(path);
    }

    /// Clean up all remaining registered spill files.
    /// This is the fail-safe: any file still registered was not cleaned up by its SpillHandle.
    pub fn cleanup_all(&self) {
        let paths: HashSet<PathBuf> = {
            let mut guard = self.paths.lock().expect("ResourceRegistry lock poisoned");
            std::mem::take(&mut *guard)
        };
        for path in &paths {
            if path.exists() {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!("ResourceRegistry: failed to remove spill file {:?}: {}", path, e);
                }
            }
        }
    }
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ResourceRegistry {
    fn drop(&mut self) {
        self.cleanup_all();
    }
}

/// Configuration for the SpillManager
#[derive(Clone, Debug)]
pub struct SpillConfig {
    /// Directory for spill files. Must be a valid local filesystem path.
    pub spill_dir: PathBuf,
    /// Plan ID for file naming (to prevent conflicts between queries)
    pub plan_id: Option<String>,
    /// Node ID for file naming (to identify which node created the spill)
    pub node_id: Option<usize>,
}

impl SpillConfig {
    pub fn new(spill_dir: impl Into<PathBuf>) -> Self {
        Self {
            spill_dir: spill_dir.into(),
            plan_id: None,
            node_id: None,
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.plan_id = Some(prefix.into());
        self
    }

    pub fn with_plan_id(mut self, plan_id: impl Into<String>) -> Self {
        self.plan_id = Some(plan_id.into());
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }
}

/// Handle to a spilled file that implements RAII cleanup.
/// When dropped, the underlying file is automatically deleted and
/// unregistered from the `ResourceRegistry`.
pub struct SpillHandle {
    /// The path to the spill file
    path: PathBuf,
    /// The schema of the spilled data
    schema: SchemaRef,
    /// Number of rows in the spilled data
    num_rows: usize,
    /// Size in bytes of the spilled data
    size_bytes: usize,
    /// Whether to keep the file on drop (for debugging)
    #[allow(dead_code)]
    keep_on_drop: bool,
    /// Optional reference to the ResourceRegistry for fail-safe cleanup
    registry: Option<Arc<ResourceRegistry>>,
}

impl SpillHandle {
    /// Returns the path to the spill file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the schema of the spilled data
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the number of rows in the spilled data
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the size in bytes of the spilled data
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Read the spilled data back into a MicroPartition
    pub fn read(&self) -> DaftResult<Arc<MicroPartition>> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            DaftError::ComputeError(format!("Failed to read spill file {:?}: {}", self.path, e))
        })?;

        let batches: Vec<RecordBatch> = RecordBatch::read_ipc_stream_iter(file)?
            .collect::<DaftResult<Vec<_>>>()?;

        if batches.is_empty() {
            return Ok(Arc::new(MicroPartition::empty(Some(self.schema.clone()))));
        }

        let mp = MicroPartition::new_loaded(self.schema.clone(), Arc::new(batches), None);
        Ok(Arc::new(mp))
    }

    /// Stream the spilled data back as an iterator of RecordBatches
    pub fn stream_batches(
        &self,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<RecordBatch>> + Send>> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            DaftError::ComputeError(format!("Failed to read spill file {:?}: {}", self.path, e))
        })?;
        let reader = RecordBatch::read_ipc_stream_iter(file)?;
        Ok(Box::new(reader))
    }
}

impl Drop for SpillHandle {
    fn drop(&mut self) {
        if self.keep_on_drop {
            return;
        }
        // Unregister from the ResourceRegistry
        if let Some(ref registry) = self.registry {
            registry.unregister(&self.path);
        }
        // Clean up the spill file
        if let Err(e) = std::fs::remove_file(&self.path) {
            // Log warning but don't panic - file might already be deleted
            tracing::warn!("Failed to remove spill file {:?}: {}", self.path, e);
        }
    }
}

/// Manager for creating and tracking spill files.
/// Uses the `tempfile` crate for secure temporary file creation.
pub struct SpillManager {
    config: SpillConfig,
    registry: Option<Arc<ResourceRegistry>>,
}

impl SpillManager {
    /// Create a new SpillManager with the given configuration.
    pub fn new(config: SpillConfig) -> DaftResult<Self> {
        // Ensure the spill directory exists
        std::fs::create_dir_all(&config.spill_dir).map_err(|e| {
            DaftError::ComputeError(format!(
                "Failed to create spill directory {:?}: {}",
                config.spill_dir, e
            ))
        })?;

        Ok(Self { config, registry: None })
    }

    /// Create a new SpillManager with a ResourceRegistry for fail-safe cleanup.
    pub fn with_registry(mut self, registry: Arc<ResourceRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Generate a filename following the design convention:
    /// `daft-spill-{plan_id}-{node_id}-{uuid}.ipc`
    pub(crate) fn generate_filename(&self) -> String {
        let uuid = Uuid::new_v4();
        let plan_part = self.config.plan_id.as_deref().unwrap_or("unknown");
        let node_part = self
            .config
            .node_id
            .map_or("0".to_string(), |n| n.to_string());
        format!("daft-spill-{}-{}-{}.ipc", plan_part, node_part, uuid)
    }

    /// Spill a MicroPartition to disk, returning a handle to the spilled data.
    /// Uses tempfile for secure file creation to prevent conflicts.
    pub fn spill(&self, data: &MicroPartition) -> DaftResult<SpillHandle> {
        let filename = self.generate_filename();
        let path = self.config.spill_dir.join(&filename);

        let schema = data.schema();
        let num_rows = data.len();

        let batches = data.record_batches();

        // Use tempfile to create the file securely, then persist it
        // This provides atomic file creation and prevents race conditions
        let mut temp_file = NamedTempFile::new_in(&self.config.spill_dir).map_err(|e| {
            DaftError::ComputeError(format!(
                "Failed to create temp file in {:?}: {}",
                self.config.spill_dir, e
            ))
        })?;

        if batches.is_empty() {
            let empty = RecordBatch::empty(Some(schema.clone()));
            RecordBatch::write_to_ipc_stream(&[empty], &mut temp_file)?;
        } else {
            RecordBatch::write_to_ipc_stream(batches.as_ref(), &mut temp_file)?;
        }

        let size_bytes = temp_file
            .as_file()
            .metadata()
            .map(|m| m.len() as usize)
            .unwrap_or(0);

        // Persist the temp file to the final path
        temp_file.persist(&path).map_err(|e| {
            DaftError::ComputeError(format!("Failed to persist spill file to {:?}: {}", path, e))
        })?;

        // Register with the ResourceRegistry for fail-safe cleanup
        if let Some(ref registry) = self.registry {
            registry.register(path.clone());
        }

        Ok(SpillHandle {
            path,
            schema,
            num_rows,
            size_bytes,
            keep_on_drop: false,
            registry: self.registry.clone(),
        })
    }

    /// Spill with explicit plan_id and node_id for better tracking
    pub fn spill_with_context(
        &self,
        data: &MicroPartition,
        plan_id: &str,
        node_id: usize,
    ) -> DaftResult<SpillHandle> {
        let uuid = Uuid::new_v4();
        let filename = format!("daft-spill-{}-{}-{}.ipc", plan_id, node_id, uuid);
        let path = self.config.spill_dir.join(&filename);

        let schema = data.schema();
        let num_rows = data.len();

        let batches = data.record_batches();

        let mut temp_file = NamedTempFile::new_in(&self.config.spill_dir).map_err(|e| {
            DaftError::ComputeError(format!(
                "Failed to create temp file in {:?}: {}",
                self.config.spill_dir, e
            ))
        })?;

        if batches.is_empty() {
            let empty = RecordBatch::empty(Some(schema.clone()));
            RecordBatch::write_to_ipc_stream(&[empty], &mut temp_file)?;
        } else {
            RecordBatch::write_to_ipc_stream(batches.as_ref(), &mut temp_file)?;
        }

        let size_bytes = temp_file
            .as_file()
            .metadata()
            .map(|m| m.len() as usize)
            .unwrap_or(0);

        temp_file.persist(&path).map_err(|e| {
            DaftError::ComputeError(format!("Failed to persist spill file to {:?}: {}", path, e))
        })?;

        // Register with the ResourceRegistry for fail-safe cleanup
        if let Some(ref registry) = self.registry {
            registry.register(path.clone());
        }

        Ok(SpillHandle {
            path,
            schema,
            num_rows,
            size_bytes,
            keep_on_drop: false,
            registry: self.registry.clone(),
        })
    }
}

/// A run represents a sorted segment of data, either in memory or spilled to disk.
pub enum SortRun {
    /// Data is held in memory
    InMemory(Arc<MicroPartition>),
    /// Data has been spilled to disk
    Spilled(SpillHandle),
}

impl SortRun {
    /// Get the number of rows in this run
    pub fn num_rows(&self) -> usize {
        match self {
            SortRun::InMemory(mp) => mp.len(),
            SortRun::Spilled(handle) => handle.num_rows(),
        }
    }

    /// Get the schema of this run
    pub fn schema(&self) -> SchemaRef {
        match self {
            SortRun::InMemory(mp) => mp.schema(),
            SortRun::Spilled(handle) => handle.schema().clone(),
        }
    }

    /// Read the data from this run
    pub fn read(&self) -> DaftResult<Arc<MicroPartition>> {
        match self {
            SortRun::InMemory(mp) => Ok(mp.clone()),
            SortRun::Spilled(handle) => handle.read(),
        }
    }

    /// Stream the data from this run as an iterator of RecordBatches
    pub fn stream_batches(
        &self,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<RecordBatch>> + Send>> {
        match self {
            SortRun::InMemory(mp) => {
                let batches = mp.record_batches().into_iter().cloned().collect::<Vec<_>>();
                Ok(Box::new(batches.into_iter().map(Ok)))
            }
            SortRun::Spilled(handle) => handle.stream_batches(),
        }
    }

    /// Get approximate size in bytes
    pub fn size_bytes(&self) -> usize {
        match self {
            SortRun::InMemory(mp) => mp.size_bytes(),
            SortRun::Spilled(handle) => handle.size_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spill_config_new() {
        let config = SpillConfig::new("/tmp/spill");
        assert_eq!(config.spill_dir, std::path::PathBuf::from("/tmp/spill"));
        assert!(config.plan_id.is_none());
        assert!(config.node_id.is_none());
    }

    #[test]
    fn test_spill_config_with_plan_id() {
        let config = SpillConfig::new("/tmp/spill")
            .with_plan_id("plan-123")
            .with_node_id(42);
        assert_eq!(config.plan_id, Some("plan-123".to_string()));
        assert_eq!(config.node_id, Some(42));
    }

    #[test]
    fn test_spill_config_with_prefix() {
        let config = SpillConfig::new("/tmp/spill").with_prefix("my-prefix");
        assert_eq!(config.plan_id, Some("my-prefix".to_string()));
    }

    #[test]
    fn test_spill_manager_creates_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let spill_dir = temp_dir.path().join("spill_subdir");

        assert!(!spill_dir.exists());

        let config = SpillConfig::new(&spill_dir);
        let _manager = SpillManager::new(config).unwrap();

        assert!(spill_dir.exists());
    }

    #[test]
    fn test_filename_format() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SpillConfig::new(temp_dir.path())
            .with_plan_id("myplan")
            .with_node_id(5);
        let manager = SpillManager::new(config).unwrap();
        let filename = manager.generate_filename();

        assert!(filename.starts_with("daft-spill-myplan-5-"));
        assert!(filename.ends_with(".ipc"));
    }

    #[test]
    fn test_filename_format_defaults() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SpillConfig::new(temp_dir.path());
        let manager = SpillManager::new(config).unwrap();
        let filename = manager.generate_filename();

        assert!(filename.starts_with("daft-spill-unknown-0-"));
        assert!(filename.ends_with(".ipc"));
    }

    #[test]
    fn test_resource_registry_register_unregister() {
        let registry = ResourceRegistry::new();
        let path = PathBuf::from("/tmp/test-spill-1.ipc");
        registry.register(path.clone());
        assert!(registry.paths.lock().unwrap().contains(&path));
        registry.unregister(&path);
        assert!(!registry.paths.lock().unwrap().contains(&path));
    }

    #[test]
    fn test_resource_registry_cleanup_all() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test-spill.ipc");
        std::fs::write(&file_path, b"test data").unwrap();
        assert!(file_path.exists());

        let registry = ResourceRegistry::new();
        registry.register(file_path.clone());
        registry.cleanup_all();

        assert!(!file_path.exists());
        assert!(registry.paths.lock().unwrap().is_empty());
    }

    #[test]
    fn test_resource_registry_drop_cleans_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test-spill-drop.ipc");
        std::fs::write(&file_path, b"test data").unwrap();
        assert!(file_path.exists());

        {
            let registry = ResourceRegistry::new();
            registry.register(file_path.clone());
            // registry dropped here
        }

        assert!(!file_path.exists());
    }
}
