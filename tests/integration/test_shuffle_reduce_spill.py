"""Integration tests for shuffle reduce spill functionality.

These tests verify that the shuffle reduce operations (Sort, Aggregate) can
handle large datasets by spilling to disk when memory limits are exceeded.
"""

from __future__ import annotations

import tempfile

import pytest

import daft
from daft import context


@pytest.fixture
def spill_dir():
    """Create a temporary directory for spill files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestSortWithSpill:
    """Tests for Sort operation with spilling enabled."""

    def test_sort_with_spill_config(self, spill_dir):
        """Test that sort works with spill configuration enabled."""
        # Configure spilling
        context.set_execution_config(
            shuffle_spill_threshold=1024 * 1024,  # 1MB threshold
            shuffle_reduce_spill_dir=spill_dir,
        )

        # Create a small dataframe
        df = daft.from_pydict(
            {
                "a": list(range(1000)),
                "b": ["x"] * 1000,
            }
        )

        # Sort and collect
        result = df.sort("a", desc=True).collect()

        # Verify results
        assert len(result) == 1000
        a_values = result.to_pydict()["a"]
        assert a_values == list(range(999, -1, -1))

    def test_sort_without_spill_config(self):
        """Test that sort works normally without spill configuration."""
        # Reset spill configuration
        context.set_execution_config(
            shuffle_spill_threshold=None,
            shuffle_reduce_spill_dir=None,
        )

        # Create a small dataframe
        df = daft.from_pydict(
            {
                "a": list(range(100)),
                "b": ["x"] * 100,
            }
        )

        # Sort and collect
        result = df.sort("a", desc=True).collect()

        # Verify results
        assert len(result) == 100
        a_values = result.to_pydict()["a"]
        assert a_values == list(range(99, -1, -1))

    def test_sort_multi_column(self, spill_dir):
        """Test multi-column sorting with spill enabled."""
        context.set_execution_config(
            shuffle_spill_threshold=1024 * 1024,
            shuffle_reduce_spill_dir=spill_dir,
        )

        df = daft.from_pydict(
            {
                "a": [1, 2, 1, 2, 1, 2],
                "b": [3, 1, 2, 3, 1, 2],
            }
        )

        result = df.sort(["a", "b"]).collect()
        result_dict = result.to_pydict()

        # Verify sorted order
        expected_a = [1, 1, 1, 2, 2, 2]
        expected_b = [1, 2, 3, 1, 2, 3]
        assert result_dict["a"] == expected_a
        assert result_dict["b"] == expected_b

    def test_sort_with_nulls(self, spill_dir):
        """Test sorting with null values and spill enabled."""
        context.set_execution_config(
            shuffle_spill_threshold=1024 * 1024,
            shuffle_reduce_spill_dir=spill_dir,
        )

        df = daft.from_pydict(
            {
                "a": [3, None, 1, None, 2],
            }
        )

        # Sort with nulls first
        result = df.sort("a", nulls_first=True).collect()
        a_values = result.to_pydict()["a"]

        # First two should be None
        assert a_values[0] is None
        assert a_values[1] is None
        # Rest should be sorted
        assert a_values[2:] == [1, 2, 3]


class TestSpillConfigValidation:
    """Tests for spill configuration validation."""

    @pytest.mark.parametrize(
        "remote_uri",
        [
            "s3://bucket/path",
            "gs://bucket/path",
            "az://container/path",
            "hdfs://namenode/path",
            "http://example.com/path",
            "https://example.com/path",
        ],
    )
    def test_remote_uri_rejected(self, remote_uri):
        """Test that remote URIs are rejected for spill directory."""
        with pytest.raises(ValueError, match="local filesystem path"):
            context.set_execution_config(
                shuffle_reduce_spill_dir=remote_uri,
            )

    def test_local_path_accepted(self, spill_dir):
        """Test that local paths are accepted for spill directory."""
        # This should not raise
        context.set_execution_config(
            shuffle_reduce_spill_dir=spill_dir,
        )

        # Verify config was set
        ctx = context.get_context()
        config = ctx.daft_execution_config
        assert config.shuffle_reduce_spill_dir == spill_dir

    def test_relative_path_accepted(self, spill_dir):
        """Test that relative paths are accepted for spill directory."""
        # This should not raise
        context.set_execution_config(
            shuffle_reduce_spill_dir="./spill_data",
        )
