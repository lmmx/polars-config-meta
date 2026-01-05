"""Test suite for core metadata functionality of the Polars config meta plugin.

This module contains unit tests for the ConfigMetaPlugin, focusing on metadata
preservation, copying, and special handling of DataFrame operations.
"""

import io

import polars as pl

from polars_config_meta import read_parquet_with_meta, scan_parquet_with_meta


def test_basic_metadata_storage():
    """Test basic set/get metadata on a DataFrame."""
    df = pl.DataFrame({"x": [1, 2, 3]})
    df.config_meta.set(owner="Alice", version=1)
    md = df.config_meta.get_metadata()

    assert md == {
        "owner": "Alice",
        "version": 1,
    }, "Metadata not stored or retrieved properly"


def test_transform_copies_metadata():
    """Test that using df.config_meta.some_method copies metadata to the new DataFrame."""
    df = pl.DataFrame({"val": [10, 20]})
    df.config_meta.set(source="generated", confidence=0.9)
    expected_meta = {"source": "generated", "confidence": 0.9}

    # Use plugin fallback for with_columns
    df2 = df.config_meta.with_columns(doubled=pl.col("val") * 2)
    assert df2.shape == (2, 2), "Unexpected shape after adding a new column"

    md2 = df2.config_meta.get_metadata()
    assert md2 == expected_meta, "Metadata not copied to new DataFrame"

    # Using plain Polars method (without config_meta) won't copy metadata
    df3 = df.with_columns(pl.col("val") * 3)
    md3 = df3.config_meta.get_metadata()
    assert md3 == expected_meta, "Plain df.with_columns should also copy metadata"


def test_merge_metadata():
    """Test merging metadata from multiple DataFrames."""
    df1 = pl.DataFrame({"a": [1]})
    df1.config_meta.set(project="Alpha", stage="dev")
    df2 = pl.DataFrame({"b": [2]})
    df1.config_meta.set(owner="Bob", stage="prod")

    df3 = pl.DataFrame({"c": [3]})
    df3.config_meta.merge(df1, df2)
    merged_md = df3.config_meta.get_metadata()

    # stage from df2 should overwrite stage from df1 if there's a conflict
    assert merged_md == {
        "project": "Alpha",
        "stage": "prod",
        "owner": "Bob",
    }, "Metadata merge did not behave as expected"


def test_parquet_roundtrip_in_memory():
    """Round trip some metadata through a Parquet file and back.

    Test writing to Parquet in memory with df.config_meta.write_parquet,
    then reading back with read_parquet_with_meta to confirm metadata is preserved.
    """
    df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df.config_meta.set(author="Carol", purpose="demo")

    # Write to an in-memory buffer instead of disk
    buffer = io.BytesIO()
    df.config_meta.write_parquet(buffer)
    buffer.seek(0)

    # Read back from the buffer
    df_in = read_parquet_with_meta(buffer)
    assert df_in.shape == (2, 2), "Data shape changed on Parquet roundtrip"
    md_in = df_in.config_meta.get_metadata()
    assert md_in == {
        "author": "Carol",
        "purpose": "demo",
    }, "Metadata lost or altered in roundtrip"


def test_scan_parquet_with_metadata():
    """Test reading Parquet file with metadata using scan_parquet."""
    df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).config_meta.lazy()

    meta_data = {
        "author": "David",
        "purpose": "test",
    }

    df.config_meta.set(**meta_data)

    # Write to a temporary file
    path = "test.parquet"
    df.config_meta.write_parquet(path)

    # Read back with scan_parquet
    df_in = scan_parquet_with_meta(path)
    md_in = df_in.config_meta.get_metadata()
    assert md_in == meta_data, "Metadata lost or altered in scan"

    # Add a new column
    df_in = df_in.config_meta.with_columns(new_col=pl.col("col1") * 2)

    md_in = df_in.config_meta.get_metadata()
    assert md_in == meta_data, "Metadata lost or altered in scan"

    # collect to dataframe and check that the same is correct
    df_in = df_in.config_meta.collect()
    assert df_in.shape == (2, 3), "Data shape changed on Parquet roundtrip"

    # check that metadata persists after collect
    md_in = df_in.config_meta.get_metadata()
    assert md_in == meta_data, "Metadata lost or altered in scan"

    # Clean up
    import os

    os.remove(path)


def test_basic_series_metadata_storage():
    """Test basic set/get metadata on a Series."""
    s = pl.Series("x", [1, 2, 3])
    s.config_meta.set(owner="Alice", version=1)
    md = s.config_meta.get_metadata()

    assert md == {
        "owner": "Alice",
        "version": 1,
    }, "Metadata not stored or retrieved properly on Series"


def test_series_parquet_roundtrip_in_memory():
    """Test Series parquet round-trip preserves metadata.

    Series gets converted to single-column DataFrame for parquet storage.
    """
    s = pl.Series("col1", [1, 2, 3])
    s.config_meta.set(author="Carol", purpose="series_demo")

    buffer = io.BytesIO()
    s.config_meta.write_parquet(buffer)
    buffer.seek(0)

    # Reads back as DataFrame (parquet doesn't have Series concept)
    df_in = read_parquet_with_meta(buffer)
    assert df_in.shape == (3, 1), "Data shape changed on Parquet roundtrip"
    md_in = df_in.config_meta.get_metadata()
    assert md_in == {
        "author": "Carol",
        "purpose": "series_demo",
    }, "Metadata lost or altered in Series parquet roundtrip"


def test_merge_metadata_across_types():
    """Test merging metadata between DataFrames and Series."""
    df = pl.DataFrame({"a": [1]})
    df.config_meta.set(source="dataframe", shared="from_df")

    s = pl.Series("b", [2])
    s.config_meta.set(source="series", extra="from_series")

    # Merge Series metadata into DataFrame
    df2 = pl.DataFrame({"c": [3]})
    df2.config_meta.merge(df, s)
    md = df2.config_meta.get_metadata()

    assert md == {
        "source": "series",  # Series overwrites DataFrame (later wins)
        "shared": "from_df",
        "extra": "from_series",
    }, "Cross-type merge did not behave as expected"

    # Merge DataFrame metadata into Series
    s2 = pl.Series("d", [4])
    s2.config_meta.merge(s, df)
    md2 = s2.config_meta.get_metadata()

    assert md2 == {
        "source": "dataframe",  # DataFrame overwrites Series (later wins)
        "shared": "from_df",
        "extra": "from_series",
    }, "Cross-type merge into Series did not behave as expected"
