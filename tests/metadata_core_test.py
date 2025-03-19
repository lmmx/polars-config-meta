import io

import polars as pl

from polars_config_meta import read_parquet_with_meta, scan_parquet_with_meta


def test_basic_metadata_storage():
    """
    Test basic set/get metadata on a DataFrame.
    """
    df = pl.DataFrame({"x": [1, 2, 3]})
    df.config_meta.set(owner="Alice", version=1)
    md = df.config_meta.get_metadata()

    assert md == {
        "owner": "Alice",
        "version": 1,
    }, "Metadata not stored or retrieved properly"


def test_transform_copies_metadata():
    """
    Test that using df.config_meta.some_method copies metadata to the new DataFrame.
    """
    df = pl.DataFrame({"val": [10, 20]})
    df.config_meta.set(source="generated", confidence=0.9)

    # Use plugin fallback for with_columns
    df2 = df.config_meta.with_columns(doubled=pl.col("val") * 2)
    assert df2.shape == (2, 2), "Unexpected shape after adding a new column"

    md2 = df2.config_meta.get_metadata()
    assert md2 == {
        "source": "generated",
        "confidence": 0.9,
    }, "Metadata not copied to new DataFrame"

    # Using plain Polars method (without config_meta) won't copy metadata
    df3 = df.with_columns(pl.col("val") * 3)
    md3 = df3.config_meta.get_metadata()
    assert md3 == {}, "Plain df.with_columns should not copy metadata"


def test_merge_metadata():
    """
    Test merging metadata from multiple DataFrames.
    """
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


def test_no_copy_for_non_df_result():
    """
    Test that if a method returns something other than a DataFrame (e.g. a Series),
    we do not attempt to copy metadata.
    """
    df = pl.DataFrame({"x": [3, 6, 9]})
    df.config_meta.set(description="Test Series return")

    # This returns a Series
    s = df.config_meta.select("x")
    assert isinstance(s, pl.DataFrame), (
        "Note: select(...) returns a DataFrame in recent Polars. "
        "If you test something that returns a Series, assert that no metadata is changed. "
    )

    # If you want to test a method that truly returns a Series, e.g. df.config_meta["x"],
    # you'd do:
    # s2 = df.config_meta["x"]  # calls __getattr__("__getitem__"), might or might not pass through
    # But typically item access is not handled by the plugin in the same way.

    # We'll check that the original DF's metadata is intact
    md = df.config_meta.get_metadata()
    assert md == {
        "description": "Test Series return",
    }, "Original DF metadata changed unexpectedly"


def test_parquet_roundtrip_in_memory():
    """
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
    """
    Test reading Parquet file with metadata using scan_parquet.
    """
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
