"""Test suite for Polars DataFrame metadata preservation strategies.

This module contains tests that verify the ability to maintain and transfer
metadata when in the "auto-preserve" mode, as well as to disable it.
"""

import polars as pl

import polars_config_meta  # noqa: F401
from polars_config_meta import ConfigMetaOpts


def test_preserving_transform_copies_metadata():
    """Test that using df.config_meta.some_method copies metadata to the new DataFrame."""
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

    # Using plain Polars method (without config_meta) should copy metadata
    df3 = df.with_columns(pl.col("val") * 3)
    md3 = df3.config_meta.get_metadata()
    assert md3 != {}, "Plain df.with_columns **should** now copy metadata"


def test_disable_auto_preserve():
    """Test that disabling auto-preserve stops regular DataFrame methods from copying metadata."""
    # Disable auto-preservation
    ConfigMetaOpts.disable_auto_preserve()

    try:
        df = pl.DataFrame({"val": [10, 20]})
        df.config_meta.set(source="generated", confidence=0.9)

        # Using plain Polars method should NOT copy metadata when disabled
        df2 = df.with_columns(doubled=pl.col("val") * 2)
        md2 = df2.config_meta.get_metadata()
        assert md2 == {}, "Plain df.with_columns should NOT copy metadata when disabled"

        # But using config_meta namespace should STILL copy metadata
        df3 = df.config_meta.with_columns(tripled=pl.col("val") * 3)
        md3 = df3.config_meta.get_metadata()
        assert md3 == {
            "source": "generated",
            "confidence": 0.9,
        }, "df.config_meta.with_columns should still copy metadata"

    finally:
        # Re-enable for other tests
        ConfigMetaOpts.enable_auto_preserve()


def test_re_enable_auto_preserve():
    """Test that re-enabling auto-preserve restores metadata copying behavior."""
    # First disable
    ConfigMetaOpts.disable_auto_preserve()

    df = pl.DataFrame({"val": [10, 20]})
    df.config_meta.set(source="test")

    df2 = df.with_columns(doubled=pl.col("val") * 2)
    assert df2.config_meta.get_metadata() == {}, "Should not copy when disabled"

    # Now re-enable
    ConfigMetaOpts.enable_auto_preserve()

    # Create a new DataFrame to trigger patching
    df3 = pl.DataFrame({"val": [30, 40]})
    df3.config_meta.set(source="test2")

    df4 = df3.with_columns(doubled=pl.col("val") * 2)
    assert df4.config_meta.get_metadata() == {
        "source": "test2",
    }, "Should copy metadata after re-enabling"


def test_dataframe_to_series_propagation():
    """Test that metadata propagates from DataFrame to Series via get_column."""
    df = pl.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
    df.config_meta.set(owner="Alice", version=1)

    s = df.get_column("foo")

    md = s.config_meta.get_metadata()
    assert md == {
        "owner": "Alice",
        "version": 1,
    }, "Metadata not propagated from DataFrame to Series via get_column"


def test_series_to_dataframe_propagation():
    """Test that metadata propagates from Series to DataFrame via to_frame."""
    s = pl.Series("foo", [1, 2, 3])
    s.config_meta.set(owner="Bob", stage="prod")

    df = s.to_frame()

    md = df.config_meta.get_metadata()
    assert md == {
        "owner": "Bob",
        "stage": "prod",
    }, "Metadata not propagated from Series to DataFrame via to_frame"


def test_series_to_series_method_chaining():
    """Test that Series methods preserve metadata through chains."""
    s = pl.Series("vals", [3, 1, 2])
    s.config_meta.set(source="original", confidence=0.9)

    # Chain several Series operations
    s2 = s.sort().head(2)

    md = s2.config_meta.get_metadata()
    assert md == {
        "source": "original",
        "confidence": 0.9,
    }, "Metadata not preserved through Series method chain"


def test_full_type_chain_lazyframe_to_series():
    """Test metadata flows through LazyFrame → DataFrame → Series."""
    lf = pl.LazyFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    lf.config_meta.set(pipeline="etl", version=2)

    # LazyFrame → DataFrame → Series
    s = lf.collect().get_column("x")

    md = s.config_meta.get_metadata()
    assert md == {
        "pipeline": "etl",
        "version": 2,
    }, "Metadata not preserved through LazyFrame → DataFrame → Series chain"


def test_series_round_trip_through_dataframe():
    """Test metadata survives Series → DataFrame → Series conversion."""
    s1 = pl.Series("val", [10, 20, 30])
    s1.config_meta.set(origin="series", processed=True)

    # Series → DataFrame → Series
    s2 = s1.to_frame().get_column("val")

    md = s2.config_meta.get_metadata()
    assert md == {
        "origin": "series",
        "processed": True,
    }, "Metadata not preserved through Series → DataFrame → Series round-trip"
