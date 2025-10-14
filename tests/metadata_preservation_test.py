import polars as pl
import polars_config_meta  # noqa: F401
from polars_config_meta import ConfigMetaOpts


def test_preserving_transform_copies_metadata():
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

    # Using plain Polars method (without config_meta) should copy metadata
    df3 = df.with_columns(pl.col("val") * 3)
    md3 = df3.config_meta.get_metadata()
    assert md3 != {}, "Plain df.with_columns **should** now copy metadata"


def test_disable_auto_preserve():
    """
    Test that disabling auto-preserve stops regular DataFrame methods from copying metadata.
    """
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
    """
    Test that re-enabling auto-preserve restores metadata copying behavior.
    """
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
