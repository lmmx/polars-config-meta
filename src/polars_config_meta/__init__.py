import json
import weakref
from typing import Literal, overload

import polars as pl
from polars.api import register_dataframe_namespace, register_lazyframe_namespace


# Configuration for automatic metadata preservation
class ConfigMetaOpts:
    """Global configuration for the config_meta plugin."""

    auto_preserve_metadata = True

    @classmethod
    def enable_auto_preserve(cls):
        """Enable automatic metadata preservation for regular DataFrame methods."""
        cls.auto_preserve_metadata = True
        _repatch_all()

    @classmethod
    def disable_auto_preserve(cls):
        """Disable automatic metadata preservation for regular DataFrame methods."""
        cls.auto_preserve_metadata = False
        _unpatch_all()


# Store original methods before patching
_ORIGINAL_METHODS = {}
_IS_PATCHED = False

# Methods that return DataFrames and should preserve metadata
METHODS_TO_PATCH = [
    "with_columns",
    "select",
    "filter",
    "sort",
    "unique",
    "drop",
    "rename",
    "cast",
    "with_columns_seq",
    "drop_nulls",
    "fill_null",
    "fill_nan",
    "head",
    "tail",
    "sample",
    "slice",
    "limit",
    "reverse",
    "rechunk",
    "clone",
    "clear",
]


def _copy_metadata_to_result(source_df: pl.DataFrame | pl.LazyFrame, result):
    """Helper to copy metadata from source to result DataFrame."""
    if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
        source_id = id(source_df)
        if source_id in ConfigMetaPlugin._df_id_to_meta:
            # Register the result and copy metadata
            ConfigMetaPlugin(result)
            ConfigMetaPlugin._df_id_to_meta[id(result)].update(
                ConfigMetaPlugin._df_id_to_meta[source_id],
            )
    return result


def _patch_dataframe_method(cls, method_name: str):
    """Patch a DataFrame/LazyFrame method to preserve metadata."""
    if not hasattr(cls, method_name):
        return

    key = (cls, method_name)

    # Store original only if we haven't stored it before
    if key not in _ORIGINAL_METHODS:
        original_method = getattr(cls, method_name)
        _ORIGINAL_METHODS[key] = original_method
    else:
        # Use the stored original (in case we're re-patching after unpatch)
        original_method = _ORIGINAL_METHODS[key]

    def wrapped_method(self, *args, **kwargs):
        result = original_method(self, *args, **kwargs)
        return _copy_metadata_to_result(self, result)

    setattr(cls, method_name, wrapped_method)


def _ensure_patched():
    """Ensure all DataFrame and LazyFrame methods are patched."""
    global _IS_PATCHED
    if not ConfigMetaOpts.auto_preserve_metadata:
        return

    if _IS_PATCHED:
        return

    for cls in [pl.DataFrame, pl.LazyFrame]:
        for method_name in METHODS_TO_PATCH:
            _patch_dataframe_method(cls, method_name)

    _IS_PATCHED = True


def _unpatch_all():
    """Restore all original methods."""
    global _IS_PATCHED
    if not _IS_PATCHED:
        return

    for (cls, method_name), original_method in _ORIGINAL_METHODS.items():
        setattr(cls, method_name, original_method)

    _IS_PATCHED = False


def _repatch_all():
    """Re-patch all methods (used when re-enabling after disable)."""
    global _IS_PATCHED
    _IS_PATCHED = False  # Reset flag to allow patching
    _ensure_patched()


@register_dataframe_namespace("config_meta")
@register_lazyframe_namespace("config_meta")
class ConfigMetaPlugin:
    """
    A plugin that:
      - attaches in-memory metadata to Polars DataFrames
      - intercepts any df.config_meta.some_method(...) calls:
          * if 'some_method' is not defined here, we forward it to df.some_method
          * if that call returns a new DataFrame, we copy the old one's metadata
      - special case for write_parquet -> store plugin metadata in the Parquet file
      - ALSO patches regular DataFrame methods to preserve metadata automatically
    """

    # Global dictionaries to store metadata:
    _df_id_to_meta = {}
    _df_id_to_ref = {}

    def __init__(self, df: pl.DataFrame | pl.LazyFrame):
        self._df = df
        self._df_id = id(df)
        # If new to us, register a weakref so we can remove it on GC
        if self._df_id not in self._df_id_to_meta:
            self._df_id_to_meta[self._df_id] = {}
            self._df_id_to_ref[self._df_id] = weakref.ref(df, self._cleanup)

        # Ensure methods are patched when plugin is first used (if enabled)
        if ConfigMetaOpts.auto_preserve_metadata:
            _ensure_patched()

    @classmethod
    def _cleanup(cls, df_weakref):
        """When the DF is GC'd, remove references in the global dicts."""
        to_remove = None
        for df_id, wref in cls._df_id_to_ref.items():
            if wref is df_weakref:
                to_remove = df_id
                break
        if to_remove is not None:
            cls._df_id_to_ref.pop(to_remove, None)
            cls._df_id_to_meta.pop(to_remove, None)

    def set(self, **kwargs) -> None:
        self._df_id_to_meta[self._df_id].update(kwargs)

    def update(self, mapping: dict) -> None:
        self._df_id_to_meta[self._df_id].update(mapping)

    def merge(self, *dfs: pl.DataFrame | pl.LazyFrame) -> None:
        """
        Merge metadata from other dataframes by dict.update.
        """
        for other_df in dfs:
            ConfigMetaPlugin(other_df)  # ensure it's registered
            other_id = id(other_df)
            self._df_id_to_meta[self._df_id].update(
                self._df_id_to_meta.get(other_id, {}),
            )

    def get_metadata(self) -> dict:
        return self._df_id_to_meta[self._df_id]

    def __getattr__(self, name: str):
        """
        Fallback for calls like: df.config_meta.write_parquet(...)
        or df.config_meta.with_columns(...).
        If 'name' is not a method/attribute on this plugin, try to get it from self._df.
        """
        # Special case for "write_parquet": we want to intercept that.
        if name == "write_parquet":
            return self._write_parquet_plugin

        # Otherwise, see if the underlying DataFrame has this attribute.
        df_attr = getattr(self._df, name, None)
        if df_attr is None:
            raise AttributeError(f"Polars DataFrame has no attribute '{name}'")

        if not callable(df_attr):
            # e.g. df.config_meta.shape -> just return df.shape
            return df_attr

        # If it's a method, wrap it so we can intercept the return value.
        def wrapper(*args, **kwargs):
            result = df_attr(*args, **kwargs)
            # If the result is a new DataFrame, copy the metadata
            if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
                ConfigMetaPlugin(result)  # ensure plugin registration
                self._df_id_to_meta[id(result)].update(self._df_id_to_meta[self._df_id])
            return result

        return wrapper

    def _write_parquet_plugin(self, file_path: str, **kwargs):
        """
        Our custom writer that:
          1) extracts plugin metadata
          2) converts DF to Arrow
          3) attaches the metadata to the Arrow schema
          4) writes to Parquet with PyArrow
        """
        import pyarrow.parquet as pq

        # 1) get plugin metadata
        metadata_dict = self._df_id_to_meta[self._df_id]
        # convert to a JSON string for storage
        metadata_json = json.dumps(metadata_dict).encode("utf-8")

        # 2) convert DF to Arrow
        arrow_table = self._df.lazy().collect().to_arrow()

        # 3) attach custom metadata
        #    existing schema metadata + our custom "polars_plugin_meta"
        existing_meta = arrow_table.schema.metadata or {}
        new_meta = dict(existing_meta)  # copy
        new_meta[b"polars_plugin_meta"] = metadata_json
        arrow_table = arrow_table.replace_schema_metadata(new_meta)

        # 4) write to Parquet with PyArrow
        pq.write_table(arrow_table, file_path, **kwargs)


@overload
def _load_parquet_with_meta(
    file_path: str,
    lazy: Literal[False] = False,
    **kwargs,
) -> pl.DataFrame: ...


@overload
def _load_parquet_with_meta(
    file_path: str,
    lazy: Literal[True],
    **kwargs,
) -> pl.LazyFrame: ...


def _load_parquet_with_meta(
    file_path: str,
    lazy: bool = False,
    **kwargs,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Loads only the metadata from a parquet file with PyArrow
    and extracts the 'polars_plugin_meta' we stored.
    Then loads the data using either the polars
    `.read_parquet' or `.scan_parquet` methods,
    and attaches the associated plugin metadata.
    """
    import pyarrow.parquet as pq

    # 1) read metadata with PyArrow
    pyarrow_metadata = pq.read_schema(file_path).metadata
    meta = pyarrow_metadata or {}
    custom_json = meta.get(b"polars_plugin_meta", None)

    # 2) read Parquet with Polars
    if lazy:
        df = pl.scan_parquet(file_path, **kwargs)
    else:
        df = pl.read_parquet(file_path, **kwargs)

    # 3) if custom metadata found, parse it + store in plugin
    if custom_json is not None:
        data_dict = json.loads(custom_json.decode("utf-8"))
        ConfigMetaPlugin(df)  # ensure plugin registration
        df.config_meta.update(data_dict)

    return df


def read_parquet_with_meta(file_path: str, **kwargs) -> pl.DataFrame:
    """
    Reads a parquet file along with the metadata.
    """
    return _load_parquet_with_meta(file_path, lazy=False, **kwargs)


def scan_parquet_with_meta(file_path: str, **kwargs) -> pl.LazyFrame:
    """
    Scans a parquet file along with the metadata.
    """
    return _load_parquet_with_meta(file_path, lazy=True, **kwargs)
