"""Polars plugin for persistent DataFrame-level metadata.

This module provides a configuration metadata management system for Polars DataFrames,
allowing users to attach, preserve, and transfer metadata across various DataFrame
operations.
"""

import json
import weakref
from typing import Literal, overload

import polars as pl
from polars.api import (
    register_dataframe_namespace,
    register_lazyframe_namespace,
    register_series_namespace,
)

from .diagnostics import (
    check_method_discovered,
    compare_discovered_methods,
    print_discovered_methods,
    verify_patching,
)
from .discovery import discover_patchable_methods, patch_method, unpatch_all_methods

__all__ = [
    "ConfigMetaOpts",
    "ConfigMetaPlugin",
    "read_parquet_with_meta",
    "scan_parquet_with_meta",
    # Diagnostics
    "check_method_discovered",
    "compare_discovered_methods",
    "print_discovered_methods",
    "verify_patching",
]


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


def _copy_metadata_to_result(source: pl.DataFrame | pl.LazyFrame | pl.Series, result):
    """Copy metadata from source to result.

    Ensures that metadata is transferred when a new DataFrame, LazyFrame, or Series
    is created from an existing one.
    """
    if isinstance(result, (pl.DataFrame, pl.LazyFrame, pl.Series)):
        source_id = id(source)
        if source_id in ConfigMetaPlugin._df_id_to_meta:
            # Register the result and copy metadata
            ConfigMetaPlugin(result)
            ConfigMetaPlugin._df_id_to_meta[id(result)].update(
                ConfigMetaPlugin._df_id_to_meta[source_id],
            )
    return result


_IS_PATCHED = False


def _ensure_patched():
    """Ensure all DataFrame, LazyFrame, and Series methods are patched."""
    global _IS_PATCHED
    if not ConfigMetaOpts.auto_preserve_metadata:
        return

    if _IS_PATCHED:
        return

    # Discover and patch all methods that return DataFrame/LazyFrame/Series
    dataframe_methods = discover_patchable_methods(pl.DataFrame)
    lazyframe_methods = discover_patchable_methods(pl.LazyFrame)
    series_methods = discover_patchable_methods(pl.Series)

    for method_name in dataframe_methods:
        patch_method(pl.DataFrame, method_name, _copy_metadata_to_result)

    for method_name in lazyframe_methods:
        patch_method(pl.LazyFrame, method_name, _copy_metadata_to_result)

    for method_name in series_methods:
        patch_method(pl.Series, method_name, _copy_metadata_to_result)

    _IS_PATCHED = True


def _unpatch_all():
    """Restore all original methods."""
    global _IS_PATCHED
    if not _IS_PATCHED:
        return

    unpatch_all_methods()
    _IS_PATCHED = False


def _repatch_all():
    """Re-patch all methods (used when re-enabling after disable)."""
    global _IS_PATCHED
    _IS_PATCHED = False  # Reset flag to allow patching
    _ensure_patched()


@register_dataframe_namespace("config_meta")
@register_lazyframe_namespace("config_meta")
@register_series_namespace("config_meta")
class ConfigMetaPlugin:
    """A plugin for managing DataFrame/LazyFrame/Series metadata.

    This plugin provides functionality to:
    - Attach in-memory metadata to Polars DataFrames, LazyFrames, and Series
    - Intercept method calls through .config_meta
    - Preserve metadata across transformations
    - Handle special cases like Parquet file writing
    """

    # Global dictionaries to store metadata:
    _df_id_to_meta = {}
    _df_id_to_ref = {}

    def __init__(self, obj: pl.DataFrame | pl.LazyFrame | pl.Series):
        """Initialize the ConfigMetaPlugin for a specific DataFrame, LazyFrame, or Series.

        Args:
            obj: The Polars DataFrame, LazyFrame, or Series to attach metadata to.

        """
        self._df = obj
        self._df_id = id(obj)
        # If new to us, register a weakref so we can remove it on GC
        if self._df_id not in self._df_id_to_meta:
            self._df_id_to_meta[self._df_id] = {}
            self._df_id_to_ref[self._df_id] = weakref.ref(obj, self._cleanup)

        # Ensure methods are patched when plugin is first used (if enabled)
        if ConfigMetaOpts.auto_preserve_metadata:
            _ensure_patched()

    @classmethod
    def _cleanup(cls, obj_weakref):
        """When the object is GC'd, remove references in the global dicts."""
        to_remove = None
        for obj_id, wref in cls._df_id_to_ref.items():
            if wref is obj_weakref:
                to_remove = obj_id
                break
        if to_remove is not None:
            cls._df_id_to_ref.pop(to_remove, None)
            cls._df_id_to_meta.pop(to_remove, None)

    def set(self, **kwargs) -> None:
        """Set metadata for the object.

        Args:
            **kwargs: Key-value pairs to store as metadata.

        """
        self._df_id_to_meta[self._df_id].update(kwargs)

    def update(self, mapping: dict) -> None:
        """Update existing metadata with new key-value pairs.

        Args:
            mapping: A dictionary of metadata to update.

        """
        self._df_id_to_meta[self._df_id].update(mapping)

    def merge(self, *objs: pl.DataFrame | pl.LazyFrame | pl.Series) -> None:
        """Merge metadata from other DataFrames, LazyFrames, or Series by dict.update."""
        for other_obj in objs:
            ConfigMetaPlugin(other_obj)  # ensure it's registered
            other_id = id(other_obj)
            self._df_id_to_meta[self._df_id].update(
                self._df_id_to_meta.get(other_id, {}),
            )

    def get_metadata(self) -> dict:
        """Retrieve the current metadata for the object.

        Returns:
            A dictionary containing the object's metadata.

        """
        return self._df_id_to_meta[self._df_id]

    def clear_metadata(self) -> None:
        """Remove all metadata for this object."""
        self._df_id_to_meta[self._df_id] = {}

    def __getattr__(self, name: str):
        """Provide fallback for method calls not defined in the plugin.

        This method allows intercepting and forwarding method calls to the underlying
        object, with special handling for certain methods like write_parquet.

        Args:
            name: The name of the method being called.

        Returns:
            The result of the method call on the underlying object.

        """
        # Special case for "write_parquet": we want to intercept that.
        if name == "write_parquet":
            return self._write_parquet_plugin

        # Otherwise, see if the underlying object has this attribute.
        obj_attr = getattr(self._df, name, None)
        if obj_attr is None:
            raise AttributeError(
                f"Polars {type(self._df).__name__} has no attribute '{name}'"
            )

        if not callable(obj_attr):
            # e.g. df.config_meta.shape -> just return df.shape
            return obj_attr

        # If it's a method, wrap it so we can intercept the return value.
        def wrapper(*args, **kwargs):
            result = obj_attr(*args, **kwargs)
            # If the result is a new DataFrame/LazyFrame/Series, copy the metadata
            if isinstance(result, (pl.DataFrame, pl.LazyFrame, pl.Series)):
                ConfigMetaPlugin(result)  # ensure plugin registration
                self._df_id_to_meta[id(result)].update(self._df_id_to_meta[self._df_id])
            return result

        return wrapper

    def _write_parquet_plugin(self, file_path: str, **kwargs):
        """Implement custom Parquet writing with metadata preservation.

        This method handles the Parquet writing process with the following steps:
        1) extracts plugin metadata
        2) converts to Arrow
        3) attaches the metadata to the Arrow schema
        4) writes to Parquet with PyArrow

        Note: For Series, converts to single-column DataFrame first.
        """
        import pyarrow.parquet as pq

        # 1) get plugin metadata
        metadata_dict = self._df_id_to_meta[self._df_id]
        # convert to a JSON string for storage
        metadata_json = json.dumps(metadata_dict).encode("utf-8")

        # 2) convert to Arrow (handle Series by converting to DataFrame first)
        if isinstance(self._df, pl.Series):
            arrow_table = self._df.to_frame().to_arrow()
        elif isinstance(self._df, pl.LazyFrame):
            arrow_table = self._df.collect().to_arrow()
        else:
            arrow_table = self._df.to_arrow()

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
    """Load a Parquet file with associated metadata.

    This method extracts the 'polars_plugin_meta' metadata stored in the Parquet file.
    It then loads the data using either the Polars `.read_parquet` or `.scan_parquet` methods,
    and attaches the associated plugin metadata to the resulting DataFrame.

    Args:
        file_path: Path to the Parquet file to load.
        lazy: Whether to return a LazyFrame from the loaded Parquet (if not, a DataFrame).
        **kwargs: Additional arguments to pass to the Polars reading method.

    Returns:
        A Polars DataFrame or LazyFrame with restored metadata.

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
    """Read a Parquet file with its associated metadata.

    Loads the Parquet file and retrieves any stored plugin metadata.

    Args:
        file_path: Path to the Parquet file to read.
        **kwargs: Additional arguments to pass to the reading method.

    Returns:
        A Polars DataFrame with restored metadata.

    """
    return _load_parquet_with_meta(file_path, lazy=False, **kwargs)


def scan_parquet_with_meta(file_path: str, **kwargs) -> pl.LazyFrame:
    """Scan a Parquet file with its associated metadata.

    Scans the Parquet file and retrieves any stored plugin metadata.

    Args:
        file_path: Path to the Parquet file to scan.
        **kwargs: Additional arguments to pass to the scanning method.

    Returns:
        A Polars LazyFrame with restored metadata.

    """
    return _load_parquet_with_meta(file_path, lazy=True, **kwargs)
