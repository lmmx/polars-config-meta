import json
import weakref

import polars as pl
from polars.api import register_dataframe_namespace


@register_dataframe_namespace("config_meta")
class ConfigMetaPlugin:
    """
    A plugin that:
      - attaches in-memory metadata to Polars DataFrames
      - intercepts any df.config_meta.some_method(...) calls:
          * if 'some_method' is not defined here, we forward it to df.some_method
          * if that call returns a new DataFrame, we copy the old one's metadata
      - special case for write_parquet -> store plugin metadata in the Parquet file
    """

    # Global dictionaries to store metadata:
    _df_id_to_meta = {}
    _df_id_to_ref = {}

    def __init__(self, df: pl.DataFrame):
        self._df = df
        self._df_id = id(df)
        # If new to us, register a weakref so we can remove it on GC
        if self._df_id not in self._df_id_to_meta:
            self._df_id_to_meta[self._df_id] = {}
            self._df_id_to_ref[self._df_id] = weakref.ref(df, self._cleanup)

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

    def merge(self, *dfs: pl.DataFrame) -> None:
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
            if isinstance(result, pl.DataFrame):
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
        arrow_table = self._df.to_arrow()

        # 3) attach custom metadata
        #    existing schema metadata + our custom "polars_plugin_meta"
        existing_meta = arrow_table.schema.metadata or {}
        new_meta = dict(existing_meta)  # copy
        new_meta[b"polars_plugin_meta"] = metadata_json
        arrow_table = arrow_table.replace_schema_metadata(new_meta)

        # 4) write to Parquet with PyArrow
        pq.write_table(arrow_table, file_path, **kwargs)


def read_parquet_with_meta(file_path: str, **kwargs) -> pl.DataFrame:
    """
    Read a Parquet file with PyArrow, extract the 'polars_plugin_meta' we stored,
    load into a Polars DataFrame, and attach that metadata in our plugin.
    """
    import pyarrow.parquet as pq

    # 1) read with PyArrow
    arrow_table = pq.read_table(file_path, **kwargs)

    # 2) check for metadata
    meta = arrow_table.schema.metadata or {}
    custom_json = meta.get(b"polars_plugin_meta", None)

    # 3) convert to Polars
    df = pl.from_arrow(arrow_table)

    # 4) if custom metadata found, parse it + store in plugin
    if custom_json is not None:
        data_dict = json.loads(custom_json.decode("utf-8"))
        ConfigMetaPlugin(df)  # ensure plugin registration
        df.config_meta.update(data_dict)

    return df
