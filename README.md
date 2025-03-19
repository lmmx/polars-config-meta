# polars-config-meta

**A Polars plugin for persistent DataFrame-level metadata.**

`polars-config-meta` offers a simple way to store and propagate Python-side metadata for Polars `DataFrame`s. It achieves this by:

- Registering a custom `config_meta` namespace on each `DataFrame` (via `@register_dataframe_namespace`).
- Keeping an internal dictionary keyed by the `id(df)`, with automatic **weak-reference cleanup** to avoid memory leaks.
- Providing a “fallthrough” mechanism so you can write `df.config_meta.some_polars_method(...)` and have the resulting new `DataFrame` automatically inherit the old metadata—no manual copying required.
- Optionally embedding that metadata in **file‐level Parquet metadata** when you call `df.config_meta.write_parquet(...)`, and retrieving it with `read_parquet_with_meta(...)` (eager) or `scan_parquet_with_meta(...)` (lazy).

## Installation

```bash
pip install polars-schema-index[polars]
```

On older CPUs add the `polars-lts-cpu` extra:

```python
pip install polars-schema-index[polars-lts-cpu]
```

For parquet file-level metadata read/writing, add the `pyarrow` extra:

```python
pip install polars-schema-index[pyarrow]
```

## Key Points

1. **No Monkey-Patching or Subclassing**
   We do not modify Polars’ built-in classes at runtime or create a custom subclass of `DataFrame`. Everything is implemented through a plugin namespace.

2. **Weak-Reference Based**
   We store metadata in class-level dictionaries keyed by `id(df)` and hold a `weakref` to the DataFrame. Once the DataFrame is garbage-collected, the metadata is removed too.

3. **Automatic Metadata Copying**
   - When you call `df.config_meta.with_columns(...)` (or any other Polars method) **through** the `config_meta` namespace, we intercept the result.
   - If it’s a new `DataFrame`, the plugin copies the old one’s metadata forward.

4. **Parquet Integration**
   - `df.config_meta.write_parquet("file.parquet")` automatically embeds the plugin metadata into the Arrow schema’s `metadata`.
   - `read_parquet_with_meta("file.parquet")` reads the file, extracts that metadata, and reattaches it to the returned `DataFrame`.
   - `scan_parquet_with_meta("file.parquet")` scans the file, extracts that metadata, and reattaches it to the returned `LazyFrame`.

5. **Opt-In Only**
   - If you call `df.with_columns(...)` *without* `.config_meta.` in front, Polars has no knowledge of this plugin, so metadata will **not** copy forward.
   - If you want transformations to preserve metadata, call them via `df.config_meta.<method>(...)`.

## Basic Usage

```python
import polars as pl
import polars_config_meta  # this registers the plugin

df = pl.DataFrame({"a": [1, 2, 3]})
df.config_meta.set(owner="Alice", confidence=0.95)

# Use the plugin to transform; the returned DataFrame inherits metadata:
df2 = df.config_meta.with_columns(doubled=pl.col("a") * 2)
print(df2.config_meta.get_metadata())
# -> {'owner': 'Alice', 'confidence': 0.95}

# Write to Parquet, storing the metadata in file-level metadata:
df2.config_meta.write_parquet("output.parquet")

# Later, read it back:
from polars_config_meta import read_parquet_with_meta
df_in = read_parquet_with_meta("output.parquet")
print(df_in.config_meta.get_metadata())
# -> {'owner': 'Alice', 'confidence': 0.95}
```

### Storage and Garbage Collection

Internally, the plugin stores metadata in a global dictionary, `_df_id_to_meta`, keyed by `id(df)`,
and also keeps a `weakref` to each DataFrame. As soon as a DataFrame is out of scope and
garbage-collected, the entry in `_df_id_to_meta` is automatically removed. This prevents memory
leaks and keeps the plugin usage simple.

### Common Patterns

- **Setting Metadata**: `df.config_meta.set(key1="val1", key2="val2", ...)`
- **Retrieving Metadata**: `df.config_meta.get_metadata()` (returns a `dict`)
- **Updating Metadata From a Dict**: `df.config_meta.update({"some_key": "new_val", ...})`
- **Merging Metadata From Other DataFrames**:
  ```python
  df3 = pl.DataFrame(...)
  df3.config_meta.merge(df1, df2)
  ```
  This copies all key–value pairs from `df1` and `df2` into `df3`’s metadata.

- **Transformations**
  - `df.config_meta.with_columns(...)`
  - `df.config_meta.select(...)`
  - `df.config_meta.filter(...)`
  - etc.

For any method that returns a new `DataFrame`, the plugin copies metadata forward. If the method
returns something else (like a `Series` or plain Python object), the plugin does nothing.

### Caveats

- **Must Use `df.config_meta.<method>`**
  If you call Polars methods directly on `df`, the plugin can’t intercept the result, so metadata will not be inherited.
- **Not Official Polars Feature**
  This is purely at the Python layer. Polars doesn’t guarantee stable IDs or official hooks for such metadata.
- **Arrow/IPC/CSV**
  For other formats, you’d need to write your own logic to embed or retrieve the metadata. Currently, only Parquet is supported out of the box via `df.config_meta.write_parquet` and `read_parquet_with_meta`/`scan_parquet_with_meta`.

## Contributing

1. **Issues & Discussions**: Please open a GitHub issue for bugs, ideas, or questions.
2. **Pull Requests**: PRs are welcome! This plugin is a community-driven approach to persist DataFrame-level metadata in Polars.

## Polars development

There is ongoing work to support file-level metadata in the Parquet writing, see [this PR](https://github.com/pola-rs/polars/pull/21806) for details.

## License

This project is licensed under the MIT License.
