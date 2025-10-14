# polars-config-meta

**A Polars plugin for persistent DataFrame-level metadata.**

`polars-config-meta` offers a simple way to store and propagate Python-side metadata for Polars `DataFrame`s and `LazyFrame`s. It achieves this by:

- Registering a custom `config_meta` namespace on each `DataFrame` and `LazyFrame`.
- Keeping an internal dictionary keyed by the `id(df)`, with automatic **weak-reference cleanup** to avoid memory leaks.
- **Automatically patching common Polars methods** (like `with_columns`, `select`, `filter`, etc.) so that metadata is preserved even when using regular Polars syntax.
- Providing a "fallthrough" mechanism so you can write `df.config_meta.some_polars_method(...)` and have the resulting new `DataFrame` automatically inherit the old metadata,
  for use to either explicitly note the metadata transfer or as a backup if a method was not monkeypatched (please file a bug report if you find any!).
- Optionally embedding that metadata in **file‐level Parquet metadata** when you call `df.config_meta.write_parquet(...)`, and retrieving it with `read_parquet_with_meta(...)` (eager) or `scan_parquet_with_meta(...)` (lazy).

## Installation

```bash
pip install polars-config-meta[polars]
```

On older CPUs add the `polars-lts-cpu` extra:

```bash
pip install polars-config-meta[polars-lts-cpu]
```

For parquet file-level metadata read/writing, add the `pyarrow` extra:

```bash
pip install polars-config-meta[pyarrow]
```

## Key Points

1. **Automatic Metadata Preservation (New in v0.2.0!)**
   By default, the plugin patches common Polars DataFrame methods (`with_columns`, `select`, `filter`, `sort`, etc.) to automatically preserve metadata. This means both of these will preserve metadata:
   - `df.with_columns(...)` ← regular Polars method (automatically patched)
   - `df.config_meta.with_columns(...)` ← through the namespace

   This behavior can be configured globally (see [Configuration](#configuration) below).

2. **Weak-Reference Based**
   We store metadata in class-level dictionaries keyed by `id(df)` and hold a `weakref` to the DataFrame. Once the DataFrame is garbage-collected, the metadata is removed too.

3. **Works with DataFrames and LazyFrames**
   The plugin supports both eager (`DataFrame`) and lazy (`LazyFrame`) execution modes.

4. **Parquet Integration**
   - `df.config_meta.write_parquet("file.parquet")` automatically embeds the plugin metadata into the Arrow schema's `metadata`.
   - `read_parquet_with_meta("file.parquet")` reads the file, extracts that metadata, and reattaches it to the returned `DataFrame`.
   - `scan_parquet_with_meta("file.parquet")` scans the file, extracts that metadata, and reattaches it to the returned `LazyFrame`.

5. **Chainable Operations**
   Since metadata is preserved across transformations, you can chain multiple operations:
   ```python
   result = (
       df.config_meta.set(owner="Alice")
       .with_columns(doubled=pl.col("a") * 2)
       .filter(pl.col("doubled") > 5)
       .select(["doubled"])
   )
   # Metadata is preserved throughout the chain!
   ```

## Basic Usage

```python
import polars as pl
import polars_config_meta  # this registers the plugin

df = pl.DataFrame({"a": [1, 2, 3]})
df.config_meta.set(owner="Alice", confidence=0.95)

# Both of these preserve metadata (auto-patching is enabled by default):
df2 = df.with_columns(doubled=pl.col("a") * 2)
print(df2.config_meta.get_metadata())
# -> {'owner': 'Alice', 'confidence': 0.95}

df3 = df.config_meta.with_columns(tripled=pl.col("a") * 3)
print(df3.config_meta.get_metadata())
# -> {'owner': 'Alice', 'confidence': 0.95}

# Chain operations - metadata flows through:
df4 = (
    df.with_columns(squared=pl.col("a") ** 2)
      .filter(pl.col("squared") > 4)
      .select(["a", "squared"])
)
print(df4.config_meta.get_metadata())
# -> {'owner': 'Alice', 'confidence': 0.95}

# Write to Parquet, storing the metadata in file-level metadata:
df4.config_meta.write_parquet("output.parquet")

# Later, read it back:
from polars_config_meta import read_parquet_with_meta
df_in = read_parquet_with_meta("output.parquet")
print(df_in.config_meta.get_metadata())
# -> {'owner': 'Alice', 'confidence': 0.95}
```

## Configuration

The plugin provides a `ConfigMetaOpts` class to control automatic metadata preservation behavior:

```python
from polars_config_meta import ConfigMetaOpts

# Disable automatic metadata preservation for regular DataFrame methods
ConfigMetaOpts.disable_auto_preserve()

df = pl.DataFrame({"a": [1, 2, 3]})
df.config_meta.set(owner="Alice")

df2 = df.with_columns(doubled=pl.col("a") * 2)
print(df2.config_meta.get_metadata())
# -> {} (metadata NOT preserved with regular methods)

df3 = df.config_meta.with_columns(tripled=pl.col("a") * 3)
print(df3.config_meta.get_metadata())
# -> {'owner': 'Alice'} (still works via namespace!)

# Re-enable automatic preservation
ConfigMetaOpts.enable_auto_preserve()

df4 = df.with_columns(quadrupled=pl.col("a") * 4)
print(df4.config_meta.get_metadata())
# -> {'owner': 'Alice'} (metadata preserved again)
```

### Configuration Options

- **`ConfigMetaOpts.enable_auto_preserve()`**: Enable automatic metadata preservation for regular DataFrame methods (this is the default behavior).
- **`ConfigMetaOpts.disable_auto_preserve()`**: Disable automatic preservation. Only `df.config_meta.<method>()` will preserve metadata.

**Note**: The `df.config_meta.<method>()` syntax **always** preserves metadata, regardless of the configuration setting.

## API Reference

### Setting and Retrieving Metadata

- **`df.config_meta.set(**kwargs)`**: Set metadata key-value pairs
  ```python
  df.config_meta.set(owner="Alice", confidence=0.95, version=2)
  ```

- **`df.config_meta.get_metadata()`**: Get all metadata as a dictionary
  ```python
  metadata = df.config_meta.get_metadata()
  # -> {'owner': 'Alice', 'confidence': 0.95, 'version': 2}
  ```

- **`df.config_meta.update(mapping)`**: Update metadata from a dictionary
  ```python
  df.config_meta.update({"confidence": 0.99, "validated": True})
  ```

- **`df.config_meta.merge(*dfs)`**: Merge metadata from other DataFrames
  ```python
  df3.config_meta.merge(df1, df2)
  # df3 now has metadata from both df1 and df2
  ```

- **`df.config_meta.clear_metadata()`**: Remove all metadata for this DataFrame
  ```python
  df.config_meta.clear_metadata()
  ```

### Parquet I/O

- **`df.config_meta.write_parquet(file_path, **kwargs)`**: Write DataFrame to Parquet with embedded metadata
  ```python
  df.config_meta.write_parquet("output.parquet")
  ```

- **`read_parquet_with_meta(file_path, **kwargs)`**: Read Parquet file with metadata (eager)
  ```python
  from polars_config_meta import read_parquet_with_meta
  df = read_parquet_with_meta("output.parquet")
  ```

- **`scan_parquet_with_meta(file_path, **kwargs)`**: Scan Parquet file with metadata (lazy)
  ```python
  from polars_config_meta import scan_parquet_with_meta
  lf = scan_parquet_with_meta("output.parquet")
  ```

### Automatic Method Forwarding

Any Polars DataFrame/LazyFrame method can be called through `df.config_meta.<method>()`:

```python
# All of these preserve metadata:
df.config_meta.with_columns(new_col=pl.col("a") * 2)
df.config_meta.select(["a", "b"])
df.config_meta.filter(pl.col("a") > 0)
df.config_meta.sort("a")
df.config_meta.unique()
df.config_meta.drop(["col1"])
df.config_meta.rename({"old": "new"})
# ... and many more!
```

## Common Patterns

### Setting Metadata on Creation

```python
df = pl.DataFrame({"a": [1, 2, 3]})
df.config_meta.set(
    source="user_upload",
    timestamp="2025-01-15",
    validated=False
)
```

### Chaining Operations

```python
result = (
    df.with_columns(normalized=pl.col("value") / pl.col("value").sum())
      .filter(pl.col("normalized") > 0.1)
      .sort("normalized", descending=True)
)
# Metadata flows through the entire chain
```

### Merging Metadata from Multiple Sources

```python
df1.config_meta.set(source="api", quality="high")
df2.config_meta.set(source="cache", timestamp="2025-01-15")

df3 = pl.concat([df1, df2])
df3.config_meta.merge(df1, df2)
# df3 now has: {'source': 'cache', 'quality': 'high', 'timestamp': '2025-01-15'}
# Note: Later DataFrames' values override earlier ones
```

### Persistent Storage with Parquet

```python
# Save with metadata
df.config_meta.set(lineage="raw_data", version=1)
df.config_meta.write_parquet("data_v1.parquet")

# Load with metadata
df_loaded = read_parquet_with_meta("data_v1.parquet")
print(df_loaded.config_meta.get_metadata())
# -> {'lineage': 'raw_data', 'version': 1}
```

## How It Works

### Automatic Patching

When you first access `.config_meta` on any DataFrame, the plugin automatically patches common Polars methods like:
- `with_columns`, `select`, `filter`, `sort`, `unique`, `drop`, `rename`, `cast`
- `drop_nulls`, `fill_null`, `fill_nan`
- `head`, `tail`, `sample`, `slice`, `limit`
- `reverse`, `rechunk`, `clone`, `clear`
- ... and more

These patched methods automatically copy metadata from the source DataFrame to the result DataFrame.

### Storage and Garbage Collection

Internally, the plugin stores metadata in a global dictionary, `_df_id_to_meta`, keyed by `id(df)`,
and also keeps a `weakref` to each DataFrame. As soon as a DataFrame is out of scope and
garbage-collected, the entry in `_df_id_to_meta` is automatically removed. This prevents memory
leaks and keeps the plugin usage simple.

### Method Interception

When you call `df.config_meta.some_method(...)`:
1. The plugin checks if `some_method` exists on the plugin itself (like `set`, `get_metadata`, `write_parquet`)
2. If not, it forwards the call to the underlying DataFrame's method
3. If the result is a new DataFrame/LazyFrame, it automatically copies the metadata

## Caveats

- **Python-Layer Only**
  This is purely at the Python layer. Polars doesn't guarantee stable IDs or official hooks for such metadata.

- **Metadata is Ephemeral (Unless Saved)**
  Metadata is stored in memory and tied to DataFrame object IDs. It won't survive serialization unless you explicitly use `df.config_meta.write_parquet()` and `read_parquet_with_meta()`.

- **Other Formats Not Supported**
  Currently, only Parquet format supports automatic metadata embedding/extraction. For CSV, Arrow, IPC, etc., you'd need to implement your own serialization logic.

- **Configuration is Global**
  The `ConfigMetaOpts` settings apply globally to all DataFrames in your Python session.

## Contributing

1. **Issues & Discussions**: Please open a GitHub issue for bugs, ideas, or questions.
2. **Pull Requests**: PRs are welcome! This plugin is a community-driven approach to persist DataFrame-level metadata in Polars.

## Polars Development

There is ongoing work to support file-level metadata in the Polars Parquet writing, see [this PR](https://github.com/pola-rs/polars/pull/21806) for details. Once that lands, this plugin may be able to integrate more seamlessly.

## License

This project is licensed under the MIT License.
