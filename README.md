# polars-config-meta

**A Polars plugin for persistent DataFrame-level metadata.**

`polars-config-meta` offers a simple way to store and propagate Python-side metadata for Polars `DataFrame`s, `LazyFrame`s, and `Series`. It achieves this by:

- Registering a custom `config_meta` namespace on each `DataFrame`, `LazyFrame`, and `Series` object.
- Keeping an internal dictionary keyed by the `id(obj)`, with automatic **weak-reference cleanup** to avoid memory leaks.
- **Automatically patching common Polars methods** (like `with_columns`, `select`, `filter`, `get_column`, `to_frame`, etc.) so that metadata is preserved even when using regular Polars syntax.
- Providing a "fallthrough" mechanism so you can write `df.config_meta.some_polars_method(...)` and have the resulting new frame/series object automatically inherit the old metadata,
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

1. **Automatic Metadata Preservation**
   By default, the plugin patches common Polars methods (`with_columns`, `select`, `filter`, `sort`, `get_column`, `to_frame`, etc.) to automatically preserve metadata. This means both of these will preserve metadata:
   - `df.with_columns(...)` ← regular Polars method (automatically patched)
   - `df.config_meta.with_columns(...)` ← through the namespace

   This behavior can be configured globally (see [Configuration](#configuration) below).

2. **Weak-Reference Based**
   We store metadata in class-level dictionaries keyed by `id(obj)` and hold a `weakref` to the frame or series object. Once the object is garbage-collected, the metadata is removed too.

3. **Works with DataFrames, LazyFrames, and Series**
   The plugin supports eager (`DataFrame`), lazy (`LazyFrame`), and columnar (`Series`) execution modes. Metadata flows seamlessly across type boundaries.

4. **Parquet Integration**
   - `df.config_meta.write_parquet("file.parquet")` automatically embeds the plugin metadata into the Arrow schema's `metadata`.
   - `read_parquet_with_meta("file.parquet")` reads the file, extracts that metadata, and reattaches it to the returned `DataFrame`.
   - `scan_parquet_with_meta("file.parquet")` scans the file, extracts that metadata, and reattaches it to the returned `LazyFrame`.
   - `series.config_meta.write_parquet("file.parquet")` converts to a single-column DataFrame and writes with metadata.

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

6. **Cross-Type Metadata Flow**
   Metadata automatically propagates across type boundaries:
```python
   df.config_meta.set(owner="Alice")
   s = df.get_column("a")        # Series inherits metadata
   df2 = s.to_frame()            # DataFrame inherits metadata
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

## Series Support

Series have full metadata support, including cross-type propagation:
```python
import polars as pl
import polars_config_meta

# Metadata flows from DataFrame to Series
df = pl.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
df.config_meta.set(owner="Alice", version=1)

s = df.get_column("foo")
print(s.config_meta.get_metadata())
# -> {'owner': 'Alice', 'version': 1}

# Metadata flows from Series to DataFrame
s2 = pl.Series("vals", [10, 20, 30])
s2.config_meta.set(source="sensor", calibrated=True)

df2 = s2.to_frame()
print(df2.config_meta.get_metadata())
# -> {'source': 'sensor', 'calibrated': True}

# Series method chains preserve metadata
s3 = s.sort().head(2)
print(s3.config_meta.get_metadata())
# -> {'owner': 'Alice', 'version': 1}

# Full chain: LazyFrame → DataFrame → Series
lf = pl.LazyFrame({"x": [1, 2, 3]})
lf.config_meta.set(pipeline="etl")
s4 = lf.collect().get_column("x")
print(s4.config_meta.get_metadata())
# -> {'pipeline': 'etl'}
```

### Series Parquet I/O

Series can be written to Parquet with metadata (converted to single-column DataFrame):
```python
s = pl.Series("values", [1, 2, 3])
s.config_meta.set(source="experiment_1")
s.config_meta.write_parquet("series_data.parquet")

# Read back as DataFrame
df = read_parquet_with_meta("series_data.parquet")
print(df.config_meta.get_metadata())
# -> {'source': 'experiment_1'}
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

- **`ConfigMetaOpts.enable_auto_preserve()`**: Enable automatic metadata preservation for regular DataFrame/LazyFrame/Series methods (this is the default behavior).
- **`ConfigMetaOpts.disable_auto_preserve()`**: Disable automatic preservation. Only `df.config_meta.<method>()` will preserve metadata.

**Note**: The `df.config_meta.<method>()` syntax **always** preserves metadata, regardless of the configuration setting.

## API Reference

### Setting and Retrieving Metadata

- **`df.config_meta.set(**kwargs)`**: Set metadata key-value pairs
```python
  df.config_meta.set(owner="Alice", confidence=0.95, version=2)
  s.config_meta.set(source="sensor", calibrated=True)
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

- **`df.config_meta.merge(*objs)`**: Merge metadata from other DataFrames, LazyFrames, or Series
```python
  df3.config_meta.merge(df1, df2)
  # df3 now has metadata from both df1 and df2

  # Cross-type merge also works:
  df.config_meta.merge(series1, series2)
  series.config_meta.merge(df1, df2)
```

- **`df.config_meta.clear_metadata()`**: Remove all metadata for this object
```python
  df.config_meta.clear_metadata()
```

### Parquet I/O

- **`df.config_meta.write_parquet(file_path, **kwargs)`**: Write to Parquet with embedded metadata
```python
  df.config_meta.write_parquet("output.parquet")
  series.config_meta.write_parquet("series.parquet")  # converts to single-column DataFrame
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

Any Polars DataFrame/LazyFrame/Series method can be called through `df.config_meta.<method>()` (and
likewise for LazyFrame and Series objects):

```python
# DataFrame methods:
df.config_meta.with_columns(new_col=pl.col("a") * 2)
df.config_meta.select(["a", "b"])
df.config_meta.filter(pl.col("a") > 0)
df.config_meta.get_column("a")  # returns Series with metadata

# Series methods:
s.config_meta.sort()
s.config_meta.head(10)
s.config_meta.filter(s > 0)
s.config_meta.to_frame()  # returns DataFrame with metadata
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
# Note: Later objects' values override earlier ones
```

### Cross-Type Workflows
```python
# Extract column, process, convert back
df = pl.DataFrame({"values": [3, 1, 4, 1, 5]})
df.config_meta.set(experiment="trial_1")

s = df.get_column("values").sort().head(3)
result = s.to_frame()
print(result.config_meta.get_metadata())
# -> {'experiment': 'trial_1'}
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

When you first access `.config_meta` on any DataFrame, LazyFrame, or Series, the plugin automatically patches methods that return any tracked type (DataFrame, LazyFrame, or Series). It determines which methods to patch by inspecting their return type annotations at runtime.

Patched methods include common Polars operations like `with_columns`, `select`, `filter`, `get_column`, `to_frame`, and so on.

All patched methods automatically copy metadata from the source object to the result, even across type boundaries (e.g., `DataFrame.get_column` returns a `Series` with the DataFrame's metadata).

### Storage and Garbage Collection

Internally, the plugin stores metadata in a global dictionary, `_df_id_to_meta`, keyed by `id(obj)`,
and also keeps a `weakref` to each object. As soon as an object is out of scope and
garbage-collected, the entry in `_df_id_to_meta` is automatically removed. This prevents memory
leaks and keeps the plugin usage simple.

### Method Interception

When you call `df.config_meta.some_method(...)`:

1. The plugin checks if `some_method` exists on the plugin itself (like `set`, `get_metadata`, `write_parquet`)
2. If not, it forwards the call to the underlying object's method
3. If the result is a new DataFrame/LazyFrame/Series, it automatically copies the metadata

## Caveats

- **Python-Layer Only**
  This is purely at the Python layer. Polars doesn't guarantee stable IDs or official hooks for such metadata.

- **Metadata is Ephemeral (Unless Saved)**
  Metadata is stored in memory and tied to object IDs. It won't survive serialization unless you explicitly use `df.config_meta.write_parquet()` and `read_parquet_with_meta()`.

- **Other Formats Not Supported**
  Currently, only Parquet format supports automatic metadata embedding/extraction. For CSV, Arrow, IPC, etc., you'd need to implement your own serialization logic.

- **Configuration is Global**
  The `ConfigMetaOpts` settings apply globally to all DataFrames, LazyFrames, and Series in your Python session.

- **Module-Level Functions Not Patched**
  Functions like `pl.concat()` are not methods, so they aren't automatically patched. Use `.merge()` to combine metadata after such operations.

## Diagnostics (Developer Tools)

The plugin provides a diagnostics module for inspecting method discovery and verifying that metadata patching is working correctly. These functions are intended for developers and can be run interactively or in tests. If you experience unexpected behaviour please try running these to diagnose the problem when filing a bug report.

### Available Functions

* `print_discovered_methods(cls)` prints all methods discovered for `DataFrame`, `LazyFrame`, or `Series`.
* `compare_discovered_methods()` compares discovered methods between `DataFrame`, `LazyFrame`, and `Series`.
* `check_method_discovered(method_name)` checks if a specific method was discovered.
* `verify_patching()` verifies that patching works as expected.

### Example Usage

- Adapted from the [discovery](https://github.com/lmmx/polars-config-meta/blob/master/tests/discovery_test.py) test module

```python
import polars as pl
from polars_config_meta.diagnostics import (
    print_discovered_methods,
    compare_discovered_methods,
    check_method_discovered,
    verify_patching,
)

# Print all discovered DataFrame methods
print_discovered_methods(pl.DataFrame)

# Print all discovered Series methods
print_discovered_methods(pl.Series)

# Compare DataFrame vs LazyFrame vs Series methods
compare_discovered_methods()

# Check critical methods individually
for method in ["with_columns", "select", "filter", "sort", "get_column", "to_frame"]:
    if not check_method_discovered(method):
        print(f"Method {method} is missing!")

# Verify that patching preserves metadata as expected
verify_patching()
```

## Contributing

1. **Issues & Discussions**: Please open a GitHub issue for bugs, ideas, or questions.
2. **Pull Requests**: PRs are welcome! This plugin is a community-driven approach to persist DataFrame-level metadata in Polars.

## Polars Development

There is ongoing work to support file-level metadata in the Polars Parquet writing, see [this PR](https://github.com/pola-rs/polars/pull/21806) for details. Once that lands, this plugin may be able to integrate more seamlessly.

## License

This project is licensed under the MIT License.
