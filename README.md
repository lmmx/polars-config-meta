# polars-config-meta

**A Polars plugin for persistent metadata on DataFrames, LazyFrames, and Series.**

`polars-config-meta` attaches Python-side metadata to Polars objects and preserves it across transformations. It works by:

- Registering a `config_meta` namespace on `DataFrame`, `LazyFrame`, and `Series`
- Storing metadata in a dictionary keyed by `id(obj)`, with automatic **weak-reference cleanup**
- **Patching Polars methods** (`with_columns`, `filter`, `get_column`, `to_frame`, etc.) so metadata propagates automatically, even across type boundaries
- Optionally embedding metadata in **Parquet file-level metadata** via `write_parquet` / `read_parquet_with_meta`

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
   The plugin patches common Polars methods so metadata propagates automatically:
```python
   df.config_meta.set(owner="Alice")
   df2 = df.with_columns(doubled=pl.col("a") * 2)  # metadata preserved
   df2.config_meta.get_metadata()  # {'owner': 'Alice'}
```

2. **Cross-Type Flow**
   Metadata flows across DataFrame, LazyFrame, and Series boundaries:
```python
   s = df.get_column("a")   # Series inherits from DataFrame
   df2 = s.to_frame()       # DataFrame inherits from Series
```

3. **Weak-Reference Based**
   Metadata is stored keyed by `id(obj)` with weak references, so when the object is garbage-collected, its metadata is automatically cleaned up.

4. **Parquet Integration**
   Metadata embeds in Parquet file-level metadata and survives round-trips:
```python
   df.config_meta.write_parquet("data.parquet")
   df_loaded = read_parquet_with_meta("data.parquet")  # metadata restored
```

5. **Configurable**
   Auto-preservation can be disabled globally; the `df.config_meta.<method>()` syntax always preserves metadata regardless of configuration.

## Basic Usage

```python
import polars as pl
import polars_config_meta  # registers the plugin

df = pl.DataFrame({"a": [1, 2, 3]})
df.config_meta.set(owner="Alice", confidence=0.95)

# Metadata preserved through transformations:
df2 = (
    df.with_columns(squared=pl.col("a") ** 2)
      .filter(pl.col("squared") > 4)
      .select(["a", "squared"])
)
df2.config_meta.get_metadata()
# {'owner': 'Alice', 'confidence': 0.95}

# Flows across types:
s = df.get_column("a")
s.config_meta.get_metadata()
# {'owner': 'Alice', 'confidence': 0.95}

# Survives Parquet round-trip:
df.config_meta.write_parquet("output.parquet")

from polars_config_meta import read_parquet_with_meta
df_loaded = read_parquet_with_meta("output.parquet")
df_loaded.config_meta.get_metadata()
# {'owner': 'Alice', 'confidence': 0.95}
```

LazyFrame and Series work in the same way. Any method that returns a DataFrame, LazyFrame, or Series will propagate metadata from its source.

## Configuration

Auto-preservation is enabled by default. To disable:

```python
from polars_config_meta import ConfigMetaOpts

ConfigMetaOpts.disable_auto_preserve()

df = pl.DataFrame({"a": [1, 2, 3]})
df.config_meta.set(owner="Alice")

df.with_columns(doubled=pl.col("a") * 2).config_meta.get_metadata()
# {}

df.config_meta.with_columns(doubled=pl.col("a") * 2).config_meta.get_metadata()
# {'owner': 'Alice'}

ConfigMetaOpts.enable_auto_preserve()  # re-enable
```

The `obj.config_meta.<method>()` syntax preserves metadata regardless of this setting.

### Configuration Options

- **`ConfigMetaOpts.enable_auto_preserve()`**: Enable automatic metadata preservation for regular DataFrame/LazyFrame/Series methods (this is the default behavior).
- **`ConfigMetaOpts.disable_auto_preserve()`**: Disable automatic preservation. Only `df.config_meta.<method>()` will preserve metadata.

**Note**: The `df.config_meta.<method>()` syntax **always** preserves metadata, regardless of the configuration setting.

## API Reference

### Metadata Operations

| Method | Description |
|--------|-------------|
| `.config_meta.set(**kwargs)` | Set metadata key-value pairs |
| `.config_meta.get_metadata()` | Get all metadata as a dict |
| `.config_meta.update(mapping)` | Update metadata from a dict |
| `.config_meta.merge(*objs)` | Merge metadata from other objects (later wins) |
| `.config_meta.clear_metadata()` | Remove all metadata |

### Parquet I/O

| Function/Method | Description |
|-----------------|-------------|
| `.config_meta.write_parquet(path)` | Write with embedded metadata (Series converts to single-column DataFrame) |
| `read_parquet_with_meta(path)` | Read DataFrame with metadata |
| `scan_parquet_with_meta(path)` | Scan LazyFrame with metadata |

### Method Forwarding

Any Polars method can be called via `.config_meta.<method>()` to explicitly preserve metadata:

```python
df.config_meta.filter(pl.col("a") > 0)
s.config_meta.sort()
lf.config_meta.collect()
```

## How It Works

**Patching**: On first `.config_meta` access, the plugin inspects return type annotations and patches methods that return `DataFrame`, `LazyFrame`, or `Series`. Patched methods copy metadata from source to result.

**Storage**: Metadata lives in a global dict keyed by `id(obj)`. A weak reference to each object triggers automatic cleanup on garbage collection.

**Interception**: When you call `obj.config_meta.some_method(...)`:

1. If `some_method` is a plugin method (`set`, `get_metadata`, etc.), it runs directly
2. Otherwise, it forwards to the underlying Polars method and copies metadata to any returned DataFrame/LazyFrame/Series

## Caveats

- **Python-layer only**: Polars doesn't officially support metadata; this uses object IDs which aren't guaranteed stable
- **Ephemeral unless saved**: metadata won't survive pickling or other serialization; use Parquet for persistence
- **Parquet only**: CSV, Arrow, IPC don't support metadata embedding
- **Global configuration**: `ConfigMetaOpts` affects all objects in the session
- **Module functions not patched**: `pl.concat()` and similar aren't methods; use `.merge()` afterward

## Diagnostics (Developer Tools)

The plugin provides a diagnostics module for inspecting method discovery and verifying that metadata patching is working correctly. These functions are intended for developers and can be run interactively or in tests. If you experience unexpected behaviour please try running these to diagnose the problem when filing a bug report.

See the [discovery tests](https://github.com/lmmx/polars-config-meta/blob/master/tests/discovery_test.py) for examples.

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
