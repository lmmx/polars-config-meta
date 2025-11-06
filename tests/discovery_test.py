# tests/test_discovery_diagnostics.py
"""Diagnostic tests to show what methods are being discovered and patched."""

import polars as pl
from polars_config_meta.discovery import discover_patchable_methods


def test_show_discovered_dataframe_methods():
    """Show all DataFrame methods discovered by the current mechanism."""
    methods = discover_patchable_methods(pl.DataFrame)
    sorted_methods = sorted(methods)

    print("\n" + "=" * 80)
    print(f"DISCOVERED {len(sorted_methods)} DataFrame METHODS")
    print("=" * 80)

    if not sorted_methods:
        print("\n⚠️  NO METHODS DISCOVERED - Discovery mechanism may not be working!")
        print("\nThis likely means:")
        print("  - Methods don't have __annotations__")
        print("  - Or get_type_hints() is failing")
        print("  - Or the type matching logic isn't finding matches")
    else:
        # Group by first letter for readability
        current_letter = None
        for method in sorted_methods:
            first_letter = method[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                print(f"\n--- {current_letter} ---")
            print(f"  {method}")

    print("\n" + "=" * 80)


def test_show_discovered_lazyframe_methods():
    """Show all LazyFrame methods discovered by the current mechanism."""
    methods = discover_patchable_methods(pl.LazyFrame)
    sorted_methods = sorted(methods)

    print("\n" + "=" * 80)
    print(f"DISCOVERED {len(sorted_methods)} LazyFrame METHODS")
    print("=" * 80)

    if not sorted_methods:
        print("\n⚠️  NO METHODS DISCOVERED - Discovery mechanism may not be working!")
    else:
        # Group by first letter for readability
        current_letter = None
        for method in sorted_methods:
            first_letter = method[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                print(f"\n--- {current_letter} ---")
            print(f"  {method}")

    print("\n" + "=" * 80)


def test_show_method_overlap():
    """Show which methods are common vs unique to each class."""
    df_methods = discover_patchable_methods(pl.DataFrame)
    lf_methods = discover_patchable_methods(pl.LazyFrame)

    common = df_methods & lf_methods
    only_df = df_methods - lf_methods
    only_lf = lf_methods - df_methods

    print("\n" + "=" * 80)
    print("METHOD DISCOVERY COMPARISON")
    print("=" * 80)
    print(f"\nDataFrame methods:  {len(df_methods)}")
    print(f"LazyFrame methods:  {len(lf_methods)}")
    print(f"Common to both:     {len(common)}")
    print(f"DataFrame only:     {len(only_df)}")
    print(f"LazyFrame only:     {len(only_lf)}")

    if common:
        print(f"\n--- Common methods (all {len(common)}) ---")
        for method in sorted(common):
            print(f"  {method}")

    if only_df:
        print(f"\n--- DataFrame-only methods (all {len(only_df)}) ---")
        for method in sorted(only_df):
            print(f"  {method}")

    if only_lf:
        print(f"\n--- LazyFrame-only methods (all {len(only_lf)}) ---")
        for method in sorted(only_lf):
            print(f"  {method}")

    print("\n" + "=" * 80)


def test_check_specific_methods():
    """Check if specific important methods are being discovered."""
    df_methods = discover_patchable_methods(pl.DataFrame)
    lf_methods = discover_patchable_methods(pl.LazyFrame)

    # Methods we know should be there based on the issue report and tests
    critical_methods = [
        "with_row_index",  # The reported issue #33
        "with_columns",  # Core transformation
        "select",  # Core transformation
        "filter",  # Core transformation
        "sort",  # Core transformation
        "head",  # Used in tests
        "tail",  # Used in tests
        "clone",  # Used in tests
    ]

    print("\n" + "=" * 80)
    print("CRITICAL METHOD DISCOVERY CHECK")
    print("=" * 80)

    print("\nDataFrame:")
    all_found = True
    for method in critical_methods:
        found = method in df_methods
        status = "✓" if found else "✗"
        print(f"  {status} {method}")
        if not found:
            all_found = False

    print("\nLazyFrame:")
    for method in critical_methods:
        found = method in lf_methods
        status = "✓" if found else "✗"
        print(f"  {status} {method}")
        if not found:
            all_found = False

    if all_found:
        print("\n✓ All critical methods discovered successfully!")
    else:
        print("\n⚠️  Some critical methods are missing!")
        print("This may indicate the discovery mechanism needs adjustment.")

    print("\n" + "=" * 80)


def test_verify_patching_works():
    """Verify that discovered methods actually get patched and work correctly."""
    import polars_config_meta  # Trigger patching

    df = pl.DataFrame({"x": [1, 2, 3]})
    df.config_meta.set(test="value")

    discovered = discover_patchable_methods(pl.DataFrame)

    # Test a sample of discovered methods
    test_methods = ["with_columns", "select", "filter", "head", "tail", "clone"]
    test_methods = [m for m in test_methods if m in discovered]

    print("\n" + "=" * 80)
    print("PATCHING VERIFICATION")
    print("=" * 80)
    print(f"\nTesting {len(test_methods)} methods to verify patching works...")

    results = []
    for method_name in test_methods:
        try:
            method = getattr(df, method_name)
            # Try calling with simple/no args
            if method_name in ("head", "tail", "clone"):
                result = method()
            elif method_name == "select":
                result = method("x")
            elif method_name == "with_columns":
                result = method(y=pl.col("x") * 2)
            elif method_name == "filter":
                result = method(pl.col("x") > 0)
            else:
                continue

            # Check if metadata preserved
            preserved = result.config_meta.get_metadata() == {"test": "value"}
            status = "✓" if preserved else "✗"
            results.append((method_name, preserved))
            print(
                f"  {status} {method_name}: metadata {'preserved' if preserved else 'LOST'}"
            )
        except Exception as e:
            print(f"  ⚠ {method_name}: error - {e}")

    success_count = sum(1 for _, preserved in results if preserved)
    print(f"\n{success_count}/{len(results)} methods successfully preserved metadata")

    if success_count == len(results):
        print("✓ All tested methods working correctly!")
    else:
        print("⚠️  Some methods failed to preserve metadata!")

    print("\n" + "=" * 80)
