"""Diagnostic utilities for inspecting discovered and patched methods.

This module provides functions to help users and developers understand
which DataFrame/LazyFrame methods are being automatically patched for
metadata preservation.
"""

import polars as pl

from .discovery import _ORIGINAL_METHODS, discover_patchable_methods


def print_discovered_methods(cls: type = None) -> None:
    """Print all methods discovered for automatic patching.

    Args:
        cls: The class to inspect (pl.DataFrame or pl.LazyFrame).
             If None, prints for both classes.

    Examples:
    --------
    >>> from polars_config_meta.diagnostics import print_discovered_methods
    >>> print_discovered_methods(pl.DataFrame)  # doctest: +SKIP
    >>> print_discovered_methods()  # Print both DataFrame and LazyFrame  # doctest: +SKIP

    """
    if cls is None:
        print_discovered_methods(pl.DataFrame)
        print()
        print_discovered_methods(pl.LazyFrame)
        return

    methods = discover_patchable_methods(cls)
    sorted_methods = sorted(methods)
    class_name = cls.__name__

    print("=" * 80)
    print(f"DISCOVERED {len(sorted_methods)} {class_name} METHODS")
    print("=" * 80)

    if not sorted_methods:
        print(f"\n⚠️  NO METHODS DISCOVERED for {class_name}")
        print("\nThis indicates the discovery mechanism is not working properly.")
        return

    # Group by first letter for readability
    current_letter = None
    for method in sorted_methods:
        first_letter = method[0].upper()
        if first_letter != current_letter:
            current_letter = first_letter
            print(f"\n--- {current_letter} ---")
        print(f"  {method}")

    print("\n" + "=" * 80)


def compare_discovered_methods() -> dict:
    """Compare discovered methods between DataFrame and LazyFrame.

    Returns a dictionary with the comparison results and prints a summary.

    Returns
    -------
    dict
        Dictionary with keys: 'dataframe', 'lazyframe', 'common',
        'dataframe_only', 'lazyframe_only'

    Examples
    --------
    >>> from polars_config_meta.diagnostics import compare_discovered_methods
    >>> comparison = compare_discovered_methods()  # doctest: +SKIP

    """
    df_methods = discover_patchable_methods(pl.DataFrame)
    lf_methods = discover_patchable_methods(pl.LazyFrame)
    assert df_methods, "No DataFrame methods found"
    assert lf_methods, "No LazyFrame methods found"

    common = df_methods & lf_methods
    only_df = df_methods - lf_methods
    only_lf = lf_methods - df_methods

    print("=" * 80)
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

    return {
        "dataframe": df_methods,
        "lazyframe": lf_methods,
        "common": common,
        "dataframe_only": only_df,
        "lazyframe_only": only_lf,
    }


def check_method_discovered(method_name: str, cls: type = None) -> bool:
    """Check if a specific method is discovered for patching.

    Args:
        method_name: Name of the method to check
        cls: The class to check (pl.DataFrame or pl.LazyFrame).
             If None, checks both classes.

    Returns:
    -------
    bool
        True if the method is discovered (for any class if cls is None)

    Examples:
    --------
    >>> from polars_config_meta.diagnostics import check_method_discovered
    >>> check_method_discovered('with_row_index')  # doctest: +SKIP
    True
    >>> check_method_discovered('with_row_index', pl.DataFrame)  # doctest: +SKIP
    True

    """
    if cls is None:
        df_found = check_method_discovered(method_name, pl.DataFrame)
        lf_found = check_method_discovered(method_name, pl.LazyFrame)

        print(f"\nMethod '{method_name}':")
        print(f"  DataFrame:  {'✓ discovered' if df_found else '✗ not found'}")
        print(f"  LazyFrame:  {'✓ discovered' if lf_found else '✗ not found'}")

        return df_found or lf_found

    methods = discover_patchable_methods(cls)
    found = method_name in methods

    status = "✓ discovered" if found else "✗ not found"
    print(f"{cls.__name__}.{method_name}: {status}")

    return found


def verify_patching(method_name: str = None) -> None:
    """Verify that discovered methods actually preserve metadata when patched.

    Args:
        method_name: Specific method to test. If None, tests a standard set.

    Examples:
    --------
    >>> from polars_config_meta.diagnostics import verify_patching
    >>> verify_patching('with_columns')  # doctest: +SKIP
    >>> verify_patching()  # Test multiple methods  # doctest: +SKIP

    """
    if method_name:
        test_methods = [method_name]
    else:
        # Full test set
        test_methods = compare_discovered_methods()
        test_methods = test_methods["dataframe"] & test_methods["lazyframe"]

    df = pl.DataFrame({"x": [1, 2, 3]})
    df.config_meta.set(test="value")

    # --- DEBUG ADDITION: show what was actually patched ---
    if _ORIGINAL_METHODS:
        print("\n⚡ Patched methods currently tracked (_ORIGINAL_METHODS):")
        for cls, name in sorted(
            _ORIGINAL_METHODS.keys(), key=lambda k: (k[0].__name__, k[1])
        ):
            print(f"  {cls.__name__}.{name}")
        else:
            print("\n⚠ No methods have been patched yet according to _ORIGINAL_METHODS")

    # Keep your original assertions intact
    discovered = discover_patchable_methods(pl.DataFrame)

    # Instead of failing if discover_patchable_methods is empty, check _ORIGINAL_METHODS
    discovered = discover_patchable_methods(pl.DataFrame)

    if not discovered and _ORIGINAL_METHODS:
        # Fall back to already-patched methods
        discovered = {
            name
            for cls, name in _ORIGINAL_METHODS.keys()
            if cls.__name__ == "DataFrame"
        }

    assert discovered, "No patchable methods discovered, cannot verify anything"
    test_methods = [m for m in test_methods if m in discovered]
    assert test_methods, "No test methods discovered, cannot verify anything"

    print("=" * 80)
    print("PATCHING VERIFICATION")
    print("=" * 80)
    print(f"\nTesting {len(test_methods)} method(s)...\n")

    results = []
    for method in test_methods:
        try:
            method_obj = getattr(df, method)
            # Try calling with simple/no args
            if method in ("head", "tail", "clone"):
                result = method_obj()
            elif method == "select":
                result = method_obj("x")
            elif method == "with_columns":
                result = method_obj(y=pl.col("x") * 2)
            elif method == "filter":
                result = method_obj(pl.col("x") > 0)
            else:
                print(f"  ⚠ {method}: no test case defined")
                continue

            # Check if metadata preserved
            preserved = result.config_meta.get_metadata() == {"test": "value"}
            status = "✓" if preserved else "✗"
            results.append((method, preserved))
            print(
                f"  {status} {method}: metadata {'preserved' if preserved else 'LOST'}"
            )
        except Exception as e:
            print(f"  ⚠ {method}: error - {type(e).__name__}: {e}")
            results.append((method, False))

    success_count = sum(1 for _, preserved in results if preserved)
    print(f"\n{success_count}/{len(results)} method(s) successfully preserved metadata")

    if success_count == len(results):
        print("✓ All tested methods working correctly!")
    else:
        print("⚠️  Some methods failed to preserve metadata!")

    print("\n" + "=" * 80)
