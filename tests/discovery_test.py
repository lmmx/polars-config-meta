"""Diagnostic tests that use the diagnostics module."""

import polars as pl

from polars_config_meta.diagnostics import (
    check_method_discovered,
    compare_discovered_methods,
    print_discovered_methods,
    verify_patching,
)


def test_show_discovered_dataframe_methods():
    """Show all DataFrame methods discovered."""
    print()
    print_discovered_methods(pl.DataFrame)


def test_show_discovered_lazyframe_methods():
    """Show all LazyFrame methods discovered."""
    print()
    print_discovered_methods(pl.LazyFrame)


def test_show_method_overlap():
    """Show comparison between DataFrame and LazyFrame methods."""
    print()
    compare_discovered_methods()


def test_check_specific_methods():
    """Check if critical methods are discovered.

    We can be certain these will not change, so if they are missing something broke.
    """
    critical_methods = [
        "lazy",
        "with_columns",
        "select",
        "filter",
        "sort",
        "head",
        "tail",
        "clone",
    ]

    print()
    print("=" * 80)
    print("CRITICAL METHOD DISCOVERY CHECK")
    print("=" * 80)

    all_found = True
    for method in critical_methods:
        found = check_method_discovered(method)
        if not found:
            all_found = False

    if all_found:
        print("\n✓ All critical methods discovered successfully!")
        print("=" * 80)
    else:
        raise ValueError("Some critical methods are missing!")


def test_verify_patching_works():
    """Verify that patching works correctly."""
    print()
    verify_patching()
