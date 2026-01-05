"""Automatic discovery and patching of DataFrame methods that return self.

This module handles the runtime discovery of Polars DataFrame, LazyFrame, and Series
methods that return instances of the same type, enabling automatic metadata preservation
across method calls.
"""

import inspect
from collections.abc import Callable
from typing import Any, get_type_hints

import polars as pl

# Store original methods before patching
_ORIGINAL_METHODS: dict[tuple[type, str], Any] = {}

# All types we track metadata for
_TRACKED_TYPES = (pl.DataFrame, pl.LazyFrame, pl.Series)
_TRACKED_TYPE_NAMES = {"DataFrame", "LazyFrame", "Series"}


def discover_patchable_methods(cls: type) -> set[str]:
    """Discover all methods of a class that return any tracked type.

    Uses type hints and return type inspection to identify methods that should
    preserve metadata when patched.

    Args:
        cls: The class to inspect (DataFrame, LazyFrame, or Series)

    Returns:
        Set of method names that return any tracked type (DataFrame, LazyFrame, or Series)

    """
    methods = set()

    for name in dir(cls):
        # Skip private/dunder methods
        if name.startswith("_"):
            continue

        attr = getattr(cls, name, None)
        if not callable(attr):
            continue

        # Skip properties, classmethods, and staticmethods
        try:
            static_attr = inspect.getattr_static(cls, name)
            if isinstance(static_attr, (property, classmethod, staticmethod)):
                continue
        except AttributeError:
            continue

        # Check if method returns any tracked type
        if _returns_tracked_type(attr):
            methods.add(name)

    return methods


def _returns_tracked_type(method: Callable) -> bool:
    """Check if a method's return type annotation indicates it returns a tracked type.

    Args:
        method: The method to inspect

    Returns:
        True if the method returns DataFrame, LazyFrame, or Series

    """
    # Try __annotations__ first (more reliable, doesn't need resolution)
    try:
        annotations = getattr(method, "__annotations__", {})
        if "return" in annotations:
            return_annotation = annotations["return"]

            # Handle string annotations
            if isinstance(return_annotation, str):
                if return_annotation == "Self" or "Self" in return_annotation:
                    return True
                return any(name in return_annotation for name in _TRACKED_TYPE_NAMES)

            # Handle direct class reference
            if return_annotation in _TRACKED_TYPES:
                return True

            # Handle string representation
            return_str = str(return_annotation)
            if "Self" in return_str:
                return True
            if any(name in return_str for name in _TRACKED_TYPE_NAMES):
                return True
    except (AttributeError, TypeError):
        pass

    # Fall back to get_type_hints if annotations didn't work
    try:
        hints = get_type_hints(method)
        return_type = hints.get("return")

        if return_type is None:
            return False

        # Direct class reference
        if return_type in _TRACKED_TYPES:
            return True

        # Convert return type to string for comparison
        return_type_str = str(return_type)

        if "Self" in return_type_str:
            return True
        return any(name in return_type_str for name in _TRACKED_TYPE_NAMES)

    except (AttributeError, NameError, TypeError):
        # Type hints unavailable or malformed
        return False


def patch_method(
    cls: type,
    method_name: str,
    metadata_copier: Callable[[Any, Any], Any],
) -> None:
    """Patch a single method to preserve metadata on return values.

    Args:
        cls: The class whose method should be patched
        method_name: Name of the method to patch
        metadata_copier: Function that copies metadata from source to result

    """
    if not hasattr(cls, method_name):
        return

    key = (cls, method_name)

    # Store original method only once
    if key not in _ORIGINAL_METHODS:
        original_method = getattr(cls, method_name)
        _ORIGINAL_METHODS[key] = original_method
    else:
        # Use stored original for re-patching
        original_method = _ORIGINAL_METHODS[key]

    def wrapped_method(self, *args, **kwargs):
        result = original_method(self, *args, **kwargs)
        return metadata_copier(self, result)

    setattr(cls, method_name, wrapped_method)


def unpatch_all_methods() -> None:
    """Restore all original methods, removing metadata preservation patches."""
    for (cls, method_name), original_method in _ORIGINAL_METHODS.items():
        setattr(cls, method_name, original_method)
