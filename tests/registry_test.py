"""Tests for the id-keyed metadata registry and its weakref cleanup."""

import gc
import sys
import threading
import weakref

import polars as pl

from polars_config_meta import ConfigMetaPlugin


class _Dead:
    pass


def test_cleanup_removes_entries_when_frame_is_collected():
    """Collecting a frame removes its registry entries."""
    df = pl.DataFrame({"x": [1]})
    df.config_meta.set(owner="Alice")
    df_id = id(df)
    assert df_id in ConfigMetaPlugin._df_id_to_ref

    del df
    gc.collect()

    assert df_id not in ConfigMetaPlugin._df_id_to_ref
    assert df_id not in ConfigMetaPlugin._df_id_to_meta


def test_stale_entry_is_not_inherited():
    """A new frame must not pick up metadata left behind under its id."""
    df = pl.DataFrame({"x": [1]})
    df_id = id(df)
    dead = _Dead()
    stale_ref = weakref.ref(dead)
    del dead
    assert stale_ref() is None
    # Simulate an entry whose cleanup never ran for a dead object at this id.
    ConfigMetaPlugin._df_id_to_ref[df_id] = stale_ref
    ConfigMetaPlugin._df_id_to_meta[df_id] = {"owner": "stale"}

    assert df.config_meta.get_metadata() == {}


def test_concurrent_registration_and_collection():
    """Cleanup must not fail while another thread registers frames.

    The cleanup used to iterate the registry, which raised "dictionary changed
    size during iteration" when another thread added an entry mid-loop. The
    entry was then never removed, and later frames at the same id inherited its
    metadata.
    """
    errors = []
    previous_hook = sys.unraisablehook
    previous_interval = sys.getswitchinterval()
    sys.unraisablehook = lambda unraisable: errors.append(unraisable.exc_value)
    sys.setswitchinterval(1e-6)
    stop = threading.Event()

    def register_frames():
        keep = []
        while not stop.is_set():
            df = pl.DataFrame({"a": [1]})
            df.config_meta.set(owner="writer")
            keep.append(df)
            if len(keep) > 200:
                keep.clear()

    writer = threading.Thread(target=register_frames)
    writer.start()
    inherited = 0
    try:
        for i in range(20_000):
            df = pl.DataFrame({"a": [i]})
            if df.config_meta.get_metadata():
                inherited += 1
            del df
    finally:
        stop.set()
        writer.join()
        sys.setswitchinterval(previous_interval)
        sys.unraisablehook = previous_hook

    assert errors == []
    assert inherited == 0
