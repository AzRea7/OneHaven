import os
import pytest

from app.jobs.refresh import _missing_core_fields
from onehaven.backend.app.adapters.clients import rentcast_listings as rc

def test_missing_core_fields_detection():
    assert _missing_core_fields({"addressLine": "", "city": "", "zipCode": ""}) is True
    assert _missing_core_fields({"addressLine": "123", "city": "X", "zipCode": "48009"}) is False

def test_rentcast_sample_writer(tmp_path, monkeypatch):
    # Redirect sample dir into temp path
    monkeypatch.chdir(tmp_path)
    rc._write_sample("x", {"raw": {"a": 1}, "canon": {"b": 2}})
    assert os.path.exists("data/rentcast_samples")
    files = os.listdir("data/rentcast_samples")
    assert len(files) == 1
