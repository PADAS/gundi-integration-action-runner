"""The reference-data response contract the Gundi portal reads.

These models moved here from the connectors that had grown them independently
(EarthRanger and Global Nature Watch had byte-identical copies), so the
defaults are a shared contract now and a change here reaches every connector.
"""
import pytest
from pydantic import ValidationError

from app.actions.core import ReferenceDataResponse, ReferenceOption


def test_only_value_is_required_and_label_defaults_to_nothing():
    """The portal falls back to `value` when `label` is absent, so a handler
    that knows nothing but the identifier still renders."""
    option = ReferenceOption(value="abc")

    assert option.value == "abc"
    assert option.label is None
    assert option.description is None
    assert option.group is None


def test_a_response_carries_its_cache_hint_and_completeness_by_default():
    """Defaults are the contract: a handler that returns a plain list of
    options is promising the portal a complete list it may cache."""
    response = ReferenceDataResponse(options=[ReferenceOption(value="a")]).dict()

    assert response == {
        "options": [{"value": "a", "label": None, "description": None, "group": None}],
        "cache_ttl_seconds": 300,
        "truncated": False,
    }


def test_a_capped_list_says_so():
    response = ReferenceDataResponse(
        options=[ReferenceOption(value="a", label="A", group="g")], truncated=True, cache_ttl_seconds=30
    ).dict()

    assert response["truncated"] is True
    assert response["cache_ttl_seconds"] == 30
    assert response["options"][0]["label"] == "A"


def test_options_are_required():
    with pytest.raises(ValidationError):
        ReferenceDataResponse()
