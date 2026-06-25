"""Unit tests for the iFDO completeness validator."""

import pytest

from marimba.core.validators import get_ifdo_validator, iFDOValidator

# The schema-required fields the validator checks, sourced from the validator itself so these tests track the
# bundled schema (if the iFDO spec's required set changes, the parametrised cases below change with it).
_REQUIRED_FIELDS: list[str] = iFDOValidator.create()._required_fields

# A representative selection of fields the schema does NOT require (so they must never be flagged as missing).
_NON_REQUIRED_FIELDS = ("image-acquisition", "image-entropy", "image-camera-yaw-degrees", "image-handle")


def _complete_header() -> dict[str, str]:
    """A header that populates every schema-required field with a dummy value."""
    return {field: f"value::{field}" for field in _REQUIRED_FIELDS}


class TestiFDOValidator:
    """The validator reports exactly the schema-required fields left unpopulated, and nothing else."""

    @pytest.mark.unit
    def test_required_set_is_non_trivial(self) -> None:
        """Guard: the required set actually loaded and includes known set-header and per-image fields."""
        assert {"image-set-name", "image-datetime", "image-license", "image-abstract"} <= set(_REQUIRED_FIELDS)
        # The non-required sample is genuinely outside the required set.
        assert not (set(_NON_REQUIRED_FIELDS) & set(_REQUIRED_FIELDS))

    @pytest.mark.unit
    def test_complete_ifdo_reports_nothing_missing(self) -> None:
        """When every required field is populated, nothing is flagged."""
        missing = iFDOValidator.create().unpopulated_required_fields(
            {"image-set-header": _complete_header(), "image-set-items": {}},
        )
        assert missing == []

    @pytest.mark.unit
    @pytest.mark.parametrize("field", _REQUIRED_FIELDS)
    def test_each_required_field_is_flagged_when_absent(self, field: str) -> None:
        """Removing any single required field flags exactly that field - the failure case for every one."""
        header = _complete_header()
        del header[field]
        missing = iFDOValidator.create().unpopulated_required_fields(
            {"image-set-header": header, "image-set-items": {}},
        )
        assert missing == [field]

    @pytest.mark.unit
    @pytest.mark.parametrize("field", _NON_REQUIRED_FIELDS)
    def test_absent_non_required_field_is_never_flagged(self, field: str) -> None:
        """A non-required field being absent never produces a warning (only the required set is checked)."""
        missing = iFDOValidator.create().unpopulated_required_fields(
            {"image-set-header": _complete_header(), "image-set-items": {"a.jpg": [{}]}},
        )
        assert missing == []
        assert field not in missing

    @pytest.mark.unit
    def test_field_populated_only_on_items_is_not_flagged(self) -> None:
        """A required field absent from the header but present on every item record counts as populated."""
        header = _complete_header()
        del header["image-datetime"]
        missing = iFDOValidator.create().unpopulated_required_fields(
            {"image-set-header": header, "image-set-items": {"a.jpg": [{"image-datetime": "2020-01-01 00:00:00"}]}},
        )
        assert missing == []

    @pytest.mark.unit
    def test_field_missing_from_one_item_is_flagged(self) -> None:
        """A required field present on only some item records is not populated for every image."""
        header = _complete_header()
        del header["image-datetime"]
        missing = iFDOValidator.create().unpopulated_required_fields(
            {
                "image-set-header": header,
                "image-set-items": {"a.jpg": [{"image-datetime": "2020-01-01 00:00:00"}], "b.jpg": [{}]},
            },
        )
        assert missing == ["image-datetime"]

    @pytest.mark.unit
    def test_present_but_empty_value_counts_as_populated(self) -> None:
        """Deliberate behaviour: a present key counts as populated, so image-set-handle='' (DOI-pending) is silent."""
        header = _complete_header()
        header["image-set-handle"] = ""
        missing = iFDOValidator.create().unpopulated_required_fields(
            {"image-set-header": header, "image-set-items": {}},
        )
        assert missing == []

    @pytest.mark.unit
    def test_empty_document_flags_all_required_fields(self) -> None:
        """An empty iFDO flags every required field (and the validator never raises)."""
        missing = iFDOValidator.create().unpopulated_required_fields({"image-set-header": {}, "image-set-items": {}})
        assert set(missing) == set(_REQUIRED_FIELDS)

    @pytest.mark.unit
    def test_malformed_items_does_not_raise(self) -> None:
        """Validation must never crash packaging, even if image-set-items is structurally wrong."""
        missing = iFDOValidator.create().unpopulated_required_fields(
            {"image-set-header": _complete_header(), "image-set-items": "not-a-mapping"},
        )
        assert missing == []  # header satisfies every required field; the bad items value is ignored safely

    @pytest.mark.unit
    def test_get_ifdo_validator_is_cached(self) -> None:
        """The module-level factory returns a single shared, parsed validator."""
        assert get_ifdo_validator() is get_ifdo_validator()
