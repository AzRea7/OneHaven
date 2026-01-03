from app.domain.normalize import normalize_property_type, is_allowed_type


def test_property_type_normalization_and_filtering():
    assert normalize_property_type("Single Family") == "single_family"
    assert is_allowed_type("single_family") is True

    assert normalize_property_type("Condo") == "condo"
    assert is_allowed_type("condo") is False

    assert normalize_property_type("Manufactured Home") == "manufactured"
    assert is_allowed_type("manufactured") is False
