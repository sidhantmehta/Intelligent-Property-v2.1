import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from convert_private_schools_xlsx import _parse_block  # noqa: E402


def test_parse_block_all_fields_present():
    block = [
        "Park Hill School", "8 Queen's Road, Kingston upon Thames, Surrey, KT2 7SH", "+44 20 8546 5496",
        "Contact email",
        "Gender Profile", "Coeducational",
        "Size", 126,
        "Day/boarding type", "Day",
        "Religious affiliation", "All Faiths",
        "VIEW FULL PROFILE", "CONTACT SCHOOL",
    ]
    result = _parse_block(block)
    assert result["name"] == "Park Hill School"
    assert result["postcode"] == "KT2 7SH"
    assert result["gender_profile"] == "Coeducational"
    assert result["size"] == 126
    assert result["day_boarding_type"] == "Day"
    assert result["religious_affiliation"] == "All Faiths"


def test_parse_block_detects_blank_field_before_next_label():
    # Gender Profile has no value in the source -- "Size" immediately
    # follows it. Must NOT be read as gender_profile="Size".
    block = [
        "Rokeby", "George Road, Kingston upon Thames, KT2 7PB", "+44 20 8942 2247",
        "Contact email",
        "Gender Profile",
        "Size", 404,
        "Day/boarding type", "Day",
        "Religious affiliation", "All Faiths",
    ]
    result = _parse_block(block)
    assert result["gender_profile"] is None
    assert result["size"] == 404
    assert result["day_boarding_type"] == "Day"


def test_parse_block_detects_stray_distance_number_as_blank_affiliation():
    # Religious affiliation has no value -- a stray "miles" distance number
    # (leaked from elsewhere in the block) must not be captured as the
    # affiliation.
    block = [
        "Some School", "1 Road, Town, AB1 2CD", "+44 1234 567890",
        "Contact email",
        "Gender Profile", "Boys only",
        "Size", 200,
        "Day/boarding type", "Day",
        "Religious affiliation", 11.4,
    ]
    result = _parse_block(block)
    assert result["religious_affiliation"] is None


def test_parse_block_extracts_postcode_from_multiline_address():
    block = ["X School", "1 Long Address, Some District, London, SW1A 1AA", "phone"]
    result = _parse_block(block)
    assert result["postcode"] == "SW1A 1AA"


def test_parse_block_raises_without_extractable_postcode():
    block = ["X School", "An address with no postcode at all", "phone"]
    try:
        _parse_block(block)
        assert False, "expected ValueError"
    except ValueError:
        pass
