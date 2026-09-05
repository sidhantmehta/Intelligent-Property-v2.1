import datetime as dt

from geo_model.domain import address_match as am


def _epc(dwelling_key, address1=None, address2=None, ptype="T", area=80.0, sector="SW1A 1", outcode="SW1A"):
    return am.EpcRecord(
        dwelling_key=dwelling_key, postcode="SW1A 1AA", sector=sector, outcode=outcode,
        property_type=ptype, total_floor_area_m2=area, lodgement_date=dt.date(2020, 1, 1),
        address1=address1, address2=address2,
    )


def _ppd(tid, paon=None, saon=None, street=None, ptype="T", price=500000, date=dt.date(2021, 6, 1)):
    return am.PpdRecord(
        transaction_id=tid, postcode="SW1A 1AA", property_type=ptype, price=price, date=date,
        district="WESTMINSTER", paon=paon, saon=saon, street=street,
    )


def test_normalize_house_number():
    assert am.normalize_house_number("40") == "40"
    assert am.normalize_house_number("94A") == "94A"
    assert am.normalize_house_number("40 SHOOT UP HILL") == "40"
    assert am.normalize_house_number("The Cottage") is None
    assert am.normalize_house_number(None) is None


def test_normalize_flat_id():
    assert am.normalize_flat_id("FLAT 4") == "4"
    assert am.normalize_flat_id("APARTMENT 12B") == "12B"
    assert am.normalize_flat_id("4") == "4"
    assert am.normalize_flat_id("GROUND FLOOR FLAT") == "GROUND"
    assert am.normalize_flat_id("") is None
    assert am.normalize_flat_id(None) is None


def test_simple_house_match():
    epc = [_epc("E1", address1="12 MAPLE AVENUE")]
    ppd = [_ppd("P1", paon="12", street="MAPLE AVENUE")]
    results = am.match_records(epc, ppd)
    assert len(results) == 1
    assert results[0].confidence == "house_match"
    assert results[0].dwelling_key == "E1"
    assert results[0].transaction_id == "P1"


def test_house_with_repeat_sales_keeps_every_sale():
    epc = [_epc("E1", address1="12 MAPLE AVENUE")]
    ppd = [
        _ppd("P1", paon="12", street="MAPLE AVENUE", date=dt.date(2015, 1, 1), price=300000),
        _ppd("P2", paon="12", street="MAPLE AVENUE", date=dt.date(2022, 1, 1), price=550000),
    ]
    results = am.match_records(epc, ppd)
    assert len(results) == 2
    assert {r.transaction_id for r in results} == {"P1", "P2"}
    assert all(r.confidence == "house_match" for r in results)


def test_flat_exact_match_disambiguates_block():
    epc = [
        _epc("E1", address1="FLAT 1", address2="10 HIGH STREET", ptype="F"),
        _epc("E2", address1="FLAT 2", address2="10 HIGH STREET", ptype="F"),
    ]
    ppd = [_ppd("P1", paon="10", saon="FLAT 2", street="HIGH STREET", ptype="F")]
    results = am.match_records(epc, ppd)
    assert len(results) == 1
    assert results[0].dwelling_key == "E2"
    assert results[0].confidence == "flat_match_exact"


def test_flat_fuzzy_match():
    epc = [
        _epc("E1", address1="GROUND FLOOR FLAT", address2="10 HIGH STREET", ptype="F"),
        _epc("E2", address1="FLAT 2", address2="10 HIGH STREET", ptype="F"),
    ]
    ppd = [_ppd("P1", paon="10", saon="GROUND FLOOR FLAT", street="HIGH STREET", ptype="F")]
    results = am.match_records(epc, ppd)
    assert len(results) == 1
    assert results[0].dwelling_key == "E1"
    assert results[0].confidence == "flat_match_fuzzy"


def test_ambiguous_when_ppd_shows_subdivided_but_epc_has_one_dwelling():
    epc = [_epc("E1", address1="10 HIGH STREET", ptype="T")]
    ppd = [_ppd("P1", paon="10", saon="FLAT 1", street="HIGH STREET", ptype="F")]
    results = am.match_records(epc, ppd)
    assert len(results) == 1
    assert results[0].confidence == "ambiguous"


def test_ambiguous_when_flat_has_no_saon_but_epc_has_multiple_units():
    epc = [
        _epc("E1", address1="FLAT 1", address2="10 HIGH STREET", ptype="F"),
        _epc("E2", address1="FLAT 2", address2="10 HIGH STREET", ptype="F"),
    ]
    ppd = [_ppd("P1", paon="10", street="HIGH STREET", ptype="F")]  # no saon at all
    results = am.match_records(epc, ppd)
    assert len(results) == 2
    assert all(r.confidence == "ambiguous" for r in results)


def test_ambiguous_on_property_type_disagreement():
    epc = [_epc("E1", address1="12 MAPLE AVENUE", ptype="D")]
    ppd = [_ppd("P1", paon="12", street="MAPLE AVENUE", ptype="F")]
    results = am.match_records(epc, ppd)
    assert len(results) == 1
    assert results[0].confidence == "ambiguous"
    assert "disagreement" in results[0].match_note


def test_no_match_when_house_number_differs_real_world_example():
    # Real case that motivated this whole module: two genuinely different
    # buildings sharing postcode NW2 3QB -- "40, Flat 4, Shoot Up Hill"
    # (a Price Paid sale) and "Flat 4, 44 Shoot-Up Hill" (an EPC
    # certificate). Same flat number, different house number -- must NOT
    # match just because postcode + flat id happen to coincide.
    epc = [_epc("E1", address1="FLAT 4", address2="44 SHOOT-UP HILL", ptype="F")]
    ppd = [_ppd("P1", paon="40", saon="FLAT 4", street="SHOOT UP HILL", ptype="F")]
    results = am.match_records(epc, ppd)
    assert results == []


def test_no_match_when_dwelling_never_sold():
    epc = [_epc("E1", address1="12 MAPLE AVENUE")]
    ppd = [_ppd("P1", paon="99", street="MAPLE AVENUE")]
    results = am.match_records(epc, ppd)
    assert results == []


def test_named_house_has_no_number_to_match_on():
    epc = [_epc("E1", address1="THE COTTAGE, MAPLE AVENUE")]
    ppd = [_ppd("P1", paon="THE COTTAGE", street="MAPLE AVENUE")]
    results = am.match_records(epc, ppd)
    assert results == []
