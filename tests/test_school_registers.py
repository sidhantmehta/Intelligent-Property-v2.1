import csv
import datetime as dt

from geo_model.data.db import get_session, init_db
from geo_model.data.models import GrammarSchool, PrivateSchool
from geo_model.school_registers import import_school_register
from geo_model.private_schools import import_private_schools
from geo_model.grammar_schools import import_grammar_schools


def _write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def test_import_school_register_geocodes_and_upserts(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    csv_path = tmp_path / "schools.csv"
    _write_csv(
        csv_path,
        ["name", "address", "postcode", "size"],
        [
            {"name": "School A", "address": "1 Road, AB1 2CD", "postcode": "AB1 2CD", "size": "100"},
            {"name": "School B", "address": "2 Road, EF3 4GH", "postcode": "EF3 4GH", "size": ""},
        ],
    )

    def fake_fetch(postcodes):
        assert set(postcodes) == {"AB1 2CD", "EF3 4GH"}
        return {"AB1 2CD": (51.5, -0.1)}  # EF3 4GH deliberately unresolved

    monkeypatch.setattr("geo_model.school_registers.fetch_postcode_centroids", fake_fetch)

    with get_session() as session:
        result = import_school_register(session, csv_path, PrivateSchool, ["size"], {"size": int})

    assert result == {"total": 2, "geocoded": 1, "not_geocoded": 1}

    with get_session() as session:
        a = session.query(PrivateSchool).filter_by(name="School A").one()
        b = session.query(PrivateSchool).filter_by(name="School B").one()
        assert a.lat == 51.5 and a.long == -0.1 and a.size == 100 and a.geocoded_at is not None
        assert b.lat is None and b.size is None and b.geocoded_at is None


def test_import_school_register_upsert_updates_existing(tmp_path, monkeypatch):
    db_path = tmp_path / "test2.db"
    init_db(db_path)
    csv_path = tmp_path / "schools.csv"

    monkeypatch.setattr("geo_model.school_registers.fetch_postcode_centroids", lambda pcs: {p: (1.0, 2.0) for p in pcs})

    _write_csv(csv_path, ["name", "address", "postcode", "region", "intake_type"], [
        {"name": "Grammar A", "address": "old address", "postcode": "AB1 2CD", "region": "X", "intake_type": "Boys"},
    ])
    with get_session() as session:
        import_school_register(session, csv_path, GrammarSchool, ["region", "intake_type"])

    _write_csv(csv_path, ["name", "address", "postcode", "region", "intake_type"], [
        {"name": "Grammar A", "address": "new address", "postcode": "AB1 2CD", "region": "X", "intake_type": "Mixed"},
    ])
    with get_session() as session:
        import_school_register(session, csv_path, GrammarSchool, ["region", "intake_type"])

    with get_session() as session:
        rows = session.query(GrammarSchool).filter_by(name="Grammar A").all()
        assert len(rows) == 1
        assert rows[0].address == "new address"
        assert rows[0].intake_type == "Mixed"


def test_private_and_grammar_wrappers_delegate_correctly(tmp_path, monkeypatch):
    db_path = tmp_path / "test3.db"
    init_db(db_path)
    monkeypatch.setattr("geo_model.school_registers.fetch_postcode_centroids", lambda pcs: {p: (1.0, 2.0) for p in pcs})

    private_csv = tmp_path / "private.csv"
    _write_csv(private_csv, ["name", "address", "postcode", "phone", "gender_profile", "size", "day_boarding_type", "religious_affiliation"], [
        {"name": "P", "address": "addr", "postcode": "AB1 2CD", "phone": "123", "gender_profile": "Mixed", "size": "50", "day_boarding_type": "Day", "religious_affiliation": "None"},
    ])
    with get_session() as session:
        result = import_private_schools(session, private_csv)
    assert result["geocoded"] == 1

    grammar_csv = tmp_path / "grammar.csv"
    _write_csv(grammar_csv, ["name", "address", "postcode", "region", "intake_type"], [
        {"name": "G", "address": "addr", "postcode": "EF3 4GH", "region": "Kent", "intake_type": "Girls"},
    ])
    with get_session() as session:
        result = import_grammar_schools(session, grammar_csv)
    assert result["geocoded"] == 1

    with get_session() as session:
        p = session.query(PrivateSchool).filter_by(name="P").one()
        g = session.query(GrammarSchool).filter_by(name="G").one()
        assert p.size == 50
        assert g.region == "Kent" and g.intake_type == "Girls"
