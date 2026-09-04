"""Imports the grammar-schools register (reference_data/grammar_schools_
london_home_counties.csv) into the grammar_schools table. See
geo_model.school_registers for the shared geocode-then-upsert logic.

The source list (93 selective/partially-selective schools, pasted as a
markdown table) had two bad postcodes, corrected before this CSV was
written -- see the CSV's own git history / the import commit message for
the specifics (a genuine typo resolved via HERE geocoding by name, and a
postcode retired in 2002 resolved via postcodes.io's terminated-postcode
lookup to the nearest live postcode at the same coordinates). This module
just consumes the already-cleaned CSV.
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from geo_model.data.models import GrammarSchool
from geo_model.school_registers import import_school_register

EXTRA_FIELDS = ["region", "intake_type"]


def import_grammar_schools(session: Session, csv_path: Path) -> dict:
    return import_school_register(session, csv_path, GrammarSchool, EXTRA_FIELDS)
