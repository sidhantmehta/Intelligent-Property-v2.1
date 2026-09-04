"""Imports the private-schools register (reference_data/private_schools_
greater_london.csv) into the private_schools table. See
geo_model.school_registers for the shared geocode-then-upsert logic.

The CSV was produced once from a scraped source spreadsheet (variable-
length blocks per school, delimited by Start/End markers, with blank
fields sometimes causing the next label to be misread as this field's
value) -- see scripts/convert_private_schools_xlsx.py for that parsing
logic. This module just consumes the already-cleaned CSV.
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from geo_model.data.models import PrivateSchool
from geo_model.school_registers import import_school_register

EXTRA_FIELDS = ["phone", "gender_profile", "size", "day_boarding_type", "religious_affiliation"]
FIELD_CONVERTERS = {"size": int}


def import_private_schools(session: Session, csv_path: Path) -> dict:
    return import_school_register(session, csv_path, PrivateSchool, EXTRA_FIELDS, FIELD_CONVERTERS)
