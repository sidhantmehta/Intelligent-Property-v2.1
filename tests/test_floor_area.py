from geo_model.domain import floor_area


def _rec(sector, outcode, area, ptype="T"):
    return floor_area.FloorAreaRecord(outcode=outcode, sector=sector, property_type=ptype, total_floor_area_m2=area)


def test_estimate_sector_floor_areas_uses_sector_when_not_sparse():
    records = [_rec("SW1A 1", "SW1A", 50.0 + i) for i in range(floor_area.MIN_CERTIFICATES)]
    estimates = floor_area.estimate_sector_floor_areas(records)
    e = next(e for e in estimates if e.property_type == "T")
    assert e.grain == "sector"
    assert e.certificate_count == floor_area.MIN_CERTIFICATES


def test_estimate_sector_floor_areas_backs_off_to_outcode_when_sector_sparse():
    sparse_sector = [_rec("SW1A 1", "SW1A", 60.0), _rec("SW1A 1", "SW1A", 65.0)]
    sibling_sector = [_rec("SW1A 2", "SW1A", 50.0 + i) for i in range(10)]
    estimates = floor_area.estimate_sector_floor_areas(sparse_sector + sibling_sector)
    e = next(e for e in estimates if e.key == "SW1A 1" and e.property_type == "T")
    assert e.grain == "outcode"
    assert e.certificate_count == 12  # pooled across both sectors


def test_estimate_sector_floor_areas_no_data_for_type():
    records = [_rec("SW1A 1", "SW1A", 60.0, ptype="F")]
    estimates = floor_area.estimate_sector_floor_areas(records)
    e = next(e for e in estimates if e.key == "SW1A 1" and e.property_type == "T")
    assert e.grain == "none"
    assert e.median_floor_area_m2 is None


def test_median_odd_and_even():
    records = [_rec("SW1A 1", "SW1A", v) for v in [40.0, 50.0, 60.0]]
    estimates = floor_area.estimate_sector_floor_areas(records)
    e = next(e for e in estimates if e.property_type == "T")
    assert e.median_floor_area_m2 == 50.0
