import datetime as dt

from geo_model.domain import matched_pricing as mp

AS_OF = dt.date(2026, 1, 1)


def _hpi_flat_index(district="WESTMINSTER"):
    # A flat index across sale dates and as-of month means adjust_to_today
    # is a no-op (ratio 1.0), so tests can reason directly in raw prices.
    hpi = {}
    for year in range(2020, 2027):
        for month in range(1, 13):
            hpi[(district, dt.date(year, month, 1))] = {
                "index_all": 100.0, "index_detached": 100.0, "index_semi": 100.0,
                "index_terraced": 100.0, "index_flat": 100.0,
            }
    return hpi


def _sale(sector, outcode, ptype, area, price, date=dt.date(2025, 6, 1), district="WESTMINSTER"):
    return mp.MatchedSale(
        sector=sector, outcode=outcode, property_type=ptype, total_floor_area_m2=area,
        sale_price=price, sale_date=date, district=district, confidence="house_match",
    )


def test_size_bin_of():
    assert mp.size_bin_of(0) == 0
    assert mp.size_bin_of(99.9) == 0
    assert mp.size_bin_of(100.0) == 100
    assert mp.size_bin_of(249.9) == 200


def test_estimate_uses_sector_when_not_sparse():
    sales = [_sale("SW1A 1", "SW1A", "T", 100.0, 500000 + i * 1000) for i in range(mp.MIN_MATCHED)]
    hpi = _hpi_flat_index()
    estimates = mp.estimate_price_per_sqm(sales, hpi, AS_OF)
    overall = next(e for e in estimates if e.property_type == "T" and e.size_bin_m2 is None)
    assert overall.grain == "sector"
    assert overall.matched_count == mp.MIN_MATCHED
    assert overall.median_price_per_sqm == 5020.0  # median price 502000 / 100 m2


def test_estimate_backs_off_to_outcode_when_sparse():
    sparse = [_sale("SW1A 1", "SW1A", "T", 100.0, 500000)]
    sibling = [_sale("SW1A 2", "SW1A", "T", 100.0, 400000 + i * 1000) for i in range(mp.MIN_MATCHED)]
    hpi = _hpi_flat_index()
    estimates = mp.estimate_price_per_sqm(sparse + sibling, hpi, AS_OF)
    overall = next(e for e in estimates if e.key == "SW1A 1" and e.property_type == "T" and e.size_bin_m2 is None)
    assert overall.grain == "outcode"
    assert overall.matched_count == mp.MIN_MATCHED + 1  # pooled across both sectors


def test_no_data_for_type_is_none_grain():
    sales = [_sale("SW1A 1", "SW1A", "F", 50.0, 300000)]
    hpi = _hpi_flat_index()
    estimates = mp.estimate_price_per_sqm(sales, hpi, AS_OF)
    overall = next(e for e in estimates if e.key == "SW1A 1" and e.property_type == "T" and e.size_bin_m2 is None)
    assert overall.grain == "none"
    assert overall.median_price_per_sqm is None


def test_size_bin_breakdown_only_when_enough_matched():
    # 5 sales at ~100-104m2 (bin 100) and only 2 at ~250-260m2 (bin 200) --
    # the smaller houses should get a bin, the larger ones shouldn't.
    small = [_sale("SW1A 1", "SW1A", "D", 100.0 + i, 500000 + i * 5000) for i in range(5)]
    large = [_sale("SW1A 1", "SW1A", "D", 250.0, 1200000), _sale("SW1A 1", "SW1A", "D", 260.0, 1300000)]
    hpi = _hpi_flat_index()
    estimates = mp.estimate_price_per_sqm(small + large, hpi, AS_OF)
    bins = {e.size_bin_m2 for e in estimates if e.key == "SW1A 1" and e.property_type == "D"}
    assert 100 in bins
    assert 200 not in bins


def test_bin_estimate_does_not_back_off_to_outcode():
    # A sparse bin in one sector should simply not appear, even though a
    # sibling sector has plenty of sales in that same bin -- bins never
    # borrow from the outcode.
    sparse_bin = [_sale("SW1A 1", "SW1A", "D", 100.0, 500000)]
    sibling_bin = [_sale("SW1A 2", "SW1A", "D", 100.0 + i, 500000 + i * 1000) for i in range(mp.MIN_MATCHED_FOR_BIN)]
    hpi = _hpi_flat_index()
    estimates = mp.estimate_price_per_sqm(sparse_bin + sibling_bin, hpi, AS_OF)
    sw1a1_bins = [e for e in estimates if e.key == "SW1A 1" and e.property_type == "D" and e.size_bin_m2 is not None]
    assert sw1a1_bins == []
