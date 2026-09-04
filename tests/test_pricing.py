import datetime as dt

from geo_model.domain import pricing


def test_weighted_median_simple():
    assert pricing.weighted_median([(100.0, 1.0), (200.0, 1.0), (300.0, 1.0)]) == 200.0


def test_weighted_median_weights_skew_result():
    # A heavily-weighted low value pulls the median down from the
    # unweighted midpoint.
    result = pricing.weighted_median([(100.0, 10.0), (200.0, 1.0), (300.0, 1.0)])
    assert result == 100.0


def test_weighted_median_empty():
    assert pricing.weighted_median([]) is None


def test_adjust_to_today_scales_by_index_ratio():
    hpi = {
        ("CAMDEN", dt.date(2023, 1, 1)): {"index_all": 100.0, "index_terraced": 100.0},
        ("CAMDEN", dt.date(2026, 1, 1)): {"index_all": 120.0, "index_terraced": 120.0},
    }
    adjusted = pricing.adjust_to_today(
        price=500_000, sale_date=dt.date(2023, 1, 15), district="CAMDEN", property_type="T",
        hpi_by_district_month=hpi, as_of_month=dt.date(2026, 1, 1),
    )
    assert adjusted == 600_000.0


def test_adjust_to_today_missing_index_returns_none():
    adjusted = pricing.adjust_to_today(
        price=500_000, sale_date=dt.date(2023, 1, 15), district="NOWHERE", property_type="T",
        hpi_by_district_month={}, as_of_month=dt.date(2026, 1, 1),
    )
    assert adjusted is None


def _txn(sector, outcode, price, days_ago, ptype="T", district="CAMDEN", now=dt.date(2026, 1, 1)):
    return pricing.Transaction(
        outcode=outcode, sector=sector, property_type=ptype, price=price,
        date=now - dt.timedelta(days=days_ago), district=district, old_new="N", ppd_category="A",
    )


def _flat_hpi(district="CAMDEN"):
    # Flat index (no appreciation) across the whole window so estimated
    # medians equal the raw prices -- keeps the aggregation tests focused
    # on the sector/outcode backoff logic, not the HPI math (covered above).
    hpi = {}
    d = dt.date(2020, 1, 1)
    while d <= dt.date(2026, 6, 1):
        hpi[(district, d)] = {"index_all": 100.0, "index_detached": 100.0, "index_semi": 100.0, "index_terraced": 100.0, "index_flat": 100.0}
        d = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return hpi


def test_estimate_sector_prices_uses_sector_when_not_sparse():
    txns = [_txn("SW1A 1", "SW1A", 500_000 + i * 1000, days_ago=10) for i in range(pricing.MIN_TRANSACTIONS)]
    estimates = pricing.estimate_sector_prices(txns, _flat_hpi(), as_of_month=dt.date(2026, 1, 1), now=dt.date(2026, 1, 1))
    e = next(e for e in estimates if e.property_type == "T")
    assert e.grain == "sector"
    assert e.transaction_count == pricing.MIN_TRANSACTIONS


def test_estimate_sector_prices_backs_off_to_outcode_when_sector_sparse():
    # Only 2 sales in the sector itself (below MIN_TRANSACTIONS) but
    # plenty in a sibling sector of the same outcode.
    sparse_sector = [_txn("SW1A 1", "SW1A", 900_000, days_ago=10), _txn("SW1A 1", "SW1A", 950_000, days_ago=20)]
    sibling_sector = [_txn("SW1A 2", "SW1A", 500_000 + i * 1000, days_ago=10) for i in range(10)]
    estimates = pricing.estimate_sector_prices(
        sparse_sector + sibling_sector, _flat_hpi(), as_of_month=dt.date(2026, 1, 1), now=dt.date(2026, 1, 1),
    )
    e = next(e for e in estimates if e.key == "SW1A 1" and e.property_type == "T")
    assert e.grain == "outcode"
    assert e.transaction_count == 12  # pooled across both sectors


def test_estimate_sector_prices_excludes_new_build_and_category_b():
    resale = [_txn("SW1A 1", "SW1A", 500_000, days_ago=10) for _ in range(pricing.MIN_TRANSACTIONS)]
    new_build = pricing.Transaction(
        outcode="SW1A", sector="SW1A 1", property_type="T", price=999_999_999,
        date=dt.date(2026, 1, 1) - dt.timedelta(days=10), district="CAMDEN", old_new="Y", ppd_category="A",
    )
    repossession = pricing.Transaction(
        outcode="SW1A", sector="SW1A 1", property_type="T", price=1,
        date=dt.date(2026, 1, 1) - dt.timedelta(days=10), district="CAMDEN", old_new="N", ppd_category="B",
    )
    estimates = pricing.estimate_sector_prices(
        resale + [new_build, repossession], _flat_hpi(), as_of_month=dt.date(2026, 1, 1), now=dt.date(2026, 1, 1),
    )
    e = next(e for e in estimates if e.key == "SW1A 1" and e.property_type == "T")
    # Neither the absurd new-build price nor the GBP1 repossession should
    # have leaked into the median or the count.
    assert e.transaction_count == pricing.MIN_TRANSACTIONS
    assert e.median_price == 500_000.0
