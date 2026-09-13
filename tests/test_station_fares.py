from geo_model.domain import station_fares as sf


class _Station:
    def __init__(self, atco_code, name):
        self.atco_code = atco_code
        self.name = name


def _loc(nlc, description, crs, fare_group=None):
    return sf.FareLocationRecord(nlc=nlc, description=description, crs_code=crs, fare_group_nlc=fare_group or nlc)


def test_normalize_strips_rail_station_and_expands_abbreviations():
    assert sf.normalize_station_name("Clapham Junction Rail Station") == "CLAPHAM JN"
    assert sf.normalize_station_name("London Cannon Street Rail Station") == "LONDON CANNON ST"


def test_normalize_drops_parenthetical_disambiguator():
    assert sf.normalize_station_name("Reedham (Surrey) Rail Station") == "REEDHAM"


def test_consonant_skeleton_drops_internal_vowels_keeps_first_letter():
    assert sf.consonant_skeleton("PADDINGTON") == "PDDNGTN"
    assert sf.consonant_skeleton("HARLINGTON") == "HRLNGTN"


def test_exact_match_wins_over_fuzzy():
    stations = [_Station("9100VIC", "London Victoria Rail Station")]
    locs = [_loc("5426", "LONDON VICTORIA", "VIC", "1072")]
    result = sf.match_stations_to_fare_locations(stations, locs)
    assert len(result) == 1
    assert result[0].confidence == "exact"
    assert result[0].nlc == "5426"
    assert result[0].fare_group_nlc == "1072"


def test_fuzzy_match_recovers_abbreviated_multiword_name():
    # LOC's real abbreviation for Highbury & Islington.
    stations = [_Station("9100HGHI", "Highbury & Islington Rail Station")]
    locs = [_loc("6009", "HIGHBURY & ISLTN", "HHY")]
    result = sf.match_stations_to_fare_locations(stations, locs)
    assert result[0].confidence == "fuzzy"
    assert result[0].nlc == "6009"


def test_short_single_token_name_does_not_fuzzy_match_a_different_station():
    # Real false positive found during validation at a looser threshold:
    # "Chelmsford" and "Chelsfield" are unrelated stations ~200 miles
    # apart but their skeletons are similar enough to collide.
    stations = [_Station("9100CHLMSFD", "Chelmsford Rail Station")]
    locs = [_loc("5098", "CHELSFIELD", "CLD")]
    result = sf.match_stations_to_fare_locations(stations, locs)
    assert result[0].confidence == "unmatched"
    assert result[0].nlc is None


def test_no_candidate_at_all_is_unmatched_not_dropped():
    stations = [_Station("9100XXX", "Nowhere Rail Station")]
    result = sf.match_stations_to_fare_locations(stations, [])
    assert len(result) == 1
    assert result[0].confidence == "unmatched"
    assert result[0].station_atco_code == "9100XXX"


def test_ambiguous_fuzzy_candidates_stay_unmatched():
    stations = [_Station("9100AMB", "Amberley Cross Rail Station")]
    locs = [
        _loc("0001", "AMBERLEY CRSS", "AMB"),
        _loc("0002", "AMBERLY CROSS", "AMC"),
    ]
    result = sf.match_stations_to_fare_locations(stations, locs)
    # Both candidates collapse to the same consonant skeleton -- shouldn't guess.
    assert result[0].confidence == "unmatched"
    assert result[0].nlc is None


def test_find_flow_id_matches_direct_direction():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="5595", dest_nlc="1072", status_code="000", direction="S")]
    assert sf.find_flow_id({"5595"}, {"1072"}, flows) == "0000001"


def test_find_flow_id_matches_reverse_direction_when_reversible():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="1072", dest_nlc="5595", status_code="000", direction="R")]
    assert sf.find_flow_id({"5595"}, {"1072"}, flows) == "0000001"


def test_find_flow_id_does_not_reverse_single_direction_flow():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="1072", dest_nlc="5595", status_code="000", direction="S")]
    assert sf.find_flow_id({"5595"}, {"1072"}, flows) is None


def test_find_flow_id_ignores_non_adult_status():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="5595", dest_nlc="1072", status_code="001", direction="S")]
    assert sf.find_flow_id({"5595"}, {"1072"}, flows) is None


def test_build_flow_index_matches_find_flow_id_direct_direction():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="5595", dest_nlc="1072", status_code="000", direction="S")]
    index = sf.build_flow_index(flows)
    assert sf.find_flow_id_indexed({"5595"}, {"1072"}, index) == "0000001"


def test_build_flow_index_matches_find_flow_id_reverse_direction_when_reversible():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="1072", dest_nlc="5595", status_code="000", direction="R")]
    index = sf.build_flow_index(flows)
    assert sf.find_flow_id_indexed({"5595"}, {"1072"}, index) == "0000001"


def test_build_flow_index_does_not_reverse_single_direction_flow():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="1072", dest_nlc="5595", status_code="000", direction="S")]
    index = sf.build_flow_index(flows)
    assert sf.find_flow_id_indexed({"5595"}, {"1072"}, index) is None


def test_build_flow_index_ignores_non_adult_status():
    flows = [sf.FlowRecord(flow_id="0000001", origin_nlc="5595", dest_nlc="1072", status_code="001", direction="S")]
    index = sf.build_flow_index(flows)
    assert sf.find_flow_id_indexed({"5595"}, {"1072"}, index) is None


def test_monthly_from_weekly_applies_regulated_multiplier():
    assert sf.monthly_from_weekly(2450) == round(2450 * 3.84)


def test_extract_fares_for_flow_computes_monthly_from_weekly():
    fares = [
        sf.FareRecord(flow_id="1", ticket_code="SDR", fare_pence=840),
        sf.FareRecord(flow_id="1", ticket_code="7DS", fare_pence=2450),
    ]
    sdr, monthly = sf.extract_fares_for_flow(fares)
    assert sdr == 840
    assert monthly == round(2450 * 3.84)


def test_extract_fares_for_flow_missing_ticket_types_are_none():
    sdr, monthly = sf.extract_fares_for_flow([])
    assert sdr is None
    assert monthly is None


def test_select_monthly_season_prefers_zones_travelcard_when_present():
    # Real example: Gerrards Cross's rail-only weekly to "London
    # Terminals" is ~£87, but the Zones 1-6 Travelcard weekly is £115 --
    # the actually-advertised commuter price.
    station_fares_for_flow = [sf.FareRecord(flow_id="A", ticket_code="7DS", fare_pence=8710)]
    zones_fares_for_flow = [sf.FareRecord(flow_id="B", ticket_code="7TS", fare_pence=11500)]
    monthly, basis = sf.select_monthly_season(station_fares_for_flow, zones_fares_for_flow)
    assert basis == sf.MONTHLY_BASIS_ZONES_TRAVELCARD
    assert monthly == round(11500 * 3.84)


def test_select_monthly_season_falls_back_when_no_zones_flow():
    # Real example: Charlton (zone 4) has no Zones 1-6 flow at all --
    # its own rail fare already covers zonal travel.
    station_fares_for_flow = [sf.FareRecord(flow_id="A", ticket_code="7DS", fare_pence=3230)]
    monthly, basis = sf.select_monthly_season(station_fares_for_flow, None)
    assert basis == sf.MONTHLY_BASIS_STATION_OR_CLUSTER
    assert monthly == round(3230 * 3.84)


def test_select_monthly_season_falls_back_when_zones_flow_has_no_7ts():
    station_fares_for_flow = [sf.FareRecord(flow_id="A", ticket_code="7DS", fare_pence=3230)]
    zones_fares_for_flow = [sf.FareRecord(flow_id="B", ticket_code="ODT", fare_pence=2490)]
    monthly, basis = sf.select_monthly_season(station_fares_for_flow, zones_fares_for_flow)
    assert basis == sf.MONTHLY_BASIS_STATION_OR_CLUSTER
    assert monthly == round(3230 * 3.84)


def test_select_monthly_season_none_when_neither_has_a_weekly():
    monthly, basis = sf.select_monthly_season([], None)
    assert monthly is None
    assert basis == sf.MONTHLY_BASIS_STATION_OR_CLUSTER


def test_extract_ticket_fare_returns_none_when_absent():
    assert sf.extract_ticket_fare([], "SDR") is None
