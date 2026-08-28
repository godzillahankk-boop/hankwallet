from decimal import Decimal

from app.services import attention_scoring as s


def test_position_exposure_buckets() -> None:
    assert s.position_exposure_score(Decimal("4.99")) == 0
    assert s.position_exposure_score(Decimal("5")) == 5
    assert s.position_exposure_score(Decimal("20")) == 8
    assert s.position_exposure_score(Decimal("50")) == 10
    assert s.position_exposure_score(Decimal("100")) == 14
    assert s.position_exposure_score(Decimal("250")) == 17
    assert s.position_exposure_score(Decimal("500")) == 20


def test_dynamic_price_threshold_market_cap_liquidity_and_down_multiplier() -> None:
    base = s.price_threshold(5, Decimal("2000000"), Decimal("200000"), s.POSITIVE)
    thin = s.price_threshold(5, Decimal("2000000"), Decimal("10000"), s.POSITIVE)
    down = s.price_threshold(5, Decimal("2000000"), Decimal("200000"), s.NEGATIVE)

    assert base == Decimal("10.0")
    assert thin == Decimal("15.00")
    assert down == Decimal("8.500")


def test_price_impact_score_0_30_35_40() -> None:
    market_cap = Decimal("2000000")
    liquidity = Decimal("200000")

    assert s.price_impact_score(Decimal("9.9"), 5, market_cap, liquidity) == 0
    assert s.price_impact_score(Decimal("10"), 5, market_cap, liquidity) == 30
    assert s.price_impact_score(Decimal("15"), 5, market_cap, liquidity) == 35
    assert s.price_impact_score(Decimal("20"), 5, market_cap, liquidity) == 40


def test_abnormality_scores_percentiles_and_insufficient_history() -> None:
    history = [Decimal(i) for i in range(1, 101)]

    assert s.abnormality_score(Decimal("1"), history) == 0
    assert s.abnormality_score(Decimal("90"), history) == 5
    assert s.abnormality_score(Decimal("95"), history) == 10
    assert s.abnormality_score(Decimal("99"), history) == 15
    assert s.abnormality_score(Decimal("100"), [Decimal("1")]) == 5


def test_holder_growth_500_to_1000_and_5000_to_5500_differ() -> None:
    assert s.holder_breadth_score(1000, 500) == 20
    assert s.holder_breadth_score(5500, 5000) == 5


def test_holder_absolute_threshold_gate() -> None:
    assert s.holder_breadth_score(110, 100) == 0
    assert s.holder_breadth_score(130, 100) == 10


def test_top_holder_reduction_scores_and_peak_importance() -> None:
    assert s.top_holder_reduction_score(Decimal("90"), Decimal("100"), Decimal("0.05")) == 0
    assert s.top_holder_reduction_score(Decimal("70"), Decimal("100"), Decimal("0.05")) == 25
    assert s.top_holder_reduction_score(Decimal("50"), Decimal("100"), Decimal("0.05")) == 35
    assert s.top_holder_reduction_score(Decimal("20"), Decimal("100"), Decimal("0.05")) == 38
    assert s.top_holder_reduction_score(Decimal("0"), Decimal("100"), Decimal("0.05"), confirmed_closed=True) == 40
    assert s.top_holder_reduction_score(None, Decimal("100"), Decimal("0.05")) == 0
    assert s.top_holder_reduction_score(Decimal("0"), Decimal("100"), Decimal("0.009")) == 0


def test_holder_cluster_and_family_max() -> None:
    assert s.holder_cluster_score([Decimal("0.2"), Decimal("0.3"), Decimal("0.4")], None, None) == 30
    assert s.holder_cluster_score([], Decimal("1000"), Decimal("849")) == 0
    assert s.holder_family_score(10, 25, 30) == 30


def test_smart_money_tiers() -> None:
    liquidity = Decimal("100000")

    assert s.smart_money_score(1, Decimal("50"), liquidity, 0, 1) == 10
    assert s.smart_money_score(3, Decimal("1000"), liquidity, 0, 3) == 25
    assert s.smart_money_score(4, Decimal("3000"), liquidity, 0, 4) == 35
    assert s.smart_money_score(5, Decimal("5000"), liquidity, 0, 5) == 40
    assert s.smart_money_score(2, Decimal("-5000"), liquidity, 2, 2) == 40


def test_kol_tiers() -> None:
    assert s.kol_score(0, 0, 0, call_count=1) == 8
    assert s.kol_score(1, 0, 1) == 18
    assert s.kol_score(2, 0, 2) == 28
    assert s.kol_score(3, 0, 3) == 35
    assert s.kol_score(1, 2, 2) == 40


def test_liquidity_tiers_and_migration() -> None:
    assert s.liquidity_score(Decimal("81"), Decimal("100")) == 0
    assert s.liquidity_score(Decimal("75"), Decimal("100")) == 25
    assert s.liquidity_score(Decimal("60"), Decimal("100")) == 35
    assert s.liquidity_score(Decimal("49"), Decimal("100")) == 40
    assert s.liquidity_score(Decimal("40"), Decimal("100"), "migration") == 0


def test_secondary_formula_and_base_cap_and_levels() -> None:
    secondary = s.secondary_signal_score(
        {s.PRICE: 30, s.HOLDER: 20, s.SMART_MONEY: 10, s.KOL: 0},
        s.PRICE,
    )

    assert secondary.score == 15
    assert min(100, 40 + 20 + 20 + 20) == 100
    assert s.attention_level(39) == s.IGNORE
    assert s.attention_level(40) == s.NOTICE
    assert s.attention_level(55) == s.WARNING
    assert s.attention_level(75) == s.CRITICAL


def test_holder_score_change_changes_final_score() -> None:
    low = 30 + 10 + 5 + s.secondary_signal_score({s.PRICE: 30, s.HOLDER: 10}, s.PRICE).score
    high = 30 + 10 + 5 + s.secondary_signal_score({s.PRICE: 30, s.HOLDER: 20}, s.PRICE).score

    assert high > low


def test_direction_combines_independently_from_score() -> None:
    assert s.combine_direction([s.POSITIVE, s.NEGATIVE]) == s.MIXED
    assert s.combine_direction([s.MIXED, s.POSITIVE]) == s.MIXED
    assert s.combine_direction([s.MIXED, s.NEGATIVE]) == s.MIXED
    assert s.combine_direction([s.POSITIVE]) == s.POSITIVE
    assert s.combine_direction([s.NEGATIVE]) == s.NEGATIVE
    assert s.combine_direction([s.NEUTRAL]) == s.NEUTRAL


def test_final_attention_score_allows_dev_modifier_above_100() -> None:
    assert s.final_attention_score(100, 20) == 120
