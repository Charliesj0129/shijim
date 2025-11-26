import pytest
from shijim.features.ofi import OFICalculator, OFISignal
from shijim.events.schema import MDBookEvent

def test_ofi_initial_state():
    calc = OFICalculator()
    event = MDBookEvent(
        ts_ns=100, 
        symbol="TEST", 
        asset_type="stock", 
        exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    
    # First event should return None (no previous state)
    result = calc.calculate(event)
    assert result is None

def test_ofi_volume_increase():
    calc = OFICalculator()
    symbol = "TEST"
    
    # T0
    event0 = MDBookEvent(
        ts_ns=100, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    calc.calculate(event0)
    
    # T1: Bid volume increases (+5), Ask volume increases (+5)
    # OFI = (100>=100)*15 - (100<=100)*10 + ...
    # Bid term: 15 - 10 = +5
    # Ask term: -(15 if 101<=101 else 0) + (10 if 101>=101 else 0) = -15 + 10 = -5
    # Total OFI = 5 - 5 = 0? Wait.
    # Let's recheck logic.
    # Bid: b_n=100, b_prev=100. term1=15, term2=10. contrib=5. Correct (buying pressure).
    # Ask: a_n=101, a_prev=101. term3=15, term4=10. contrib=-5. Correct (selling pressure).
    # Total = 0.
    
    event1 = MDBookEvent(
        ts_ns=101, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[15],
        ask_prices=[101.0], ask_volumes=[15]
    )
    result = calc.calculate(event1)
    assert result.ofi_value == 0.0
    
    # T2: Bid volume increases (+5), Ask volume constant
    # Bid contrib = +5
    # Ask contrib = 0
    # Total = 5
    event2 = MDBookEvent(
        ts_ns=102, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[20],
        ask_prices=[101.0], ask_volumes=[15]
    )
    result = calc.calculate(event2)
    assert result.ofi_value == 5.0

def test_ofi_price_change():
    calc = OFICalculator()
    symbol = "TEST"
    
    # T0: Bid 100 (10), Ask 101 (10)
    event0 = MDBookEvent(
        ts_ns=100, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    calc.calculate(event0)
    
    # T1: Bid moves up to 100.5 (5)
    # b_n=100.5, b_prev=100
    # term1 (b_n >= b_prev): 5
    # term2 (b_n <= b_prev): 0 (100.5 > 100)
    # Bid contrib = 5
    # Ask constant -> 0
    # Total = 5
    event1 = MDBookEvent(
        ts_ns=101, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.5], bid_volumes=[5],
        ask_prices=[101.0], ask_volumes=[10]
    )
    result = calc.calculate(event1)
    assert result.ofi_value == 5.0
    
    # T2: Bid moves down to 100 (10)
    # b_n=100, b_prev=100.5
    # term1 (100 >= 100.5): 0
    # term2 (100 <= 100.5): 10 (prev vol was 5) -> Wait, q_{n-1}^b is 5.
    # Bid contrib = -5
    # Total = -5
    event2 = MDBookEvent(
        ts_ns=102, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    result = calc.calculate(event2)
    assert result.ofi_value == -5.0

def test_ofi_ask_price_change():
    calc = OFICalculator()
    symbol = "TEST"
    
    # T0: Bid 100 (10), Ask 101 (10)
    event0 = MDBookEvent(
        ts_ns=100, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    calc.calculate(event0)
    
    # T1: Ask moves up to 101.5 (5) (Less selling pressure?)
    # a_n=101.5, a_prev=101
    # term3 (a_n <= a_prev): 0
    # term4 (a_n >= a_prev): 10 (prev vol)
    # Ask contrib = 10
    # Total = 10
    # Logic check: Ask moving up means sellers are retreating, which is bullish. Positive OFI. Correct.
    event1 = MDBookEvent(
        ts_ns=101, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.5], ask_volumes=[5]
    )
    result = calc.calculate(event1)
    assert result.ofi_value == 10.0

def test_ofi_accumulator():
    from shijim.features.ofi import OFIAccumulator
    
    acc = OFIAccumulator(interval_seconds=1.0)
    symbol = "TEST"
    
    # T0: Initial
    event0 = MDBookEvent(
        ts_ns=1_000_000_000, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    assert acc.process(event0) is None
    
    # T1: +0.5s, OFI=+5
    event1 = MDBookEvent(
        ts_ns=1_500_000_000, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[15],
        ask_prices=[101.0], ask_volumes=[10]
    )
    assert acc.process(event1) is None
    
    # T2: +1.0s (total 1.0s from T0), OFI=+5
    # Should emit sum = 5 + 5 = 10
    event2 = MDBookEvent(
        ts_ns=2_000_000_000, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[20],
        ask_prices=[101.0], ask_volumes=[10]
    )
    result = acc.process(event2)
    assert result is not None
    assert result.symbol == symbol
    assert result.ts_ns == 2_000_000_000
    assert result.ofi_value == 10.0
    
    # T3: +0.1s, OFI=-5
    event3 = MDBookEvent(
        ts_ns=2_100_000_000, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[15],
        ask_prices=[101.0], ask_volumes=[10]
    )
    assert acc.process(event3) is None
    
    # T4: +1.0s from T2 (total 2.0s from T0), OFI=0
    event4 = MDBookEvent(
        ts_ns=3_000_000_000, symbol=symbol, asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[15],
        ask_prices=[101.0], ask_volumes=[10]
    )
    result = acc.process(event4)
    assert result is not None
    assert result.ofi_value == -5.0
