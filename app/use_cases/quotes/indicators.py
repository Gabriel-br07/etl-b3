"""Technical indicators on a close-price series (daily)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from app.core.market_data_constants import INDICATOR_PERIOD_MAX, INDICATOR_PERIOD_MIN


def validate_period(period: int) -> None:
    if period < INDICATOR_PERIOD_MIN or period > INDICATOR_PERIOD_MAX:
        raise ValueError(
            f"period must be between {INDICATOR_PERIOD_MIN} and {INDICATOR_PERIOD_MAX} inclusive"
        )


def _q(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def compute_sma(closes: list[Decimal], period: int) -> list[Decimal | None]:
    out: list[Decimal | None] = []
    for i in range(len(closes)):
        if i + 1 < period:
            out.append(None)
        else:
            window = closes[i - period + 1 : i + 1]
            total = sum(window, Decimal(0))
            out.append(_q(total / Decimal(period)))
    return out


def compute_ema(closes: list[Decimal], period: int) -> list[Decimal | None]:
    """EMA with smoothing α = 2/(period+1); seed = SMA of first ``period`` closes."""
    out: list[Decimal | None] = [None] * len(closes)
    if len(closes) < period:
        return out
    alpha = Decimal(2) / (Decimal(period) + 1)
    ema: Decimal = sum(closes[:period], Decimal(0)) / Decimal(period)
    out[period - 1] = _q(ema)
    for i in range(period, len(closes)):
        ema = alpha * closes[i] + (Decimal(1) - alpha) * ema
        out[i] = _q(ema)
    return out


def compute_rsi_wilder(closes: list[Decimal], period: int) -> list[Decimal | None]:
    """RSI (Wilder / RMA smoothing) on close-to-close changes. Output 0–100 scale."""
    out: list[Decimal | None] = [None] * len(closes)
    if len(closes) < period + 1:
        return out
    deltas: list[Decimal] = []
    for i in range(1, len(closes)):
        deltas.append(closes[i] - closes[i - 1])
    # Wilder: first avg gain/loss = simple mean of first `period` deltas
    gains = [d if d > 0 else Decimal(0) for d in deltas]
    losses = [(-d) if d < 0 else Decimal(0) for d in deltas]
    avg_gain: Decimal = sum(gains[:period], Decimal(0)) / Decimal(period)
    avg_loss: Decimal = sum(losses[:period], Decimal(0)) / Decimal(period)
    idx = period  # index in closes for first RSI (0-based)
    rs = _rs_ratio(avg_gain, avg_loss)
    out[idx] = _q(Decimal(100) - (Decimal(100) / (Decimal(1) + rs)))

    p = Decimal(period)
    p1 = Decimal(period - 1)
    for j in range(period, len(deltas)):
        avg_gain = (avg_gain * p1 + gains[j]) / p
        avg_loss = (avg_loss * p1 + losses[j]) / p
        idx = j + 1
        rs = _rs_ratio(avg_gain, avg_loss)
        out[idx] = _q(Decimal(100) - (Decimal(100) / (Decimal(1) + rs)))
    return out


def _rs_ratio(avg_gain: Decimal, avg_loss: Decimal) -> Decimal:
    if avg_loss == 0:
        if avg_gain == 0:
            return Decimal(1)
        return Decimal("1e9")
    return avg_gain / avg_loss


def build_indicator_series(
    dates: list[date],
    closes: list[Decimal],
    indicator: str,
    period: int,
) -> list[tuple[date, Decimal | None]]:
    validate_period(period)
    ind = indicator.strip().upper()
    if ind == "SMA":
        vals = compute_sma(closes, period)
    elif ind == "EMA":
        vals = compute_ema(closes, period)
    elif ind == "RSI":
        vals = compute_rsi_wilder(closes, period)
    else:
        raise ValueError(f"unsupported indicator {indicator!r}")
    return list(zip(dates, vals, strict=True))
