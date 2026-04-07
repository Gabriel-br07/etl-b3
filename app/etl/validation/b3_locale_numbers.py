"""Parse B3 boletim CSV numeric cells (pt-BR locale and dot-decimal variants).

B3 exports may use comma as decimal separator, optional thousands grouping with
``.``, or ASCII dot decimals. Polars inference can pick ``Int64`` and then fail
on later fractional rows; reading selected columns as UTF-8 and casting through
this parser keeps validation deterministic without silent coercion of invalid
tokens (they become null).
"""

from __future__ import annotations


def parse_b3_locale_number(value: str | None) -> float | None:
    """Parse a single B3-style numeric cell into ``float``.

    Rules:
        - ``None``, empty string, and ``"-"`` (B3 placeholder) → ``None``.
        - If ``","`` is present, treat as **pt-BR**: remove ``"."`` (thousands),
          then replace ``","`` with ``"."`` and parse as float.
        - Otherwise parse with ``float()`` (ASCII dot as decimal).
        - Optional trailing ``%`` is stripped (e.g. variation columns).
        - Non-numeric garbage → ``None`` (no exception).

    Returns:
        Parsed float, or ``None`` for null/invalid inputs.
    """
    if value is None:
        return None
    t = value.strip() if isinstance(value, str) else str(value).strip()
    if t in {"", "-"}:
        return None
    if t.endswith("%"):
        t = t[:-1].strip()
    try:
        if "," in t:
            if "." in t:
                t = t.replace(".", "")
            t = t.replace(",", ".")
            return float(t)
        if "." in t:
            sign = ""
            body = t
            if body[:1] in "+-":
                sign = body[:1]
                body = body[1:]
            parts = body.split(".")
            if all(p.isdigit() for p in parts):
                if len(parts) > 2:
                    return float(sign + "".join(parts))
                if len(parts) == 2 and len(parts[1]) == 3:
                    # e.g. ``1.500`` → 1500 (pt-BR thousands, no fractional part)
                    return float(sign + parts[0] + parts[1])
        return float(t)
    except ValueError:
        return None
