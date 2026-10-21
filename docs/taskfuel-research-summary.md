# Taskfuel integration research summary

| Available feed | Value for the bot | Limitations |
|---|---|---|
| Hyperliquid funding, prices, OI, volume | Consistent response format | Duplicates directly available data |
| Predicted funding: Hyperliquid, Binance, Bybit | Easier cross-venue research | Live accuracy remains unverified |
| Six-venue funding, including Lighter | Less adapter work when expanding coverage | Some fields unavailable; additional dependency |
| Backpack and TradeXYZ | No demonstrated additional value | Coverage unconfirmed |

**PROS:**

- One interface across multiple venues.
- Less integration and maintenance work.
- Easier exploration of additional feeds.

**CONS:**

- Recurring charges for often-free public data.
- Additional outage and schema-change risks.
- No demonstrated unique data or speed advantage.

**Recommendation:** Keep direct feeds primary; use Taskfuel selectively for research or additional venue coverage.
