# Lighter SDK Update: September 11, 2026

## Installation

The project pins the official SDK to commit
`7d27964428220696861768260d68ad5d8f94438c`. PyPI and this Git revision both report
version `1.1.2`, but only the Git revision contains the August API models and
order-version signer ABI and September 11 signer binaries for newly assigned
market IDs. Do not rely on the version number alone.

For an existing virtual environment, force replacement of the older wheel:

```bash
./venv/bin/python -m pip install --force-reinstall --no-deps 'lighter-sdk @ git+https://github.com/elliottech/lighter-python.git@7d27964428220696861768260d68ad5d8f94438c'
./venv/bin/python -m pip check
./venv/bin/python -m unittest discover -v
```

New environments can use `pip install -r requirements.txt` as usual.

## Adapter Changes

- Market discovery uses `orderBookDetails` and `market_type == "perp"`.
  IDs are preserved as Python integers, with no perp/spot assumptions based on
  their magnitude. This adapter remains perpetual-only.
- Unknown symbols fail closed instead of falling back to market 1. Market 0
  remains valid after refresh. Disk-loaded mappings are revalidated against
  the configured instance before symbol resolution.
- Default WebSocket subscriptions resolve BTC/ETH from current metadata;
  explicitly configured unknown or non-perp IDs are rejected at initialization.
- The updated SDK supports the newly assigned IDs currently on testnet
  (ETH 4095, BTC 4096, SOL 4097). Its Python native signing boundary still uses
  `ctypes.c_int`, so orders with IDs outside the signed C-int range are rejected
  before signing to prevent silent truncation. Full-width 64-bit metadata is
  supported, but full-width 64-bit order signing needs a further upstream ABI
  update. We do not change ctypes types independently of the native library.

- Funding queries now pass integer lists to the SDK's `market_ids` argument.
  The pinned SDK serializes lists as repeated URL parameters, whereas the
  endpoint documents comma-separated IDs. Requests therefore use one market
  at a time, with independent pagination and duplicate input IDs removed.
  Legacy SDKs with singular `market_id` retain their fallback.
- Funding API errors and repeated pagination cursors return `None`, not an
  empty payment history, so the engine does not overwrite known PnL with zero.
- Mark-price candles use `CandlestickApi.mark_price_candles` and its response
  models instead of manually constructing HTTP requests. Existing normalized
  output and millisecond timestamps are preserved.
- WebSockets derive their instance and path from `LIGHTER_API_URL`, including
  Core/RHC mainnet and testnet. REST and WebSocket data no longer silently use
  different environments. This does not grant access to restricted hosts.
- Market orders keep the SDK-managed lazy nonce path and self-trade settings.
  No order modifications, partner fees, or account-tier changes are enabled.

## Announcements That Do Not Require Trading Changes

Starting **September 14, 2026 at 13:00 UTC**, Standard accounts cannot use
partner-attributed trades or integrator approvals. Our adapter does neither:
orders use the SDK's default integrator index of zero. No automatic tier switch
is needed. Any future partner integration must check Plus/Premium eligibility
and disclose fees before changing account tiers.

The new global daily Parquet export is separate from our account-scoped
`get_trade_export`. Its documented route is `/api/v1/export/historicalTrades`,
with a UTC `date` and `l1_address`; it returns a presigned URL valid for one hour.
It is not yet exposed by the pinned SDK. Access requires a one-time 100 LIT
in-app transfer, so no paid access, download, or new backtesting pipeline is
enabled by this update. Account CSV exports remain unchanged.

The September 4 SDK commit adds examples for TWAP, market take-profit, and
market stop-loss orders. These are not automatically applied to paired
arbitrage positions: independent execution or exits can leave an unhedged leg.

Direct CloudFront bypass requires Lighter approval and qualifying account/staking
status. Keep the existing public endpoint unless Lighter grants access and
provides the endpoint details. RHC maintenance is scheduled for September 5 at
12:00 UTC; avoid placing orders during that window.

## Sources

- [Official SDK revision](https://github.com/elliottech/lighter-python/commit/7d27964428220696861768260d68ad5d8f94438c)
- [Current testnet market metadata](https://testnet.zklighter.elliot.ai/api/v1/orderBookDetails)
- [PyPI release](https://pypi.org/project/lighter-sdk/)
- [Funding query SDK contract](https://github.com/elliottech/lighter-python/blob/fd4ee2530f78940cbed3dd80131d0fa66f74ad2a/docs/AccountApi.md#position_funding)
- [Historical exports](https://apidocs.lighter.xyz/reference/export_historicaltrades)
- [September 14 account restrictions](https://t.me/lighter_api_updates/166)
- [RHC maintenance](https://t.me/lighter_api_updates/170)
