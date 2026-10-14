# Lighter SDK Updates: October 2026

## Current SDK, Oracle and Maintenance Support

The current pin is upstream revision
`24fb46a7f7c5417b7abb7b2220635d55772f0ff6`, which updates the ctypes declarations
and bundled native signers to accept 64-bit market indexes. PyPI still reports
1.1.6, so installing that version alone does not include this patch. Install
`requirements.txt` to obtain the exact Git revision. The adapter's overflow guard
now follows the loaded signer ABI; SDK protocol validation can impose tighter
limits than the C integer type. Market types still come from metadata, not IDs.

Both `LighterExchange` and `LighterRHCExchange` now provide:

- `await exchange.get_price_oracle_info(symbol="BTC")`, or omit `symbol` to
  fetch all markets. The public endpoint's timestamp, index/mark source metadata,
  internal-price flags, median sources, cap basis points and reference prices
  are preserved. The SDK does not yet generate this endpoint, so the method uses
  its existing HTTP transport. Errors are explicit, not an empty success. This
  is diagnostic metadata, not an execution-price fallback or new funding source.
- `await exchange.get_service_status(force=False)`, using the instance's public
  Better Stack JSON feed. Responses are cached for 60 seconds; scheduled start
  times are reevaluated on each read. Resolved reports are ignored, and unresolved
  maintenance remains active after its estimated end. Unknown/custom/testnet
  hosts and failed feeds return `state="unknown"`, never a fabricated healthy
  state. No exchange credentials are sent to status hosts.
- Optional server-reported `liquidation_price` in normalized positions and nested
  balance positions. The announced multi-collateral correction is server-side;
  the adapter passes through its result without recomputing a USDC-only estimate.
  Missing values remain absent, and a supplied zero is preserved.

Before opening either arbitrage leg, the engine checks selected Lighter venues
for active maintenance and skips the pair when confirmed. Scheduled future
maintenance is exposed but does not force an early close. Status-feed failures
do not replace existing balance/signer/API checks, and do not disable closing,
rollback or monitoring. This is a best-effort entry check, not a guarantee of
exchange availability or a background notification service.

Use [Core's status feed](https://status.lighter.xyz/index.json) and
[RHC's status feed](https://robinhoodstatus.lighter.xyz/index.json).
`status.betterstack.com` reports Better Stack's own services, not Lighter Core.

## October 9 API Compatibility

As of October 9, SDK `1.1.6` was the latest upstream release; the current Git pin
above supersedes it. Some announced API
additions are not yet generated into the SDK, so the adapter bridges them:

- `get_recent_trades()` accepts keyword-only `integrator_account_index`,
  `order_index`, and `order_index_str`. The string filter takes precedence and
  preserves uint64 precision. New filters use the existing SDK HTTP transport
  and response models; ordinary requests retain the generated SDK method.
  Account scope, pagination, timeouts, and instance-specific auth are preserved.
  Omit the integrator filter (or use the documented `-1` default) to disable it;
  the server validates actual integrator indexes. Zero is not an "all" filter.
- Optionally set `LIGHTER_READ_ONLY_TOKEN` in the local environment for the
  maker-only-key status check on either instance. Without it, signed auth is
  unchanged. This token does not replace the trading key and is not used for
  other private endpoints. Never add token values to tracked config files.
- Account WebSocket callbacks ignore strictly older `transaction_time` values
  per account, including during automatic handover. Equal values are delivered
  because one transaction can produce distinct updates. Missing or invalid
  timestamps retain legacy delivery and do not advance the watermark. Explicit
  disconnect clears the watermark. This is state ordering, not event deduplication
  or lossless history; only the existing `account_all` subscription is used.
- Order submission results expose optional `remaining_send_tx` telemetry,
  including zero. SDK 1.1.6 stores this unknown field in `additional_properties`.
  Missing or invalid values become `None`. This is an advisory server snapshot,
  not a reservation or a local rate limiter; order scheduling remains unchanged.

RHC's new market IDs already use our metadata-driven discovery (`market_type`,
not numeric ranges) and instance-specific caches. The current native signer ABI
uses signed 64-bit market indexes; out-of-range IDs are rejected before
signing rather than truncated. No new subscriptions, deposit flows, or unused
metadata/referral/leaderboard integrations are introduced.

Reference schemas are now downloadable from
[Core](https://apidocs.lighter.xyz/openapi/page.json) and
[RHC](https://apidocs.rh.lighter.xyz/openapi/api.json).
The adapter does not download or regenerate code from these schemas at runtime.

## Install

The current Git pin supersedes the earlier PyPI `lighter-sdk==1.1.6` pin and
includes the SDK's `on_shutdown` callback. The callback alone
does not reconnect; our wrapper owns connection replacement and cleanup.

```bash
./venv/bin/python -m pip install -r requirements.txt
./venv/bin/python -m pip check
./venv/bin/python -m unittest discover -v
```

## WebSocket Lifecycle

- On `shutdown`, open one replacement while the old connection still serves data.
- Restore both order-book and account subscriptions. Wait for all initial
  snapshots before switching the active client and closing the old socket.
- Keep SDK books separate per connection. Only the active client publishes
  callbacks; publish the replacement's current snapshots when switching.
- Reconnect after unexpected closure, including code 1012 without a preceding
  shutdown event. Failed attempts back off from 1 to 30 seconds.
- A failed replacement does not prematurely close a still-working old socket.
- Startup waits for snapshots, not a fixed sleep. Close/disconnect cancels
  reconnects, closes active/warming sockets, and removes cached prices.
- Disconnected, empty, crossed, or over-30-second-old books cannot provide
  prices. SDK delta lists may be unsorted, so compute BBO across price levels.

This maintains current book/account state across a successful warm handover;
it is not an exactly-once event archive. Network failures, expired drain windows,
or unavailable replacement servers can still create a gap. This wrapper does
not subscribe to a trade-event stream or promise lossless replay.

## Public Core Trades

`get_recent_trades()` no longer initializes a signer for known Lighter Core
mainnet/testnet hosts. It preserves account scoping and maker/taker normalization.
It does not set the initialized-account flag merely to read public history, so
later order placement still initializes the signer normally.

Use `get_recent_trades(authenticated=True)` when L1-based rate-limit attribution
is desired, including Builder accounts. RHC and unknown/custom hosts continue
to require authentication by default. This change does not remove authentication
from exports, position funding, order placement, or other private endpoints.
Mainnet account indexes must not be reused blindly on testnet after resets.

## Limits and Earlier Deposit Announcements

Core endpoint weights now include `accountOrders`/`accountActiveOrders` at 100,
`trades` at 200, and `exchangeMetrics` at 120. The top Premium staking tier allows
72,000 `sendTx`/`sendTxBatch` requests per minute; this is not a default entitlement.
The repo has no local endpoint-weight table or tier-aware request budget to
change. Polling and order frequency are unchanged; server throttling still applies.
Do not apply Core-specific quotas to RHC.

The repo has no deposit orchestration flow. Fun.xyz quotes and Arc CCTP support
therefore require no change to the arbitrage adapter. No bridge API key, quote
request, UDA creation, deposit, or transfer is added or executed. A future deposit
feature would require a separate builder API key and explicit user controls.

Monitor Lighter's testnet channels for resets and testing windows on both instances.
This update does not add automated channel monitoring.

## Sources

- [64-bit SDK signer update](https://github.com/elliottech/lighter-python/commit/24fb46a7f7c5417b7abb7b2220635d55772f0ff6)
- [Core oracle metadata](https://apidocs.lighter.xyz/reference/priceoracleinfo)
- [RHC oracle metadata](https://apidocs.rh.lighter.xyz/reference/priceoracleinfo)
- [SDK 1.1.6](https://pypi.org/project/lighter-sdk/1.1.6/)
- [SDK shutdown callback](https://github.com/elliottech/lighter-python/commit/106a5f4b2ec65f802eb558364b9eaabded320a3e)
- [Trades endpoint](https://apidocs.lighter.xyz/reference/trades)
- [WebSocket reference](https://apidocs.lighter.xyz/docs/websocket-reference)
- [Core getting started](https://apidocs.lighter.xyz/docs/get-started)
- [RHC getting started](https://apidocs.rh.lighter.xyz/docs/get-started)
- [Core rate limits](https://apidocs.lighter.xyz/docs/rate-limits)
- [Deposit quotes and supported chains](https://apidocs.lighter.xyz/docs/deposits-transfers-and-withdrawals)
