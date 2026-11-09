# Lighter API Compatibility: November 2026

## SDK Installation

The current dependency is upstream revision
`38a34672e7cb43c67d807ece25f125fda2dfdecf` (package version 1.1.8).
It removes the obsolete urllib3 upper bound, so the earlier local compatibility
wheel is no longer required. This supersedes the wheel instructions in the
October/Taskfuel notes. Keep the patched HTTP pins in `requirements-http.txt`.

```bash
./venv/bin/python -m pip install -r requirements.txt
./venv/bin/python -m pip check
```

The generated SDK still does not expose accountOrderFills or the new stats
subscriptions. Thin extensions use its existing HTTP transport and our existing
WebSocket lifecycle. Core and Robinhood Chain share these extensions, with
separate hosts, account IDs, read-only tokens, and connection state.
Endpoint rollout can differ by instance; an unavailable private read raises an
error and does not silently return an empty history or query the other instance.

## Market Stats

When WebSockets are enabled, the adapter subscribes to perpetual stats for its
selected order-book markets. Order books continue to supply execution prices;
stats cannot overwrite their freshness timestamps or mid-prices. REST funding
polling remains enabled. Funding callbacks receive merged stats, and the engine
filters for the monitored symbol and converts hourly percentage points to
fractional hourly rates. REST eight-hour fractions are divided by eight once.

The existing WebSocket connection supports these additional operations:

```python
await exchange.ws_client.subscribe_market_stats([0, 1])
await exchange.ws_client.unsubscribe_market_stats([1])
await exchange.ws_client.set_market_stats_markets([0, 2])
snapshot = exchange.ws_client.get_market_snapshot(0)
```

- Up to 500 distinct IDs per selected-set subscription, validated before sending.
- `"all"` selects the `/all` stream; unsubscribe with no IDs to end it. Individual
  exclusions from `/all` are not supported. Empty selected sets unsubscribe.
- `spot=True` selects the independent spot channel. The caller must select IDs
  using market metadata, not numeric ID ranges. The arbitrage adapter itself
  selects perpetual markets only.
- Core uses `stats_partial=True` by default. Full initial/add-market snapshots
  prime the cache; partial updates merge only supplied fields, preserving zeros,
  false values and unchanged fields. Partial mode never uses single-market channels.
- RHC currently rejects bare multi-market channels. RHC and unknown/custom hosts
  default to full single-market streams (or full `/all`), without a `partial`
  flag. `multi_market_stats=True` can opt in after a server rollout is verified.
- Callbacks may be synchronous or asynchronous. Exceptions are isolated.
- Each connection has its own state. Warming sockets publish nothing until all
  requested snapshots arrive. Reconnects restore selected sets and partial mode.
- Removed markets are purged immediately; disconnected/stale snapshots return
  an empty dictionary. Stats are read-only diagnostics, not guaranteed history.
- Public-only probes may construct `LighterWebSocketClient` with `read_only=True`
  and `account_ids=[]` to use the documented `?readonly=true` connection. This
  does not enable trading or change the adapter's authenticated connection mode.

## Account Fills

```python
page = await exchange.get_account_order_fills(
    market_id, start_seconds, end_seconds, limit=1000, cursor=None,
)
history = await exchange.get_all_account_order_fills(
    market_id, start_seconds, end_seconds, limit=1000, max_pages=100,
)
```

Both methods query the configured account. They prefer the instance-specific
`LIGHTER_READ_ONLY_TOKEN` or `LIGHTER_RHC_READ_ONLY_TOKEN`; without one, they use
the existing signer-auth flow. Tokens are sent only in the authorization header.
Server/transport errors raise sanitized exceptions, not empty successful history.

Query times are Unix **seconds**, encoded as `start-end`; returned fill timestamps
are **milliseconds**. Amounts and order/trade IDs remain exact strings. The page
contains orders, deleverage and market-settlement fills, not individual executions
of every order. TWAP parents are excluded by the endpoint. Only order rows are
paged by limit/cursor, so the combined helper deduplicates across pages using
market, fill type and the applicable order/trade ID. Cursor loops, page limits
and failures raise rather than silently returning incomplete history.

This is a reconciliation read API. It does not change order confirmation, PnL,
automatic closing, or rollback behavior.

## Referrals and Withdrawals

`get_user_referrals(l1_address, cursor=None, limit=100)` preserves `l1_eligible`
independently of code binding and `used_code_eligible`. Missing eligibility is
not fabricated as false; explicit false remains false.

`get_withdrawal_history(cursor=None, status="all")` preserves `l2_tx_hash`,
`l1_claim_tx_hash`, and `l1_claim_amount`. Empty/missing claim fields are retained
without inventing a completed claim. The claim amount can cover multiple
withdrawals; never sum it per row as a withdrawal total. No withdrawal or
referral-binding transactions are submitted by these methods.

## Validation

Automated coverage includes real SDK HTTP serialization, instance-scoped auth,
exact amounts/IDs, pagination loops/duplicates/errors, partial merges, 500-ID
limits, concurrent add/remove, snapshot readiness and reconnect isolation.
Tests remain local according to the repository owner's commit preference.

Public REST metadata and oracle checks passed on both instances on November 9.
Ordinary WebSocket handshakes returned HTTP 400 from the validation environment;
the documented public read-only mode succeeded. Live checks passed for Core
multi-market partial snapshots/updates, RHC full single-market snapshots/updates,
spot stats on both, and adding/removing markets without reconnecting. This
exposed RHC's rejection of the new bare channel, now covered by a regression test.
Private account reads are covered with mocked transport, not real credentials.
No orders or transfers were submitted.

## Sources

- [SDK revision](https://github.com/elliottech/lighter-python/commit/38a34672e7cb43c67d807ece25f125fda2dfdecf)
- [WebSocket reference](https://docs.lighter.xyz/api/websocket)
- [Account order fills](https://docs.lighter.xyz/api/orders/accountorderfills)
- [User referrals](https://docs.lighter.xyz/api/referral/userreferrals)
- [Withdrawal history](https://docs.lighter.xyz/api/transactions/withdraw_history)
