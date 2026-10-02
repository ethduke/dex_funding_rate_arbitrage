# Lighter SDK Update: October 2, 2026

## Install

The project now pins the published `lighter-sdk==1.1.6`, replacing the September
Git revision. It includes the SDK's `on_shutdown` callback. The callback alone
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
57,600 `sendTx`/`sendTxBatch` requests per minute; this is not a default entitlement.
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

- [SDK 1.1.6](https://pypi.org/project/lighter-sdk/1.1.6/)
- [SDK shutdown callback](https://github.com/elliottech/lighter-python/commit/106a5f4b2ec65f802eb558364b9eaabded320a3e)
- [Trades endpoint](https://apidocs.lighter.xyz/reference/trades)
- [Core rate limits](https://apidocs.lighter.xyz/docs/rate-limits)
- [Deposit quotes and supported chains](https://apidocs.lighter.xyz/docs/deposits-transfers-and-withdrawals)
