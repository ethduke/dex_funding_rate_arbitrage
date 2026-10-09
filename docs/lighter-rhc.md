# Lighter on Robinhood Chain

`LighterRHCExchange` subclasses `LighterExchange` and is registered as
`LighterRHC`. It shares the SDK, market discovery, normalized data, execution,
and WebSocket lifecycle code, but has separate configuration, clients, caches,
account state, signers, and funding-payment accounting.

## Configuration

RHC is disabled by default. In `config.yaml`, set `LIGHTER_RHC_ACCOUNT_INDEX` to
the account index shown by the RHC app, and `LIGHTER_RHC_API_KEY_INDEX` to its
registered trading key index. Keep the API private key in the local `.env` or
environment variable `LIGHTER_RHC_PRIVATE_KEY`, never in YAML or tracked files.
Optionally set `LIGHTER_RHC_READ_ONLY_TOKEN` for maker-only-key status reads.

To compare Core with RHC, add `LighterRHC` to `EXCHANGES.ENABLED` and
`[Lighter, LighterRHC]` to `EXCHANGES.COMPARISON_PAIRS`. Existing venues and pairs
can stay enabled. `[LighterRHC, TradeXYZ]` and `[LighterRHC, Hyperliquid]` also
work for matching symbols. No `main.py` changes are needed.

The configured mainnet URL is `https://api.rh.lighter.xyz` (signing chain ID
466324). Testnet uses `https://api.rh-testnet.lighter.xyz` (chain ID 300).
WebSocket URLs and signing chain IDs follow the selected endpoint profile;
an RHC adapter cannot point to a Core URL. Custom RHC hosts are not supported.
Market IDs and precision are discovered on that instance, never copied from Core.

RHC has its own accounts, balances, account indexes, API keys, and nonces, even
when the L1 wallet is the same. Missing RHC credentials never fall back to Core.
Public funding/market discovery works without an account; private account
operations require an RHC account, and order execution additionally requires
a valid RHC trading key. API key indexes 4-156 and 158-254 are supported.

The current native SDK identifies signing clients by account/key index, not
chain. If both adapters would use the same pair of indexes simultaneously, the
second signer is disabled before it can overwrite the first. Register a different
API key index on one venue to resolve that collision.

Balances expose `collateral_asset`: `USDC` for Core and `USDG` for RHC. The
engine compares API-reported USD-equivalent values; it does not transfer funds
or convert between collateral tokens. RHC has its own fees and rate limits;
Core's premium quotas must not be assumed to apply.

## Monitoring and Validation

Funding uses only the instance's `exchange=lighter` rows, excluding the CEX
comparison rows also returned by its endpoint. Normalized records are labeled
`LighterRHC`, with the shared hourly funding convention. Actual funding payments
are tracked independently. Opening, rollback, monitoring, and closing use the
requested venue; closing an RHC leg must not close an unrelated Core position.

Funding is REST-polled. WebSockets supply order-book/account data using the
shared lifecycle wrapper. A failed startup handshake falls back to REST polling.

```bash
./venv/bin/python -m unittest tests.test_lighter_rhc -v
./venv/bin/python -m unittest discover -q
```

October 9 read-only validation returned 239 Core markets, 58 RHC markets, and
44 shared symbols in their funding responses. The RHC public WebSocket handshake
was rejected from the test environment and timed out; live WebSocket delivery
and authenticated RHC trading are not verified. No live orders were sent.

## References

- [Differences from Core](https://apidocs.lighter.xyz/docs/lighter-rh)
- [RHC setup](https://apidocs.rh.lighter.xyz/docs/get-started)
- [SDK endpoint profiles](https://github.com/elliottech/lighter-python/blob/main/lighter/endpoint_profiles.py)
