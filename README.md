# DEX Funding Rate Arbitrage Bot

A Python bot that identifies and executes funding rate arbitrage opportunities between Backpack and Hyperliquid DEXes.

## Development in progress

... 

## Features
- Monitors funding rates across multiple DEXes
- Identifies profitable arbitrage opportunities
- Calculates potential profits and APR
- Automatically executes trades when opportunities are found
- Includes error handling and position management

## Setup
1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your settings in `config.yaml`

3. Run the bot:
```bash
python main.py
```

## Testing
Run the deterministic automated tests:
```bash
python -m unittest discover -v
```

## Lighter SDK
`requirements.txt` installs the official `lighter-sdk==1.2.0` release.
To inspect the configured Core mainnet key, run
`./venv/bin/python -m scripts.lighter_key_restrictions show`.
For policy examples, L1-signed replacement, expiry and rotation, see the
[API key restrictions guide](docs/lighter-sdk-november-2026.md#api-key-restrictions).

See [September 2026 migration notes](docs/lighter-sdk-september-2026.md) for the
earlier market-ID migration and account-type compatibility.

## Note
involves real money
