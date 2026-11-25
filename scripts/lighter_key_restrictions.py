"""Inspect or replace restrictions for the configured Lighter Core mainnet API key."""

import argparse
import asyncio
from getpass import getpass, GetPassWarning
import json
import os
from pathlib import Path
import warnings

from dotenv import load_dotenv
import lighter
from lighter.endpoint_profiles import MAINNET
import yaml

from model.exchanges.lighter_key_restrictions import (
    read_restrictions, replace_restrictions, require_core_mainnet, validate_policy,
)


async def run(args):
    root = Path(__file__).resolve().parents[1]
    config = yaml.safe_load((root / "config.yaml").read_text())
    require_core_mainnet(config["LIGHTER_API_URL"])
    account = int(config["LIGHTER_ACCOUNT_INDEX"])
    index = int(config["LIGHTER_API_KEY_INDEX"])
    if account < 0 or not 0 <= index <= 254:
        raise ValueError("Invalid configured account or API key index")
    load_dotenv(root / ".env")
    token = os.environ.get("LIGHTER_READ_ONLY_TOKEN", "").strip()
    if args.action == "show" and token:
        async with lighter.ApiClient(lighter.Configuration(host=MAINNET.api_url)) as client:
            return await read_restrictions(lighter.AccountApi(client), account, index, token)

    if args.action == "replace":
        policy = json.loads(args.policy.read_text())
        validate_policy(policy)
        if index < 4:
            raise ValueError("API key indexes 0 to 3 are reserved")
    private_key = os.environ.get("LIGHTER_PRIVATE_KEY")
    if not private_key:
        raise ValueError("The SDK signer requires LIGHTER_PRIVATE_KEY")
    signer = lighter.SignerClient(
        url=MAINNET.api_url, account_index=account,
        api_private_keys={index: private_key}, chain_id=MAINNET.chain_id,
    )
    try:
        if args.action == "show":
            token, error = signer.create_auth_token_with_expiry(api_key_index=index)
            if error:
                raise RuntimeError("Could not authenticate the configured API key")
            return await read_restrictions(lighter.AccountApi(signer.api_client), account, index, token)

        with warnings.catch_warnings():
            warnings.simplefilter("error", GetPassWarning)
            wallet_key = getpass(f"L1 wallet private key for Core account {account}, API key {index}: ")
        await replace_restrictions(signer, index, wallet_key, policy)
        return {"account_index": account, "api_key_index": index, "code": 200}
    finally:
        await signer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("show", help="View the authenticated key policy")
    replace = actions.add_parser("replace", help="Replace all restrictions with a JSON policy")
    replace.add_argument("policy", type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args)), indent=2))
    except (GetPassWarning, EOFError):
        parser.exit(1, "Run replace in a terminal with a hidden password prompt.\n")
    except (ValueError, OSError, RuntimeError, KeyError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
