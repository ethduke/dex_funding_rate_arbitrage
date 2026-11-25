"""Account-owner operations on Lighter Core mainnet."""

from ipaddress import ip_address
import time

import lighter
from lighter.endpoint_profiles import MAINNET


def require_core_mainnet(url):
    if url.rstrip("/") != MAINNET.api_url:
        raise ValueError("API key restrictions require Lighter Core mainnet")


def validate_policy(policy):
    """Validate a complete replacement before signing it."""
    fields = {"market_ids", "series", "allowed_ips", "valid_until"}
    if not isinstance(policy, dict) or policy.keys() != fields:
        raise ValueError("Provide exactly market_ids, series, allowed_ips and valid_until")
    for field in ("market_ids", "series", "allowed_ips"):
        if not isinstance(policy[field], list) or len(policy[field]) > 16:
            raise ValueError(f"{field} must be a list with at most 16 entries")
    if any(type(mid) is not int or mid < 0 for mid in policy["market_ids"]):
        raise ValueError("Market IDs must be nonnegative integers")
    for entry in policy["series"]:
        if not isinstance(entry, dict) or entry.keys() != {"creator_account_index", "tag"}:
            raise ValueError("Each series needs creator_account_index and tag")
        if type(entry["creator_account_index"]) is not int or entry["creator_account_index"] < 0:
            raise ValueError("Series creator_account_index must be a nonnegative integer")
        if not isinstance(entry["tag"], str) or not 1 <= len(entry["tag"]) <= 100:
            raise ValueError("Series tags must contain 1 to 100 characters")
    for address in policy["allowed_ips"]:
        if not isinstance(address, str) or "%" in address:
            raise ValueError("Provide individual IPv4 or IPv6 addresses")
        ip_address(address)
    now = int(time.time())
    expiry = policy["valid_until"]
    if type(expiry) is not int or (expiry != 0 and not now < expiry <= now + 180 * 86400):
        raise ValueError("valid_until must be 0 or Unix seconds within the next 180 days")


async def read_restrictions(account_api, account_index, api_key_index, authorization):
    require_core_mainnet(account_api.api_client.configuration.host)
    if not authorization:
        raise ValueError("Account authentication is required to view restrictions")
    try:
        response = await account_api.apikeys(
            account_index=account_index, api_key_index=api_key_index, authorization=authorization,
        )
    except Exception:
        raise RuntimeError("API key lookup failed") from None
    if response.code != 200:
        raise RuntimeError(f"API key lookup failed with code {response.code}")
    for key in response.api_keys or []:
        if key.account_index == account_index and key.api_key_index == api_key_index:
            if key.restrictions is None:
                raise RuntimeError("Restrictions were omitted. Check authentication for this account.")
            policy = key.restrictions.to_dict()
            lighter.ApiKeyRestrictions.model_validate(policy)
            return policy
    raise RuntimeError("Configured API key was missing from the lookup")


async def replace_restrictions(signer, api_key_index, eth_private_key, policy):
    require_core_mainnet(signer.url)
    if signer.chain_id != MAINNET.chain_id or not 4 <= api_key_index <= 254:
        raise ValueError("Use a Core mainnet signer and an API key index from 4 to 254")
    validate_policy(policy)
    response, error = await signer.set_api_key_restrictions(
        eth_private_key=eth_private_key, api_key_index=api_key_index, **policy,
    )
    if error or response is None:
        # SDK errors can include signed request data.
        raise RuntimeError("Could not confirm the restriction update. Inspect the key with show.")
    if response.code != 200:
        raise RuntimeError(f"Restriction update failed with code {response.code}")
