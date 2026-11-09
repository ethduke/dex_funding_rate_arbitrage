"""Per-connection Lighter stats snapshots; never mix deltas from two sockets."""

from copy import deepcopy
import json
import time


CHANNELS = ("market_stats", "spot_market_stats")


def market_selection(ids):
    if ids == "all":
        return "all"
    if not isinstance(ids, (list, tuple, set)):
        raise ValueError("Market IDs must be a collection or 'all'")
    if any(type(mid) is not int or not 0 <= mid < 2**63 for mid in ids):
        raise ValueError("Market IDs must be nonnegative int64 integers")
    result = sorted(set(ids))
    if len(result) > 500:
        raise ValueError("At most 500 markets per stats subscription")
    return result


class MarketStatsState:
    def __init__(self, selections, partial=True, multi_market=True):
        self.selections = {channel: market_selection(selections.get(channel, [])) for channel in CHANNELS}
        if type(partial) is not bool:
            raise ValueError("partial must be a boolean")
        self.multi_market = multi_market
        self.partial = partial and multi_market
        self.states = {channel: {} for channel in CHANNELS}
        self.received = {channel: {} for channel in CHANNELS}
        self.timestamps = {channel: {} for channel in CHANNELS}
        self.snapshots = set()

    def subscription(self, channel, selection):
        if not self.multi_market and selection != "all":
            return [{"type": "subscribe", "channel": f"{channel}/{mid}"} for mid in selection]
        message = {"type": "subscribe", "channel": channel}
        if self.multi_market:
            message["partial"] = self.partial
        if selection == "all":
            message["channel"] += "/all"
        else:
            message["market_ids"] = selection
        return [message]

    def unsubscription(self, channel, selection):
        if selection == "all":
            return [{"type": "unsubscribe", "channel": channel + "/all"}]
        if self.multi_market:
            return [{"type": "unsubscribe", "channel": channel, "market_ids": selection}]
        return [{"type": "unsubscribe", "channel": f"{channel}/{mid}"} for mid in selection]

    async def subscribe(self, ws):
        for channel, selection in self.selections.items():
            if selection:
                for message in self.subscription(channel, selection):
                    await ws.send(json.dumps(message))

    @property
    def ready(self):
        return all(
            (channel in self.snapshots if selection == "all"
             else all(str(mid) in self.states[channel] for mid in selection))
            for channel, selection in self.selections.items()
        )

    def change(self, channel, selection):
        """Update desired state synchronously; return wire messages to apply it."""
        selection = market_selection(selection)
        previous = self.selections[channel]
        if selection == previous:
            return []
        messages = []
        if previous == "all" or selection == "all":
            if previous:
                messages.extend(self.unsubscription(channel, previous))
            self.states[channel].clear()
            self.received[channel].clear()
            self.timestamps[channel].clear()
            self.snapshots.discard(channel)
            if selection:
                messages.extend(self.subscription(channel, selection))
        else:
            removed = sorted(set(previous) - set(selection))
            added = sorted(set(selection) - set(previous))
            if removed:
                messages.extend(self.unsubscription(channel, removed))
                for mid in removed:
                    for cache in (self.states, self.received, self.timestamps):
                        cache[channel].pop(str(mid), None)
            if added:
                messages.extend(self.subscription(channel, added))
        self.selections[channel] = selection
        return messages

    def consume(self, message):
        kind, _, channel = message.get("type", "").partition("/")
        if channel not in CHANNELS or kind not in ("subscribed", "update"):
            return None
        selection = self.selections[channel]
        single_market = None
        if not self.multi_market and selection != "all":
            prefix, _, suffix = message.get("channel", "").partition(":")
            if prefix != channel or not suffix.isascii() or not suffix.isdecimal():
                return []
            single_market = int(suffix)
            expected_channel = f"{channel}:{single_market}"
        else:
            expected_channel = channel + ":all" if selection == "all" else channel
        if not selection or message.get("channel") != expected_channel:
            return []
        rows = message.get(channel)
        if not isinstance(rows, dict):
            raise ValueError("Invalid market stats payload")
        if single_market is not None:
            rows = {str(single_market): rows}
        timestamp = message.get("timestamp")
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("Invalid market stats timestamp")
        changed = []
        if kind == "subscribed" and single_market is None:
            self.states[channel].clear()
            self.received[channel].clear()
            self.timestamps[channel].clear()
            self.snapshots.add(channel)
        for key, row in rows.items():
            if not isinstance(row, dict):
                raise ValueError("Invalid market stats row")
            mid = row.get("market_id")
            if type(mid) is not int or not 0 <= mid < 2**63 or str(mid) != key:
                raise ValueError("Invalid market stats ID")
            if selection != "all" and mid not in selection:
                continue
            if timestamp < self.timestamps[channel].get(key, -1):
                continue
            previous = self.states[channel].get(key)
            # New IDs arrive with a full snapshot, even in an update message.
            if previous is None and not isinstance(row.get("symbol"), str):
                continue
            state = {**(previous or {}), **row} if self.partial else dict(row)
            self.states[channel][key] = state
            self.received[channel][key] = time.monotonic()
            self.timestamps[channel][key] = timestamp
            changed.append((channel, deepcopy(state)))
        return changed

    def get(self, channel, market_id, max_age):
        key = str(market_id)
        if time.monotonic() - self.received[channel].get(key, 0) > max_age:
            return {}
        return deepcopy(self.states[channel].get(key, {}))
