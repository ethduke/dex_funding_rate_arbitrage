import asyncio
import inspect
import json
import math
import time
from typing import Callable, List, Dict, Optional
from urllib.parse import urlsplit, urlunsplit

from lighter import WsClient
from lighter.endpoint_profiles import MAINNET
from websockets.client import connect as _ws_connect_async
from utils.logger import setup_logger
from model.exchanges.lighter_stats import MarketStatsState, market_selection

logger = setup_logger(__name__)


class _PatchedWsClient(WsClient):
    """SDK protocol handling with snapshot readiness and owned socket cleanup."""

    def __init__(self, stats_selections=None, stats_partial=True, stats_multi_market=True, on_stats=None, **kwargs):
        super().__init__(**kwargs)
        self.stats = MarketStatsState(stats_selections or {}, partial=stats_partial, multi_market=stats_multi_market)
        self.on_stats = on_stats
        self.protocol_connected = False
        self.ready = asyncio.Event()
        self.shutdown = asyncio.Event()
        self.on_shutdown = lambda close_in_ms: self.shutdown.set()

    async def on_message_async(self, ws, message):
        decoded = json.loads(message)
        changes = self.stats.consume(decoded)
        if changes is None:
            await super().on_message_async(ws, message)
        elif self.on_stats:
            for channel, state in changes:
                await self.on_stats(self, channel, state)
        if decoded.get("type") == "connected":
            self.protocol_connected = True
            await self.stats.subscribe(ws)
        self.refresh_ready()

    def refresh_ready(self):
        books = {str(mid) for mid in self.subscriptions["order_books"]}
        accounts = {str(aid) for aid in self.subscriptions["accounts"]}
        if books.issubset(self.order_book_states) and accounts.issubset(self.account_states) and self.stats.ready:
            self.ready.set()
        else:
            self.ready.clear()

    async def run_async(self):
        try:
            # Lighter uses application-level ping/pong, handled by the SDK.
            self.ws = await _ws_connect_async(
                self.base_url, ping_interval=None, ping_timeout=None,
                open_timeout=10, close_timeout=2,
            )
            async for message in self.ws:
                await self.on_message_async(self.ws, message)
        finally:
            self.ready.clear()
            if self.ws is not None:
                await self.ws.close()


class LighterWebSocketClient:
    CONNECT_TIMEOUT = 20
    SNAPSHOT_TIMEOUT = 12
    MAX_PRICE_AGE = 30

    def __init__(self, account_ids: List[int] = None,
                 on_account_update: Callable = None,
                 on_order_book_update: Callable = None,
                 market_mapping: Dict[int, str] = None,
                 order_book_ids: List[int] = None,
                 api_url: str = MAINNET.api_url,
                 exchange_name: str = "Lighter",
                 market_stats_ids=None, spot_market_stats_ids=None,
                 stats_partial: bool = True, on_market_stats_update: Callable = None,
                 on_spot_market_stats_update: Callable = None, read_only: bool = False,
                 multi_market_stats: Optional[bool] = None):
        self.exchange_name = exchange_name
        endpoint = urlsplit(api_url)
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
            raise ValueError("Lighter API URL must be an HTTP(S) URL")
        if endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
            raise ValueError("Lighter API URL must not contain credentials, query, or fragment")
        self.ws_url = urlunsplit((
            "wss" if endpoint.scheme == "https" else "ws", endpoint.netloc,
            endpoint.path.rstrip("/") + "/stream", "", "",
        ))
        self.account_ids = account_ids if account_ids is not None else [1]
        if type(read_only) is not bool or (read_only and self.account_ids):
            raise ValueError("Read-only WebSockets require account_ids=[]")
        if read_only:
            self.ws_url += "?readonly=true"
        self.order_book_ids = order_book_ids if order_book_ids is not None else []
        self._market_stats = {}
        self._market_mapping = market_mapping or {}
        self.on_account_update = on_account_update or self._default_account_handler
        self.on_order_book_update = on_order_book_update or self._default_order_book_handler
        self.ws_client = None
        self.ws_task = None
        self._connected = False
        self._ready = asyncio.Event()
        self._sessions = {}
        self._account_transaction_times = {}
        self._stats_selections = {
            "market_stats": market_selection(market_stats_ids if market_stats_ids is not None else []),
            "spot_market_stats": market_selection(spot_market_stats_ids if spot_market_stats_ids is not None else []),
        }
        if type(stats_partial) is not bool:
            raise ValueError("stats_partial must be a boolean")
        self.stats_partial = stats_partial
        if multi_market_stats is not None and type(multi_market_stats) is not bool:
            raise ValueError("multi_market_stats must be a boolean or None")
        self.multi_market_stats = (
            endpoint.hostname in {"mainnet.zklighter.elliot.ai", "testnet.zklighter.elliot.ai"}
            if multi_market_stats is None else multi_market_stats
        )
        self.on_market_stats_update = on_market_stats_update
        self.on_spot_market_stats_update = on_spot_market_stats_update
        self._stats_lock = asyncio.Lock()

    async def connect(self):
        """Wait for fresh subscription snapshots, not merely a scheduled task."""
        if self.is_connected():
            return True
        if not self.account_ids and not self.order_book_ids:
            raise ValueError("No Lighter subscriptions configured")
        self._ready.clear()
        if self.ws_task is None or self.ws_task.done():
            self.ws_task = asyncio.create_task(self._run_websocket())
        try:
            await asyncio.wait_for(self._ready.wait(), self.CONNECT_TIMEOUT)
            return self.is_connected()
        except (Exception, asyncio.CancelledError):
            await self.disconnect()
            raise

    def _new_session(self):
        # Each SDK client owns an independent snapshot/delta book. Only the
        # active client publishes callbacks, so overlapping streams never mix.
        client = _PatchedWsClient(
            ws_url=self.ws_url, account_ids=list(self.account_ids),
            order_book_ids=list(self.order_book_ids),
            on_account_update=lambda aid, data: self._handle_account_update(aid, data)
            if self.ws_client is client else None,
            on_order_book_update=lambda mid, data: self._handle_order_book_update(mid, data)
            if self.ws_client is client else None,
            on_unhandled_message=self._handle_unhandled_message,
            stats_selections=self._stats_selections, stats_partial=self.stats_partial,
            stats_multi_market=self.multi_market_stats,
            on_stats=self._publish_stats,
        )
        self._sessions[client] = asyncio.create_task(client.run_async())
        return client

    async def _wait_session(self, client, ready=False):
        waiters = [asyncio.create_task(client.shutdown.wait())]
        if ready:
            waiters.append(asyncio.create_task(client.ready.wait()))
        task = self._sessions[client]
        try:
            done, _ = await asyncio.wait(
                [task, *waiters], return_when=asyncio.FIRST_COMPLETED,
                timeout=self.SNAPSHOT_TIMEOUT if ready else None,
            )
            if task in done:
                await task
                raise ConnectionError("Lighter WebSocket closed")
            if ready and (not done or client.shutdown.is_set() or not client.ready.is_set()):
                raise ConnectionError("Lighter replacement did not provide fresh snapshots")
        finally:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def _stop_session(self, client):
        task = self._sessions.pop(client, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _activate(self, client):
        self.ws_client = client
        self._market_stats.clear()
        self._connected = True
        for mid, book in client.order_book_states.items():
            self._handle_order_book_update(mid, book)
        for aid, account in client.account_states.items():
            self._handle_account_update(aid, account)
        for channel, states in client.stats.states.items():
            for state in states.values():
                await self._publish_stats(client, channel, dict(state))
        self._ready.set()

    async def _run_websocket(self):
        """Warm a replacement on shutdown; retry unexpected closes with backoff."""
        delay = 1
        try:
            while True:
                candidate = self._new_session()
                try:
                    await self._wait_session(candidate, ready=True)
                    if self._sessions[candidate].done() or candidate.ws.closed or candidate.shutdown.is_set():
                        raise ConnectionError("Lighter replacement closed before activation")
                    old = self.ws_client
                    await self._activate(candidate)
                    if old is not None:
                        await self._stop_session(old)
                    logger.info("%s WebSocket subscriptions ready", self.exchange_name)
                    delay = 1
                    await self._wait_session(candidate)
                    logger.info("%s server draining; warming replacement WebSocket", self.exchange_name)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._stop_session(candidate)
                    if not self.is_connected():
                        self._connected = False
                        self._ready.clear()
                        self._market_stats.clear()
                    logger.warning("%s WebSocket retry in %ss (%s)", self.exchange_name, delay, type(exc).__name__)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)
        finally:
            self._connected = False
            self._ready.clear()
            self._market_stats.clear()
            for client in list(self._sessions):
                await self._stop_session(client)
            self.ws_client = None

    async def disconnect(self):
        """Stop reconnects and close both active and warming connections."""
        if self.ws_task is not None:
            self.ws_task.cancel()
            await asyncio.gather(self.ws_task, return_exceptions=True)
            self.ws_task = None
        self._connected = False
        self._ready.clear()
        self._market_stats.clear()
        self._account_transaction_times.clear()

    async def close(self):
        await self.disconnect()

    def is_connected(self) -> bool:
        task = self._sessions.get(self.ws_client)
        return bool(
            self._connected and task is not None and not task.done()
            and self.ws_client.ws is not None and not self.ws_client.ws.closed
        )

    async def resubscribe_order_books(self, market_ids: List[int]) -> bool:
        new_ids = sorted(set(market_ids))
        if new_ids == sorted(set(self.order_book_ids)) and self.is_connected():
            return True
        await self.disconnect()
        self.order_book_ids = new_ids
        try:
            return await self.connect()
        except Exception as exc:
            logger.warning("%s resubscribe failed (%s)", self.exchange_name, type(exc).__name__)
            return False

    def set_market_mapping(self, market_mapping: Dict[int, str]):
        self._market_mapping = market_mapping

    async def set_market_stats_markets(self, market_ids, *, spot=False):
        """Replace the selected set (or 'all'), adding/removing IDs without reconnecting."""
        channel = "spot_market_stats" if spot else "market_stats"
        selection = market_selection(market_ids)
        async with self._stats_lock:
            await self._set_stats_markets(channel, selection)

    async def _set_stats_markets(self, channel, selection):
        self._stats_selections[channel] = selection
        updates = []
        for client in list(self._sessions):
            messages = client.stats.change(channel, selection)
            client.refresh_ready()
            updates.append((client, messages))
        for client, messages in updates:
            if client.protocol_connected and client.ws is not None and not client.ws.closed:
                try:
                    for message in messages:
                        await client.ws.send(json.dumps(message))
                except Exception:
                    # Neither the active nor warming socket may retain a half-applied set.
                    await asyncio.gather(*(
                        session.ws.close() for session in list(self._sessions)
                        if session.ws is not None
                    ), return_exceptions=True)
                    raise ConnectionError("Lighter stats subscription update failed") from None

    async def subscribe_market_stats(self, market_ids, *, spot=False):
        channel = "spot_market_stats" if spot else "market_stats"
        ids = market_selection(market_ids)
        async with self._stats_lock:
            current = self._stats_selections[channel]
            selection = "all" if ids == "all" or current == "all" else sorted(set(current) | set(ids))
            await self._set_stats_markets(channel, market_selection(selection))

    async def unsubscribe_market_stats(self, market_ids=None, *, spot=False):
        channel = "spot_market_stats" if spot else "market_stats"
        if market_ids is None:
            await self.set_market_stats_markets([], spot=spot)
            return
        ids = market_selection(market_ids)
        async with self._stats_lock:
            current = self._stats_selections[channel]
            if current == "all" or ids == "all":
                raise ValueError("Unsubscribe the entire /all stream before selecting individual markets")
            await self._set_stats_markets(channel, sorted(set(current) - set(ids)))

    def get_market_snapshot(self, market_id, *, spot=False):
        """Stats are separate from the order-book prices used for execution."""
        if not self.is_connected():
            return {}
        channel = "spot_market_stats" if spot else "market_stats"
        return self.ws_client.stats.get(channel, market_id, self.MAX_PRICE_AGE)

    async def _publish_stats(self, client, channel, state):
        if client is not self.ws_client:
            return
        callback = self.on_spot_market_stats_update if channel == "spot_market_stats" else self.on_market_stats_update
        if callback:
            try:
                result = callback(state)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                logger.warning("%s stats callback failed (%s)", self.exchange_name, type(exc).__name__)

    def get_market_stats(self, market_id: int) -> Dict:
        stats = self._market_stats.get(str(market_id), {})
        if not self.is_connected() or time.monotonic() - stats.get("received_at", 0) > self.MAX_PRICE_AGE:
            return {}
        return stats

    def get_latest_price(self, market_id: int) -> Optional[float]:
        return self.get_market_stats(market_id).get("mid_price")

    def _handle_account_update(self, account_id: int, account: Dict):
        transaction_time = account.get("transaction_time")
        timestamped = type(transaction_time) is int and transaction_time >= 0
        key = str(account_id)
        # Equal timestamps may carry distinct updates from the same transaction.
        if timestamped and transaction_time < self._account_transaction_times.get(key, -1):
            return
        try:
            self.on_account_update(account_id, account)
            if timestamped:
                self._account_transaction_times[key] = transaction_time
        except Exception as exc:
            logger.warning("Lighter account callback failed (%s)", type(exc).__name__)

    def _handle_order_book_update(self, order_book_id: int, order_book):
        key = str(order_book_id)
        try:
            bids = order_book.get("bids", []) if isinstance(order_book, dict) else order_book.bids
            asks = order_book.get("asks", []) if isinstance(order_book, dict) else order_book.asks
            # SDK deltas append new price levels; the lists need not be sorted.
            bid_prices = [p for row in bids if (p := self._extract_price(row)) is not None]
            ask_prices = [p for row in asks if (p := self._extract_price(row)) is not None]
            if bid_prices and ask_prices and max(bid_prices) <= min(ask_prices):
                self._market_stats[key] = {
                    "mid_price": (max(bid_prices) + min(ask_prices)) / 2,
                    "received_at": time.monotonic(),
                }
            else:
                self._market_stats.pop(key, None)
            self.on_order_book_update(order_book_id, order_book)
        except Exception as exc:
            self._market_stats.pop(key, None)
            logger.warning("Lighter order-book callback failed (%s)", type(exc).__name__)

    def _extract_price(self, row):
        try:
            if isinstance(row, dict):
                value = next((row[k] for k in ("price", "p", "px") if k in row), None)
            elif isinstance(row, list):
                value = row[0] if row else None
            else:
                value = getattr(row, "price", row)
            price = float(value)
            return price if math.isfinite(price) and price > 0 else None
        except (ValueError, TypeError):
            return None

    def _default_account_handler(self, account_id: int, account: Dict):
        logger.debug("Lighter account update received")

    def _default_order_book_handler(self, order_book_id: int, order_book: Dict):
        logger.debug("Lighter order-book update for market %s", order_book_id)

    def _handle_unhandled_message(self, message):
        # Ping/pong and shutdown are handled by the SDK, on the originating socket.
        logger.debug("Unhandled Lighter WebSocket message type: %s", message.get("type"))

    async def subscribe_to_market_updates(self, market_ids: List[int], callback: Callable):
        self.on_order_book_update = callback
        return await self.resubscribe_order_books(market_ids)
