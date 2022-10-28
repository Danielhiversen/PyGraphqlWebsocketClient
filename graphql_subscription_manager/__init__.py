"""Subscription manager for Graph QL websocket."""

import asyncio
import json
import logging
import sys
from time import time

import pkg_resources
import websockets

_LOGGER = logging.getLogger(__name__)

STATE_STARTING = "starting"
STATE_RUNNING = "running"
STATE_STOPPED = "stopped"

try:
    VERSION = pkg_resources.require("graphql-subscription-manager")[0].version
except Exception:  # pylint: disable=broad-except
    VERSION = "dev"


class SubscriptionManager:
    """Subscription manager."""

    # pylint: disable=too-many-instance-attributes

    def __init__(self, init_payload, url, user_agent=None):
        """Create resources for websocket communication."""
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()
        self.subscriptions = {}
        self._url = url
        self._state = None
        self.websocket = None
        self._retry_timer = None
        self._client_task = None
        self._session_id = 0
        self._init_payload = init_payload
        if user_agent is not None:
            self._user_agent = user_agent
        else:
            _ver = sys.version_info
            self._user_agent = f"Python/{_ver[0]}.{_ver[1]}"
        self._user_agent += f" graphql-subscription-manager/{VERSION}"

    def start(self):
        """Start websocket."""
        _LOGGER.debug("Start state %s.", self._state)
        if self.is_running:
            return
        self._state = STATE_STARTING
        self._cancel_client_task()
        self._client_task = self.loop.create_task(self.running())
        for subscription in self.subscriptions.copy():
            callback, sub_query = self.subscriptions.pop(subscription, (None, None))
            _LOGGER.debug("Removed, %s", subscription)
            if callback is None:
                continue
            _LOGGER.debug("Add subscription %s", callback)
            self.loop.create_task(self.subscribe(sub_query, callback))

    @property
    def is_running(self):
        """Return if client is running or not."""
        return self._state == STATE_RUNNING

    async def running(self):
        """Start websocket connection."""
        _LOGGER.debug("Starting")

        try:
            await self._init_web_socket()
        except Exception:  # pylint: disable=broad-except
            _LOGGER.debug("Failed to connect. Reconnecting... ", exc_info=True)
            await self.retry()
            return

        k = 0
        while self._state in (
            STATE_RUNNING,
            STATE_STARTING,
        ):
            try:
                msg = await asyncio.wait_for(self.websocket.recv(), timeout=60)
            except asyncio.TimeoutError:
                k += 1
                if k > 10:
                    _LOGGER.debug("No data, reconnecting.")
                    await self.retry()
                    return
                _LOGGER.debug("No websocket data, sending a ping.")
                await asyncio.wait_for(await self.websocket.ping(), timeout=20)
            except Exception:  # pylint: disable=broad-except
                if self._state == STATE_RUNNING:
                    await self.retry()
            else:
                k = 0
                self._process_msg(msg)

    async def stop(self):
        """Close websocket connection."""
        _LOGGER.debug("Stopping client.")
        self._cancel_retry_timer()

        start_time = time()

        for subscription_id in self.subscriptions.copy():
            _LOGGER.debug("Sending unsubscribe: %s", subscription_id)
            await self.unsubscribe(subscription_id)

        while (
            self.websocket is not None
            and self.subscriptions
            and (time() - start_time) < 5.0
        ):
            await asyncio.sleep(0.1)
        self._state = STATE_STOPPED
        await self._close_websocket()

        self._cancel_client_task()
        _LOGGER.debug("Server connection is stopped")

    async def retry(self):
        """Retry to connect to websocket."""
        _LOGGER.debug("Retry, state: %s", self._state)
        self._cancel_retry_timer()
        self._state = STATE_STARTING
        _LOGGER.debug("Close websocket")
        await self._close_websocket()
        _LOGGER.debug("Restart")
        self._retry_timer = self.loop.call_soon(self.start)
        _LOGGER.debug("Reconnecting to server.")

    async def subscribe(self, sub_query, callback, timeout=3):
        """Add a new subscription."""
        current_session_id = str(self._session_id)
        self._session_id += 1
        subscription = {
            "payload": {"query": sub_query},
            "type": "subscribe",
            "id": current_session_id,
        }

        json_subscription = json.dumps(subscription)
        self.subscriptions[current_session_id] = (callback, sub_query)

        start_time = time()
        while time() - start_time < timeout:
            if self.websocket is None or not self.websocket.open or not self.is_running:
                await asyncio.sleep(0.1)
                continue

            await self.websocket.send(json_subscription)
            _LOGGER.debug("New subscription %s", current_session_id)
            return current_session_id
        return None

    async def unsubscribe(self, subscription_id):
        """Unsubscribe."""
        if self.websocket is None or not self.websocket.open:
            _LOGGER.warning("Websocket is closed.")
            return
        await self.websocket.send(
            json.dumps({"id": str(subscription_id), "type": "complete"})
        )

    async def _close_websocket(self):
        if self.websocket is None:
            return
        try:
            await self.websocket.close()
        finally:
            self.websocket = None

    def _process_msg(self, msg):
        """Process received msg."""
        result = json.loads(msg)

        if (msg_type := result.get("type", "")) == "connection_ack":
            _LOGGER.debug("Running")
            self._state = STATE_RUNNING
            return

        if (subscription_id := result.get("id")) is None:
            return

        if msg_type == "complete":
            _LOGGER.debug("Unsubscribe %s successfully.", subscription_id)
            if self.subscriptions and subscription_id in self.subscriptions:
                self.subscriptions.pop(subscription_id)
            return

        if (data := result.get("payload")) is None:
            return

        if subscription_id not in self.subscriptions:
            _LOGGER.warning("Unknown id %s.", subscription_id)
            return
        _LOGGER.debug("Received data %s", data)
        self.subscriptions[subscription_id][0](data)

    def _cancel_retry_timer(self):
        if self._retry_timer is None:
            return
        try:
            self._retry_timer.cancel()
        finally:
            self._retry_timer = None

    def _cancel_client_task(self):
        if self._client_task is None:
            return
        try:
            self._client_task.cancel()
        finally:
            self._client_task = None

    async def _init_web_socket(self):
        self.websocket = await asyncio.wait_for(
            # pylint: disable=no-member
            websockets.connect(
                self._url,
                subprotocols=["graphql-transport-ws"],
                extra_headers={"User-Agent": self._user_agent},
            ),
            timeout=10,
        )
        await self.websocket.send(
            json.dumps(
                {
                    "type": "connection_init",
                    "payload": self._init_payload,
                }
            )
        )
