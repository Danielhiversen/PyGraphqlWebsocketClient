"""Subscription manager for Graph QL websocket."""

import asyncio
import json
import logging
import sys
from time import time
from uuid import uuid4

import pkg_resources
import websockets

_LOGGER = logging.getLogger(__name__)

STATE_STARTING = "starting"
STATE_RUNNING = "running"
STATE_STOPPED = "stopped"

SUB_STATE_CREATED = "created"
SUB_STATE_INIT = "initializing"
SUB_STATE_RUNNING = "subscription_running"
SUB_STATE_FAILED = "failed"

try:
    VERSION = pkg_resources.require("graphql-subscription-manager")[0].version
except Exception:  # pylint: disable=broad-except
    VERSION = "dev"


class Subscription:
    """Holds data for a single subscription"""

    def __init__(self, callback, sub_query):
        self._id = uuid4().hex
        self._callback = callback
        self._sub_query = sub_query
        self.state = SUB_STATE_CREATED

    @property
    def sub_id(self):
        """The subscription ID over the websocket"""
        return self._id

    @property
    def callback(self):
        """Callback function provided by the subscriber"""
        return self._callback

    @property
    def sub_query(self):
        """Subscription query provided by the subscriber"""
        return self._sub_query


class SubscriptionManager:
    """Subscription manager."""

    # pylint: disable=too-many-instance-attributes

    def __init__(self, init_payload, url, user_agent=None):
        """Create resources for websocket communication."""
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()
        self.subscriptions: dict[str, Subscription] = {}
        self.subs_waiting: list[str] = []
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

    async def _ensure_subscriptions(self, timeout=30):
        _LOGGER.debug("Handle deferred subscriptions. state: %s", self._state)

        if not self.subs_waiting:
            _LOGGER.debug("No deferred subscriptions found.")
            return

        start_time = time()
        while not self.is_running:
            if time() - start_time <= timeout:
                await asyncio.sleep(1)
                continue
            _LOGGER.error(
                "Subscriptions cannot start. No websocket connection available"
            )
            # something else to do? raise? remove pending subscriptions?
            return

        while self.subs_waiting:
            for sub_id in self.subs_waiting:
                sub = self.subscriptions[sub_id]
                _LOGGER.info(
                    "Handle deferred subscription %s, sub_state %s",
                    sub_id,
                    sub.state
                )
                if sub.state in [SUB_STATE_CREATED, SUB_STATE_FAILED]:
                    if await self._subscribe_remote(sub_id):
                        self.subs_waiting.remove(sub_id)
                    continue

                # running subscription in waiting state, need to re-trigger
                old_sub = self.subscriptions.pop(sub_id)
                self.subs_waiting.remove(sub_id)

                _LOGGER.debug("Removed, %s", sub_id)
                if old_sub.callback is None:
                    continue
                _LOGGER.debug("Add subscription %s", old_sub.callback)
                await self.subscribe(old_sub.sub_query, old_sub.callback)

    def start(self):
        """Start websocket."""
        _LOGGER.debug("Websocket start state %s.", self._state)
        if self.is_running:
            return
        self._state = STATE_STARTING
        self._cancel_client_task()
        self._client_task = self.loop.create_task(self.running())
        self.loop.create_task(self._ensure_subscriptions())

    @property
    def is_running(self):
        """Return if client is running or not."""
        return self._state == STATE_RUNNING

    async def running(self):
        """Start websocket connection."""
        _LOGGER.debug("Websocket client start")

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
                if not self.websocket:
                    _LOGGER.debug(
                        "Websocket connection lost. State: %s",
                        self._state
                    )
                    return

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
        _LOGGER.debug("Invalidate subscriptions")
        self._retrigger_subscriptions()
        _LOGGER.debug("Restart")
        self._retry_timer = self.loop.call_soon(self.start)
        _LOGGER.debug("Reconnecting to server.")

    def _retrigger_subscriptions(self):
        for sub in self.subscriptions:
            self.subs_waiting.append(sub)

    async def _subscribe_remote(self, session_id, timeout=3):
        sub = self.subscriptions[session_id]

        if sub.state == SUB_STATE_RUNNING:
            _LOGGER.debug(
                "Subscription %s already running, no action",
                session_id
            )
            return True

        sub.state = SUB_STATE_INIT
        subscription = {
            "payload": {"query": sub.sub_query},
            "type": "subscribe",
            "id": session_id,
        }

        retries_left = 5
        json_subscription = json.dumps(subscription)
        while retries_left:
            start_time = time()
            while time() - start_time < timeout:
                if (self.websocket is None
                        or not self.websocket.open
                        or not self.is_running):
                    await asyncio.sleep(0.1)
                    continue

                await self.websocket.send(json_subscription)
                _LOGGER.debug(
                    "Subscription started, subscription ID: %s",
                    session_id
                )
                sub.state = SUB_STATE_RUNNING
                return True

            # timed out
            retries_left -= 1

        sub.state = SUB_STATE_CREATED
        return False

    async def subscribe(self, sub_query, callback):
        """Request a new subscription."""
        new_sub = Subscription(callback, sub_query)
        self.subscriptions[new_sub.sub_id] = new_sub
        _LOGGER.debug(
            "Subscription request accepted, subscription ID: %s",
            new_sub.sub_id
        )

        # if the socket is running and subscription could be created
        #   there is no need to put the subscription to wait
        if self.is_running and await self._subscribe_remote(new_sub.sub_id):
            return new_sub.sub_id

        _LOGGER.debug(
            "Remote subscription deferred, state: %s, subscription ID: %s",
            self._state,
            new_sub.sub_id
        )
        self.subs_waiting.append(new_sub.sub_id)
        return new_sub.sub_id

    async def unsubscribe(self, subscription_id):
        """Unsubscribe."""
        if self.websocket is None or not self.websocket.open:
            _LOGGER.warning("Websocket is closed.")
            return
        await self.websocket.send(
            json.dumps({"id": str(subscription_id), "type": "complete"})
        )
        self._session_id = None

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
        self.subscriptions[subscription_id].callback(data)

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

        # without wait the websocket is detected open,
        # it takes some time to receive the CLOSE event so it gets closed
        await asyncio.sleep(0.5)
        if self.websocket is None or not self.websocket.open:
            raise Exception("Failed to initiate websocket connection")
