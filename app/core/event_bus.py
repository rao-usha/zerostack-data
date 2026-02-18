"""
In-memory event bus for real-time SSE streaming.

Simple pub/sub using asyncio.Queue. Clients subscribe to channels
and receive SSE-formatted events. State is entirely in-memory;
clients reconnect on restart.
"""

import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Dict, Any, Set

logger = logging.getLogger(__name__)

# Keepalive interval (seconds)
KEEPALIVE_INTERVAL = 15


class _EventBus:
    """Singleton event bus for in-process pub/sub."""

    def __init__(self):
        # channel -> set of asyncio.Queue
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}

    def publish(self, channel: str, event_type: str, data: Dict[str, Any]) -> int:
        """
        Publish an event to all subscribers of a channel.

        Args:
            channel: Channel name (e.g. "collection_all", "collection_42")
            event_type: SSE event type (e.g. "progress", "started", "completed")
            data: Event payload

        Returns:
            Number of subscribers notified
        """
        subscribers = self._subscribers.get(channel, set())
        if not subscribers:
            return 0

        event = {
            "type": event_type,
            "data": data,
            "timestamp": time.time(),
        }

        dead_queues = set()
        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Slow consumer — drop the event
                dead_queues.add(queue)

        # Clean up dead queues
        for q in dead_queues:
            subscribers.discard(q)

        return len(subscribers) - len(dead_queues)

    async def subscribe_stream(
        self, channel: str, max_queue_size: int = 100
    ) -> AsyncGenerator[str, None]:
        """
        Async generator yielding SSE-formatted strings.

        Includes keepalive comments every KEEPALIVE_INTERVAL seconds.

        Args:
            channel: Channel to subscribe to
            max_queue_size: Max queued events before dropping

        Yields:
            SSE-formatted event strings
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)

        if channel not in self._subscribers:
            self._subscribers[channel] = set()
        self._subscribers[channel].add(queue)

        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        queue.get(), timeout=KEEPALIVE_INTERVAL
                    )
                    # Format as SSE
                    yield f"event: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive comment
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            self._subscribers.get(channel, set()).discard(queue)
            # Clean up empty channel sets
            if channel in self._subscribers and not self._subscribers[channel]:
                del self._subscribers[channel]

    @property
    def active_channels(self) -> Dict[str, int]:
        """Get active channels and subscriber counts."""
        return {
            channel: len(subs) for channel, subs in self._subscribers.items() if subs
        }


# Module-level singleton
EventBus = _EventBus()
