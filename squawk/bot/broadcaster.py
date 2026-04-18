"""Broadcaster protocol and TelegramBroadcaster implementation.

DigestActor receives a Broadcaster — it does not import Telegram directly.
TelegramBroadcaster is the only concrete implementation.
"""

from __future__ import annotations

import logging
from typing import Protocol

from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import Application

from squawk.digest import DigestOutput
from squawk.repositories.users import UserRepository

logger = logging.getLogger(__name__)

# Telegram caption limit is 1024 chars; message text limit is 4096 chars.
_CAPTION_LIMIT = 1024


class Broadcaster(Protocol):
    async def broadcast(
        self, digest: DigestOutput, chart_png: bytes | None = None
    ) -> None:
        """Send digest to all active users."""


class TelegramBroadcaster:
    """Sends a DigestOutput to all active Telegram users.

    Shares the PTB Application built in __main__.py with TelegramBot to avoid
    two Bot instances on the same token.
    """

    def __init__(self, app: Application, users: UserRepository) -> None:
        self._bot = app.bot
        self._users = users

    async def broadcast(
        self, digest: DigestOutput, chart_png: bytes | None = None
    ) -> None:
        """Send digest to all active users.

        Sends text, then optional aircraft photo, then optional traffic chart.
        Failures for individual users are logged and skipped so a single bad
        chat_id does not abort delivery to the remaining recipients.
        """
        chat_ids = await self._users.get_active()
        if not chat_ids:
            logger.info("broadcaster: no active users, skipping")
            return

        logger.info("broadcaster: sending digest to %d user(s)", len(chat_ids))

        for chat_id in chat_ids:
            try:
                await self._send_to(chat_id, digest, chart_png)
            except TelegramError as exc:
                logger.warning(
                    "broadcaster: failed to deliver to chat_id=%s: %s", chat_id, exc
                )

    async def _send_to(
        self,
        chat_id: int,
        digest: DigestOutput,
        chart_png: bytes | None = None,
    ) -> None:
        await self._bot.send_message(
            chat_id=chat_id,
            text=digest.text,
            parse_mode=ParseMode.HTML,
        )
        if digest.photo_url:
            caption = digest.photo_caption or ""
            await self._bot.send_photo(
                chat_id=chat_id,
                photo=digest.photo_url,
                caption=caption[:_CAPTION_LIMIT] if caption else None,
            )
        if chart_png:
            await self._bot.send_photo(
                chat_id=chat_id,
                photo=chart_png,
                caption="📈 Flugverkehr der Woche",
            )
