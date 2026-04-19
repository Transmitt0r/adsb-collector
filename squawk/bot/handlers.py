"""Telegram command handlers: /debug.

These are registered on the PTB Application by TelegramBot. Each handler
receives the bot's dependencies via closure — no global state.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


def make_handlers(
    on_debug_digest: Callable[[int], Coroutine],
    admin_chat_id: int,
) -> dict[str, Any]:
    """Return a dict of {command: handler_coroutine} for registration on PTB."""

    async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat is None:
            return
        chat_id = update.effective_chat.id
        if chat_id != admin_chat_id:
            if update.message:
                await update.message.reply_text("Nicht autorisiert.")
            return
        logger.info("bot: /debug triggered by chat_id=%d", chat_id)
        asyncio.create_task(on_debug_digest(chat_id))
        if update.message:
            await update.message.reply_text("Digest wird generiert…")

    return {"debug": debug}
