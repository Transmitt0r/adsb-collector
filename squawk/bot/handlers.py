"""Telegram command handlers: /start, /stop, /debug.

These are registered on the PTB Application by TelegramBot. Each handler
receives the bot's dependencies via closure — no global state.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Coroutine

from telegram import Update
from telegram.ext import ContextTypes

from squawk.repositories.users import UserRepository

logger = logging.getLogger(__name__)


def make_handlers(
    users: UserRepository,
    on_debug_digest: Callable[[], Coroutine],
    admin_chat_id: int,
) -> dict[str, any]:
    """Return a dict of {command: handler_coroutine} for registration on PTB."""

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat is None:
            return
        chat_id = update.effective_chat.id
        username = update.effective_user.username if update.effective_user else None
        newly_registered = await users.register(chat_id, username)
        if newly_registered:
            await update.message.reply_text(
                "Du wirst ab jetzt über neue Flugzeuge benachrichtigt. ✈️"
            )
        else:
            await update.message.reply_text("Du bist bereits registriert.")

    async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat is None:
            return
        chat_id = update.effective_chat.id
        was_active = await users.unregister(chat_id)
        if was_active:
            await update.message.reply_text(
                "Du wirst ab jetzt nicht mehr benachrichtigt."
            )
        else:
            await update.message.reply_text("Du warst nicht registriert.")

    async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat is None:
            return
        chat_id = update.effective_chat.id
        if chat_id != admin_chat_id:
            await update.message.reply_text("Nicht autorisiert.")
            return
        asyncio.create_task(on_debug_digest())
        await update.message.reply_text("Digest wird generiert…")

    return {"start": start, "stop": stop, "debug": debug}
