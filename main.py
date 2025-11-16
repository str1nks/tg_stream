#!/usr/bin/env python3
# main.py - полностью рабочий для Aiogram 3.22 + поток YouTube → RTMP
import asyncio
import os
import signal
import subprocess
import sys
import time
from yt_dlp import YoutubeDL
from dataclasses import dataclass, field
from typing import List, Optional

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# ================= CONFIG =================
TG_BOT_TOKEN = "8396785240:AAG_Ys7UP7C1iwZQPeLW8cc0j__xz8GeCaM"
OWNER_ID = 5135680104
RTMP_URL = "rtmps://dc4-1.rtmp.t.me/s/"
STREAM_KEY = "3114622344:MN4WNnEPwg7OPDCHqzw9Nw"
COOKIES_FILE = "cookies.txt"
FULL_RTMP = RTMP_URL.rstrip("/") + "/" + STREAM_KEY
PLACEHOLDER_URL = "https://www.youtube.com/watch?v=G-kF940PFE4"
FFMPEG_CMD = "ffmpeg"
# ==========================================

bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher()


@dataclass
class VideoItem:
    url: str
    title: Optional[str] = None
    added_by: Optional[int] = None


@dataclass
class StreamState:
    process: Optional[subprocess.Popen] = None
    current: Optional[VideoItem] = None
    queue: List[VideoItem] = field(default_factory=list)
    playing_queue: bool = False
    is_paused: bool = False
    placeholder_pos: float = 0.0
    placeholder_last_start: Optional[float] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


state = StreamState()

# ----------------- Utils -----------------
async def owner_only(msg: Message) -> bool:
    return msg.from_user and msg.from_user.id == OWNER_ID


def get_video_stream_url(youtube_url: str) -> str:
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "best",
        "noplaylist": True,
        "cookiefile": COOKIES_FILE,
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(youtube_url, download=False)
    if "url" in info:
        return info["url"]
    if "formats" in info and info["formats"]:
        for f in reversed(info["formats"]):
            if f.get("url"):
                return f["url"]
    raise RuntimeError("Не удалось получить прямой потоковый URL")


def spawn_ffmpeg(input_url: str, extra_args: Optional[List[str]] = None) -> subprocess.Popen:
    args = [
        FFMPEG_CMD,
        "-re",
        "-i", input_url,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-b:v", "2500k",
        "-maxrate", "2500k",
        "-bufsize", "5000k",
        "-vf", "scale=-2:720",
        "-c:a", "aac",
        "-b:a", "160k",
        "-f", "flv",
        FULL_RTMP
    ]
    if extra_args:
        args = args[:-1] + extra_args + [args[-1]]
    # Отправляем stdout/stderr в /dev/null, чтобы ffmpeg не блокировал процесс
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid)
    return proc


async def stop_process(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=10)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                proc.kill()


def pause_process_posix(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGSTOP)


def resume_process_posix(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGCONT)


async def safe_task(coro):
    try:
        await coro
    except Exception as e:
        try:
            await bot.send_message(OWNER_ID, f"Ошибка таска: {e}")
        except Exception:
            pass


# ------------- Core streaming -------------
async def play_item(item: VideoItem):
    async with state.lock:
        state.current = item
        state.is_paused = False

    try:
        direct = get_video_stream_url(item.url)
        await bot.send_message(OWNER_ID, f"Поток получен: {direct[:50]}...")
    except Exception as e:
        await bot.send_message(OWNER_ID, f"Ошибка получения потока для {item.url}: {e}")
        return

    extra_ff_args = None
    if item.url == PLACEHOLDER_URL:
        ss = int(state.placeholder_pos)
        if ss > 0:
            extra_ff_args = ["-ss", str(ss)]

    proc = spawn_ffmpeg(direct, extra_args=extra_ff_args)
    async with state.lock:
        state.process = proc
        if item.url == PLACEHOLDER_URL:
            state.placeholder_last_start = time.time()

    try:
        await asyncio.to_thread(proc.wait)
    finally:
        async with state.lock:
            if item.url == PLACEHOLDER_URL and state.placeholder_last_start:
                played = time.time() - state.placeholder_last_start
                state.placeholder_pos += played
                state.placeholder_last_start = None
            state.process = None
            state.current = None

    async with state.lock:
        if state.playing_queue and state.queue:
            return
        if state.playing_queue and not state.queue:
            await start_placeholder()


async def start_placeholder():
    async with state.lock:
        if state.current and state.current.url == PLACEHOLDER_URL:
            return
        proc = state.process
    if proc:
        await stop_process(proc)
    placeholder_item = VideoItem(url=PLACEHOLDER_URL, title="placeholder")
    asyncio.create_task(safe_task(play_item(placeholder_item)))


async def start_queue_runner():
    async with state.lock:
        if state.playing_queue:
            return
        state.playing_queue = True

    while True:
        async with state.lock:
            if not state.playing_queue:
                break
            if not state.queue:
                state.playing_queue = False
                break
            item = state.queue.pop(0)
        async with state.lock:
            proc = state.process
        if proc:
            await stop_process(proc)
        await play_item(item)
    await start_placeholder()


# ----------------- Bot Handlers -----------------
@dp.message(Command("start"))
async def cmd_start_queue(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        if not state.queue:
            await msg.reply("Очередь пуста.")
            return
    asyncio.create_task(safe_task(start_queue_runner()))
    await msg.reply("Очередь запущена.")


@dp.message(Command("play"))
async def cmd_play(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.reply("Использование: /play <youtube_url>")
        return
    url = args[1].strip()
    await msg.reply(f"Запускаю: {url}")
    async with state.lock:
        proc = state.process
        was_placeholder = state.current and state.current.url == PLACEHOLDER_URL
    if proc:
        if was_placeholder and state.placeholder_last_start:
            played = time.time() - state.placeholder_last_start
            state.placeholder_pos += played
            state.placeholder_last_start = None
        await stop_process(proc)
    state.playing_queue = False
    asyncio.create_task(safe_task(play_item(VideoItem(url=url, added_by=msg.from_user.id))))


@dp.message(Command("add"))
async def cmd_add(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.reply("Использование: /add <youtube_url>")
        return
    url = args[1].strip()
    async with state.lock:
        state.queue.append(VideoItem(url=url, added_by=msg.from_user.id))
        qlen = len(state.queue)
    await msg.reply(f"Добавлено. Позиция в очереди: {qlen}")


@dp.message(Command("list"))
async def cmd_list(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        text = []
        if state.current:
            text.append(f"Сейчас: {state.current.url}")
        if state.queue:
            text.append("Очередь:")
            for i, it in enumerate(state.queue, 1):
                text.append(f"{i}. {it.url}")
        if not text:
            text = ["Нечего показывать. Сейчас нет запущенных видео и очередь пуста."]
    await msg.reply("\n".join(text))


@dp.message(Command("stop"))
async def cmd_stop(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        state.playing_queue = False
        proc = state.process
    if proc:
        await stop_process(proc)
    await msg.reply("Останавливаю очередь. Возвращаю заглушку.")
    asyncio.create_task(safe_task(start_placeholder()))


@dp.message(Command("pause"))
async def cmd_pause(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        proc = state.process
        cur = state.current
    if not proc:
        await msg.reply("Нечего ставить на паузу.")
        return
    try:
        pause_process_posix(proc)
        async with state.lock:
            state.is_paused = True
            if cur and cur.url == PLACEHOLDER_URL and state.placeholder_last_start:
                played = time.time() - state.placeholder_last_start
                state.placeholder_pos += played
                state.placeholder_last_start = None
        await msg.reply("Поставлено на паузу.")
    except Exception as e:
        await msg.reply(f"Ошибка паузы: {e}")


@dp.message(Command("resume"))
async def cmd_resume(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        proc = state.process
        cur = state.current
    if not proc:
        await msg.reply("Нечего резюмировать.")
        return
    try:
        resume_process_posix(proc)
        async with state.lock:
            state.is_paused = False
            if cur and cur.url == PLACEHOLDER_URL:
                state.placeholder_last_start = time.time()
        await msg.reply("Продолжил воспроизведение.")
    except Exception as e:
        await msg.reply(f"Ошибка resume: {e}")


@dp.message(Command("break"))
async def cmd_break(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        cur = state.current
        proc = state.process
        is_queue = state.playing_queue
    if not proc or not cur:
        await msg.reply("Сейчас ничего не играет, break не сработает.")
        return
    if cur.url == PLACEHOLDER_URL:
        await msg.reply("Нельзя прервать заглушку.")
        return
    await msg.reply("Прерываю текущее видео...")
    await stop_process(proc)
    if is_queue:
        await msg.reply("Если очередь запущена — включаю следующий.")
    else:
        await msg.reply("Возврат к заглушке.")
        asyncio.create_task(safe_task(start_placeholder()))


# ---------------- Startup -----------------
async def on_startup():
    asyncio.create_task(safe_task(start_placeholder()))
    try:
        await bot.send_message(OWNER_ID, "Бот запущен. Запущена видео-заглушка.")
    except Exception:
        pass


async def main():
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
        sys.exit(0)
