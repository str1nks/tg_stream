#!/usr/bin/env python3
import asyncio
import os
import signal
import subprocess
import sys
from dataclasses import dataclass, field
from typing import List, Optional
from yt_dlp import YoutubeDL
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# ================= CONFIG =================
TG_BOT_TOKEN = "8396785240:AAG_Ys7UP7C1iwZQPeLW8cc0j__xz8GeCaM"
OWNER_ID = 5135680104
RTMP_URL = "rtmps://dc4-1.rtmp.t.me/s/"
STREAM_KEY = "3114622344:MN4WNnEPwg7OPDCHqzw9Nw"
FULL_RTMP = RTMP_URL.rstrip("/") + "/" + STREAM_KEY
COOKIES_FILE = "cookies.txt"
FFMPEG_CMD = "ffmpeg"
# ==========================================

bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher()

@dataclass
class VideoItem:
    url: str
    added_by: Optional[int] = None

@dataclass
class StreamState:
    process: Optional[subprocess.Popen] = None
    queue: List[VideoItem] = field(default_factory=list)
    playing_queue: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

state = StreamState()

# ---------------- utils -------------------
def get_video_stream_url(youtube_url: str) -> str:
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "best[ext=mp4]/best",
        "noplaylist": True,
        "cookiefile": COOKIES_FILE,
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(youtube_url, download=False)
    if "url" in info:
        return info["url"]
    if "formats" in info:
        for f in info["formats"]:
            if f.get("url"):
                return f["url"]
    raise RuntimeError("Не удалось получить прямой потоковый URL")

def spawn_ffmpeg(input_url: str) -> subprocess.Popen:
    args = [
        FFMPEG_CMD,
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
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid)

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

# ---------------- core --------------------
async def play_item(item: VideoItem):
    async with state.lock:
        state.process = None
    try:
        direct = get_video_stream_url(item.url)
    except Exception as e:
        await bot.send_message(OWNER_ID, f"Ошибка получения потока: {e}")
        return
    proc = spawn_ffmpeg(direct)
    async with state.lock:
        state.process = proc
    await asyncio.to_thread(proc.wait)
    async with state.lock:
        state.process = None

async def start_queue_runner():
    async with state.lock:
        if state.playing_queue:
            return
        state.playing_queue = True
    while True:
        async with state.lock:
            if not state.queue:
                state.playing_queue = False
                break
            item = state.queue.pop(0)
            proc = state.process
        if proc:
            await stop_process(proc)
        await play_item(item)

# ---------------- bot commands ----------------
@dp.message(Command("play"))
async def cmd_play(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.reply("Использование: /play <youtube_url>")
        return
    url = args[1].strip()
    async with state.lock:
        proc = state.process
    if proc:
        await stop_process(proc)
    asyncio.create_task(play_item(VideoItem(url=url, added_by=msg.from_user.id)))
    await msg.reply(f"Воспроизвожу: {url}")

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
    await msg.reply(f"Добавлено в очередь. Позиция: {qlen}")

@dp.message(Command("list"))
async def cmd_list(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        text = []
        if state.process:
            text.append("Сейчас играет видео")
        if state.queue:
            text.append("Очередь:")
            for i, it in enumerate(state.queue, 1):
                text.append(f"{i}. {it.url}")
        if not text:
            text = ["Очередь пуста."]
    await msg.reply("\n".join(text))

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    asyncio.create_task(start_queue_runner())
    await msg.reply("Очередь запущена.")

@dp.message(Command("stop"))
async def cmd_stop(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        state.playing_queue = False
        proc = state.process
    if proc:
        await stop_process(proc)
    await msg.reply("Очередь остановлена.")

# ---------------- startup ----------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
