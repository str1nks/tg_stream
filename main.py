#!/usr/bin/env python3
# main.py — минимально-надёжный стрим-бот (yt-dlp -> ffmpeg -> RTMP)
import asyncio
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from yt_dlp import YoutubeDL

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# ============== CONFIG ==============
TG_BOT_TOKEN = "8396785240:AAG_Ys7UP7C1iwZQPeLW8cc0j__xz8GeCaM"  # <-- поставь свой токен
OWNER_ID = 5135680104  # <-- твой айди
RTMP_URL = "rtmps://dc4-1.rtmp.t.me/s/"
STREAM_KEY = "2438025732:ImXRMnHS6wkcQKxtsG9atQ"  # <-- ключ
FULL_RTMP = RTMP_URL.rstrip("/") + "/" + STREAM_KEY
COOKIES_FILE = "cookies.txt"  # если нет — можно оставить, yt-dlp проигнорирует
PLACEHOLDER_URL = "https://www.youtube.com/watch?v=G-kF940PFE4"
FFMPEG_CMD = "ffmpeg"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
FFMPEG_LOG = os.path.join(LOG_DIR, "ffmpeg.log")
# ====================================

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
    crash_counts: Dict[str, int] = field(default_factory=dict)

state = StreamState()

# -------- utils --------
def get_video_stream_url(youtube_url: str, retries: int = 3, delay: float = 1.0) -> str:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "best[ext=mp4]/best",
        "noplaylist": True,
        "skip_download": True,
    }
    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
            if not info:
                raise RuntimeError("yt-dlp returned empty info")
            # direct url field
            if info.get("url"):
                return info["url"]
            # try formats (pick best available url)
            fmts = info.get("formats") or []
            for f in reversed(fmts):
                u = f.get("url")
                if u:
                    return u
            raise RuntimeError("Не найден прямой URL в данных yt-dlp")
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(delay * attempt)
                continue
            raise RuntimeError(f"yt-dlp error: {e}") from e
    raise last_exc  # pragma: no cover

def spawn_ffmpeg(input_url: str, extra_args: Optional[List[str]] = None) -> subprocess.Popen:
    args = [
        FFMPEG_CMD,
        "-fflags", "+nobuffer",
        "-flags", "+low_delay",
        "-avioflags", "direct",
        "-analyzeduration", "1000000",
        "-probesize", "500000",
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
        FULL_RTMP,
    ]
    if extra_args:
        # вставим перед FINAL RTMP
        args = args[:-1] + extra_args + [args[-1]]

    # логируем в файл + сохраняем дескриптор в объект процесса,
    # чтобы дескриптор не был закрыт сборщиком мусора
    logfile = open(FFMPEG_LOG, "a", encoding="utf-8")
    logfile.write(f"\n\n--- FFMPEG START {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
    logfile.flush()
    proc = subprocess.Popen(args, stdout=logfile, stderr=logfile, start_new_session=True)
    # держим ссылку
    setattr(proc, "_logfile", logfile)
    return proc

async def stop_process(proc: subprocess.Popen):
    if not proc or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=10)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

def pause_process(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGSTOP)
        except Exception:
            proc.send_signal(signal.SIGSTOP)

def resume_process(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
        except Exception:
            proc.send_signal(signal.SIGCONT)

def record_crash(url: str) -> int:
    c = state.crash_counts.get(url, 0) + 1
    state.crash_counts[url] = c
    return c

def reset_crashes(url: str):
    if url in state.crash_counts:
        del state.crash_counts[url]

# ------ core ------

async def play_item(item: VideoItem):
    async with state.lock:
        state.current = item
        state.is_paused = False

    try:
        direct = await asyncio.to_thread(get_video_stream_url, item.url)
    except Exception as e:
        try:
            await bot.send_message(OWNER_ID, f"Ошибка получения потока для {item.url}: {e}")
        except Exception:
            pass
        async with state.lock:
            state.current = None
        return

    extra_ff_args = None
    if item.url == PLACEHOLDER_URL:
        ss = int(state.placeholder_pos)
        if ss > 0:
            extra_ff_args = ["-ss", str(ss)]

    try:
        proc = await asyncio.to_thread(spawn_ffmpeg, direct, extra_ff_args)
    except Exception as e:
        try:
            await bot.send_message(OWNER_ID, f"Ошибка запуска ffmpeg для {item.url}: {e}")
        except Exception:
            pass
        async with state.lock:
            state.current = None
        return

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

    code = proc.returncode if proc.returncode is not None else -999
    if code != 0:
        async with state.lock:
            c = record_crash(item.url)
        if c <= 5:
            backoff = min(30, 2 ** (c - 1))
            try:
                await bot.send_message(OWNER_ID, f"ffmpeg для {item.url} упал (code {code}). Перезапуск через {backoff}s (#{c})")
            except Exception:
                pass
            await asyncio.sleep(backoff)
            asyncio.create_task(play_item(item))
            return
        else:
            try:
                await bot.send_message(OWNER_ID, f"ffmpeg для {item.url} падал слишком часто ({c} раз). Пропускаю.")
            except Exception:
                pass
            reset_crashes(item.url)
    else:
        reset_crashes(item.url)

    async with state.lock:
        if state.playing_queue and state.queue:
            return
    await start_placeholder()

async def start_placeholder():
    async with state.lock:
        if state.current and state.current.url == PLACEHOLDER_URL:
            return
        proc = state.process
    if proc:
        await stop_process(proc)
    placeholder = VideoItem(url=PLACEHOLDER_URL, title="placeholder")
    asyncio.create_task(play_item(placeholder))

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
        try:
            await play_item(item)
        except Exception as e:
            try:
                await bot.send_message(OWNER_ID, f"Ошибка в play_item: {e}")
            except Exception:
                pass
            await asyncio.sleep(1)

    await start_placeholder()

async def monitor_loop():
    while True:
        await asyncio.sleep(5)
        async with state.lock:
            proc = state.process
            cur = state.current
            playing_queue = state.playing_queue
        if proc and proc.poll() is not None:
            try:
                await bot.send_message(OWNER_ID, f"ffmpeg для {cur.url if cur else '---'} завершился с {proc.returncode}")
            except Exception:
                pass
            await asyncio.sleep(2)
            async with state.lock:
                if not state.process:
                    if playing_queue and state.queue:
                        asyncio.create_task(start_queue_runner())
                    else:
                        asyncio.create_task(start_placeholder())
        else:
            async with state.lock:
                if not state.process and not state.current:
                    if state.playing_queue and state.queue:
                        asyncio.create_task(start_queue_runner())
                    else:
                        asyncio.create_task(start_placeholder())

# ------ bot commands ------
@dp.message(Command("start"))
async def cmd_start(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        if not state.queue:
            await msg.reply("Очередь пуста.")
            return
    asyncio.create_task(start_queue_runner())
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
    asyncio.create_task(play_item(VideoItem(url=url, added_by=msg.from_user.id)))

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
    await msg.reply(f"Добавлено. Позиция: {qlen}")

@dp.message(Command("list"))
async def cmd_list(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        lines = []
        if state.current:
            lines.append(f"Сейчас: {state.current.url}")
        if state.queue:
            lines.append("Очередь:")
            for i, it in enumerate(state.queue, 1):
                lines.append(f"{i}. {it.url}")
        if not lines:
            lines = ["Нечего показывать."]
    await msg.reply("\n".join(lines))

@dp.message(Command("stop"))
async def cmd_stop(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    async with state.lock:
        state.playing_queue = False
        proc = state.process
    if proc:
        await stop_process(proc)
    await msg.reply("Стоп. Включаю заглушку.")
    await start_placeholder()

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
        pause_process(proc)
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
        resume_process(proc)
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
        await msg.reply("Ничего не играет.")
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
        await start_placeholder()

# ------- startup -------
async def on_startup():
    asyncio.create_task(start_placeholder())
    asyncio.create_task(monitor_loop())
    try:
        await bot.send_message(OWNER_ID, "Бот запущен. Заглушка и монитор активны.")
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
