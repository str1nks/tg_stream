#!/usr/bin/env python3
# main.py
import asyncio
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# ========== CONFIG (вставь свои данные, уже взял из сообщения) ==========
TG_BOT_TOKEN = "8396785240:AAG_Ys7UP7C1iwZQPeLW8cc0j__xz8GeCaM"
OWNER_ID = 5135680104
RTMP_URL = "rtmps://dc4-1.rtmp.t.me/s/"
STREAM_KEY = "2912540670:bv_QzYpMt4WeUqenJ03Peg"
FULL_RTMP = RTMP_URL.rstrip("/") + "/" + STREAM_KEY
# Заглушка (циклично)
PLACEHOLDER_URL = "https://www.youtube.com/watch?v=G-kF940PFE4"
# Путь к утилитам
YTDLP_CMD = "yt-dlp"  # или полный путь
FFMPEG_CMD = "ffmpeg"  # или полный путь
# ========================================================================

bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher()


@dataclass
class VideoItem:
    url: str
    title: Optional[str] = None  # можно расширить через yt-dlp info
    added_by: Optional[int] = None


@dataclass
class StreamState:
    process: Optional[subprocess.Popen] = None
    current: Optional[VideoItem] = None
    queue: List[VideoItem] = field(default_factory=list)
    playing_queue: bool = False
    is_paused: bool = False
    placeholder_pos: float = 0.0  # seconds already played of placeholder
    placeholder_last_start: Optional[float] = None  # timestamp when placeholder started
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


state = StreamState()


# ---------------- utilities ----------------
async def owner_only(msg: Message) -> bool:
    return msg.from_user and msg.from_user.id == OWNER_ID


def get_video_stream_url(youtube_url: str) -> str:
    """
    Возвращает прямой URL для ffmpeg через yt-dlp -g.
    Бросает RuntimeError при ошибке.
    """
    try:
        res = subprocess.run(
            [YTDLP_CMD, "-f", "best", "-g", youtube_url],
            capture_output=True,
            check=True,
            text=True,
            timeout=30,
        )
        url = res.stdout.strip().splitlines()[0]
        if not url:
            raise RuntimeError("yt-dlp вернул пустой адрес")
        return url
    except Exception as e:
        raise RuntimeError(f"Can't get stream url: {e}")


def spawn_ffmpeg(input_url: str, extra_args: Optional[List[str]] = None) -> subprocess.Popen:
    """
    Запускает ffmpeg, читает из input_url и пушит в FULL_RTMP.
    Команда настроена на потоковую передачу (h264, aac).
    """
    args = [
        FFMPEG_CMD,
        "-re",                # читать в реальном времени
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
    # Запускаем в новой сессии чтобы корректно посылать сигналы
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
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
    # Requires POSIX
    if proc and proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGSTOP)


def resume_process_posix(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGCONT)


# ------------ core streaming logic -------------
async def play_item(item: VideoItem, allow_placeholder_resume: bool = True):
    """
    Запустить один item (играет пока ffmpeg не завершится).
    Если item.url == PLACEHOLDER_URL, стартует с placeholder_pos и обновляет placeholder_last_start.
    """
    async with state.lock:
        state.current = item
        state.is_paused = False

    # prepare input stream URL
    try:
        direct = get_video_stream_url(item.url)
    except Exception as e:
        await bot.send_message(OWNER_ID, f"Ошибка получения потока для {item.url}: {e}")
        return

    # if placeholder - seek to saved position
    extra_ff_args = None
    if item.url == PLACEHOLDER_URL:
        # use ffmpeg -ss to start from offset (approximate). To resume, we need to re-run ffmpeg with -ss
        ss = int(state.placeholder_pos)
        if ss > 0:
            extra_ff_args = ["-ss", str(ss)]
    proc = spawn_ffmpeg(direct, extra_args=extra_ff_args)
    async with state.lock:
        state.process = proc
        if item.url == PLACEHOLDER_URL:
            state.placeholder_last_start = time.time()

    # Wait for process end in background (read stderr for detection optional)
    try:
        await asyncio.to_thread(proc.wait)
    finally:
        # process ended
        async with state.lock:
            # if it was placeholder, accumulate played time
            if item.url == PLACEHOLDER_URL and state.placeholder_last_start:
                played = time.time() - state.placeholder_last_start
                state.placeholder_pos += played
                state.placeholder_last_start = None
            state.process = None
            state.current = None

    # after a normal video ends, if we are playing queue, continue to next
    async with state.lock:
        if state.playing_queue and state.queue:
            # next will be started by queue runner
            return
        if state.playing_queue and not state.queue:
            # queue exhausted -> return to placeholder
            await start_placeholder()
        # if not queue playing, do nothing (stay on placeholder or idle)


async def start_placeholder():
    """
    Запускает заглушку (заменяет текущий процесс).
    """
    async with state.lock:
        # if already playing placeholder, nothing
        if state.current and state.current.url == PLACEHOLDER_URL:
            return
        # stop current process if any
        proc = state.process
    if proc:
        # if stopping other video, and it's not placeholder, we must end it
        await stop_process(proc)

    # start placeholder item (it will resume using placeholder_pos)
    placeholder_item = VideoItem(url=PLACEHOLDER_URL, title="placeholder")
    # spawn as background task
    asyncio.create_task(play_item(placeholder_item))


async def start_queue_runner():
    """
    Запускает последовательный показ очереди.
    """
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
        # stop current process (placeholder or single play)
        async with state.lock:
            proc = state.process
        if proc:
            await stop_process(proc)
        # play this item synchronously (task will wait until ended)
        await play_item(item)
        # loop continues and picks next
    # queue finished -> return to placeholder
    await start_placeholder()


# --------------- Bot handlers -----------------
@dp.message(Command("start"))
async def cmd_start_queue(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    await msg.reply("Запуск очереди...")
    # start queue runner if anything in queue
    async with state.lock:
        if not state.queue:
            await msg.reply("Очередь пуста.")
            return
    # start runner in background
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
    # stop current (but save placeholder progress if placeholder was playing)
    async with state.lock:
        proc = state.process
        was_placeholder = state.current and state.current.url == PLACEHOLDER_URL
    if proc:
        # if stopping placeholder, accumulate played time
        if was_placeholder and state.placeholder_last_start:
            played = time.time() - state.placeholder_last_start
            state.placeholder_pos += played
            state.placeholder_last_start = None
        await stop_process(proc)
    # not queue: single play
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
    # start placeholder
    await msg.reply("Останавливаю очередь. Возвращаю заглушку.")
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
    # POSIX pause
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
    # Stop current video; if queue running, next will start automatically; else go to placeholder
    await msg.reply("Прерываю текущее видео...")
    await stop_process(proc)
    if is_queue:
        # queue runner will handle next item
        await msg.reply("Если очередь запущена — включаю следующий.")
    else:
        await msg.reply("Возврат к заглушке.")
        await start_placeholder()


# -------------- startup / main -----------------
async def on_startup():
    # при старте — запустить placeholder и уведомить владельца
    asyncio.create_task(start_placeholder())
    try:
        await bot.send_message(OWNER_ID, "Бот запущен. Запущена видео-заглушка.")
    except Exception:
        # просто продолжаем — возможно бот не может отправить сообщение
        pass


async def main():
    # register handlers already via decorator usage
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    # Запуск
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
        sys.exit(0)

