#!/usr/bin/env python3
# main.py — улучшенная устойчивая версия стрим-бота
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

# ================= CONFIG =================
TG_BOT_TOKEN = "8396785240:AAG_Ys7UP7C1iwZQPeLW8cc0j__xz8GeCaM"
OWNER_ID = 5135680104
RTMP_URL = "rtmps://dc4-1.rtmp.t.me/s/"
STREAM_KEY = "2438025732:ImXRMnHS6wkcQKxtsG9atQ"
FULL_RTMP = RTMP_URL.rstrip("/") + "/" + STREAM_KEY
COOKIES_FILE = "cookies.txt"

PLACEHOLDER_URL = "https://www.youtube.com/watch?v=G-kF940PFE4"
YTDLP_CMD = "yt-dlp"
FFMPEG_CMD = "ffmpeg"

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
FFMPEG_LOG = os.path.join(LOG_DIR, "ffmpeg.log")
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
    crash_counts: Dict[str, int] = field(default_factory=dict)  # track crashes per-url


state = StreamState()


# --------------- utils ----------------
async def owner_only(msg: Message) -> bool:
    return msg.from_user and msg.from_user.id == OWNER_ID


def get_video_stream_url(youtube_url: str, retries: int = 3, delay: float = 1.0) -> str:
    """
    Получаем прямой потоковый URL (HLS/DASH) для ffmpeg.
    Повторяем несколько раз при ошибках — иногда yt-dlp возвращает transient ошибки.
    """
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "best[ext=mp4]/best",
        "noplaylist": True,
        "cookiefile": COOKIES_FILE,
        "skip_download": True,
    }

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
            if not info:
                raise RuntimeError("yt-dlp returned empty info")
            if "url" in info and info["url"]:
                return info["url"]
            if "formats" in info:
                # try best candidate
                for f in reversed(info["formats"]):
                    u = f.get("url")
                    if u:
                        return u
            raise RuntimeError("Не удалось найти URL в мета-информации yt-dlp")
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(delay * attempt)
                continue
            raise RuntimeError(f"yt-dlp error: {e}") from e
    raise last_exc  # pragma: no cover


def spawn_ffmpeg(input_url: str, extra_args: Optional[List[str]] = None) -> subprocess.Popen:
    """
    Запускаем ffmpeg с low-latency параметрами.
    Используем start_new_session=True для кросс-платформенности вместо preexec_fn=os.setsid.
    Логи пишем в файл (чтобы видеть причину падений).
    """
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
        args = args[:-1] + extra_args + [args[-1]]

    # rotate simple: append timestamp block to log
    with open(FFMPEG_LOG, "a", encoding="utf-8") as lf:
        lf.write(f"\n\n--- FFMPEG START {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
        lf.flush()
    logfile = open(FFMPEG_LOG, "a", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            args,
            stdout=logfile,
            stderr=logfile,
            start_new_session=True,  # portable process group
        )
    except FileNotFoundError as e:
        logfile.write(f"FFMPEG not found: {e}\n")
        logfile.flush()
        logfile.close()
        raise
    return proc


async def stop_process(proc: subprocess.Popen):
    if not proc:
        return
    if proc.poll() is not None:
        return
    try:
        # try graceful group terminate
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


def pause_process_posix(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGSTOP)
        except Exception:
            proc.send_signal(signal.SIGSTOP)


def resume_process_posix(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
        except Exception:
            proc.send_signal(signal.SIGCONT)


# --------------- resilience helpers ----------------
def record_crash(url: str) -> int:
    c = state.crash_counts.get(url, 0) + 1
    state.crash_counts[url] = c
    return c


def reset_crashes(url: str):
    if url in state.crash_counts:
        del state.crash_counts[url]


# --------------- core streaming ----------------
async def play_item(item: VideoItem):
    async with state.lock:
        state.current = item
        state.is_paused = False

    try:
        direct = await asyncio.to_thread(get_video_stream_url, item.url)
    except Exception as e:
        await bot.send_message(OWNER_ID, f"Ошибка получения потока для {item.url}: {e}")
        async with state.lock:
            state.current = None
        return

    extra_ff_args = None
    if item.url == PLACEHOLDER_URL:
        ss = int(state.placeholder_pos)
        if ss > 0:
            extra_ff_args = ["-ss", str(ss)]

    try:
        proc = await asyncio.to_thread(spawn_ffmpeg, direct, extra_args)
    except Exception as e:
        await bot.send_message(OWNER_ID, f"Ошибка запуска ffmpeg для {item.url}: {e}")
        async with state.lock:
            state.current = None
        return

    async with state.lock:
        state.process = proc
        if item.url == PLACEHOLDER_URL:
            state.placeholder_last_start = time.time()

    # wait for process finish in background but handle unexpected exit
    try:
        await asyncio.to_thread(proc.wait)
    finally:
        async with state.lock:
            # update placeholder position if needed
            if item.url == PLACEHOLDER_URL and state.placeholder_last_start:
                played = time.time() - state.placeholder_last_start
                state.placeholder_pos += played
                state.placeholder_last_start = None
            state.process = None
            state.current = None

    # process exited — decide what to do (maybe crash)
    # Determine whether it was expected (we stopped it) or unexpected (crash)
    exited_code = proc.returncode
    if exited_code is None:
        exited_code = -999
    # Treat non-zero as crash (0 may be normal end-of-stream)
    if exited_code != 0:
        # record and attempt restart up to limit
        async with state.lock:
            c = record_crash(item.url)
        if c <= 5:
            backoff = min(30, 2 ** (c - 1))
            await bot.send_message(OWNER_ID, f"ffmpeg для {item.url} упал (code {exited_code}). Попытка перезапуска через {backoff}s (#{c})")
            await asyncio.sleep(backoff)
            # restart same item (resume placeholder logic preserved by play_item)
            asyncio.create_task(play_item(item))
            return
        else:
            await bot.send_message(OWNER_ID, f"ffmpeg для {item.url} падал слишком часто ({c} раз). Пропускаю.")
            reset_crashes(item.url)

    else:
        # graceful exit -> reset crash counter
        reset_crashes(item.url)

    async with state.lock:
        # if queue is playing, let queue runner drive next; otherwise start placeholder
        if state.playing_queue and state.queue:
            return
        if state.playing_queue and not state.queue:
            await start_placeholder()
        if not state.playing_queue:
            await start_placeholder()


async def start_placeholder():
    async with state.lock:
        if state.current and state.current.url == PLACEHOLDER_URL:
            return
        proc = state.process
    if proc:
        await stop_process(proc)
    placeholder_item = VideoItem(url=PLACEHOLDER_URL, title="placeholder")
    asyncio.create_task(play_item(placeholder_item))


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
            # protect loop from bubbling exceptions
            try:
                await bot.send_message(OWNER_ID, f"Ошибка при play_item: {e}")
            except Exception:
                pass
            await asyncio.sleep(1)

    await start_placeholder()


# --------------- monitor ----------------
async def monitor_loop():
    """
    Постоянно следит за состоянием ffmpeg. Если процесс внезапно пропал —
    убедится, что включена заглушка или перезапустит очередь.
    """
    while True:
        await asyncio.sleep(5)
        async with state.lock:
            proc = state.process
            cur = state.current
            playing_queue = state.playing_queue

        if proc:
            if proc.poll() is not None:
                # процесс завершился неожиданно — play_item логика уже пытается перезапустить,
                # но на всякий случай запускаем placeholder/queue контроллер
                try:
                    await bot.send_message(OWNER_ID, f"ffmpeg для {cur.url if cur else '---'} завершился с кодом {proc.returncode}")
                except Exception:
                    pass
                # small delay to let play_item handle restart; if no process after timeout, ensure placeholder
                await asyncio.sleep(2)
                async with state.lock:
                    if not state.process:
                        if playing_queue and state.queue:
                            asyncio.create_task(start_queue_runner())
                        else:
                            asyncio.create_task(start_placeholder())
        else:
            # нет процесса вообще — убедимся, что запущена заглушка
            async with state.lock:
                cur = state.current
                playing_queue = state.playing_queue
                has_queue = bool(state.queue)
            if not cur:
                if playing_queue and has_queue:
                    asyncio.create_task(start_queue_runner())
                else:
                    asyncio.create_task(start_placeholder())


# --------------- bot commands ----------------
@dp.message(Command("start"))
async def cmd_start_queue(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    await msg.reply("Запуск очереди...")
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
    # сразу стартуем новый play_item в фоне
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
        await start_placeholder()


# ---------------- startup ----------------
async def on_startup():
    # стартуем заглушку + монитор
    asyncio.create_task(start_placeholder())
    asyncio.create_task(monitor_loop())
    try:
        await bot.send_message(OWNER_ID, "Бот запущен. Заглушка включена; монитор активен.")
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
