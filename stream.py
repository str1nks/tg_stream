import os
import asyncio
import subprocess
from aiogram import Bot, Dispatcher, types, F, html
from aiogram.enums import ContentType
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession

def log(*args):
    print("[LOG]", *args)

# --- Telegram bot setup ---
session = AiohttpSession(timeout=120)  # увеличить таймаут скачивания
bot = Bot(
    token="8396785240:AAG_Ys7UP7C1iwZQPeLW8cc0j__xz8GeCaM",
    session=session,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

# --- Video paths ---
VIDEO_DIR = "videos"
DEFAULT_VIDEO = "video.mp4"

os.makedirs(VIDEO_DIR, exist_ok=True)

# --- Streaming state ---
ffmpeg_process = None   # текущий процесс ffmpeg
current_video = None    # текущее воспроизводимое видео
pending_upload_name = {}  # user_id -> filename


async def stop_ffmpeg():
    global ffmpeg_process

    if ffmpeg_process:
        log("Останавливаю текущий FFmpeg процесс…")
        try:
            if ffmpeg_process.returncode is None:
                ffmpeg_process.kill()
                await ffmpeg_process.wait()
                log("FFmpeg остановлен.")
        except Exception as e:
            log("Ошибка при остановке FFmpeg:", e)

    ffmpeg_process = None



# --- Play video via FFmpeg ---
async def play_video(video_path, loop=False):
    global ffmpeg_process, current_video

    await stop_ffmpeg()

    log(f"Запускаю FFmpeg: {video_path} {'(loop)' if loop else ''}")

    cmd = [
        "ffmpeg",
        "-re",
        "-i", video_path,
        "-vcodec", "libx264",
        "-preset", "veryfast",
        "-acodec", "aac",
        "-ar", "44100",
        "-b:a", "128k",
        "-f", "flv",
        "rtmps://dc4-1.rtmp.t.me/s/3114622344:MN4WNnEPwg7OPDCHqzw9Nw"
    ]

    if loop:
        cmd.insert(1, "-stream_loop")
        cmd.insert(2, "-1")

    try:
        ffmpeg_process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        current_video = video_path
        log("FFmpeg ЗАПУЩЕН. PID:", ffmpeg_process.pid)

    except Exception as e:
        log("FFmpeg НЕ ЗАПУСТИЛСЯ:", e)
        ffmpeg_process = None
        return

    async def watch_errors():
        if ffmpeg_process:
            stderr = await ffmpeg_process.stderr.read()
            text = stderr.decode(errors="ignore")

            if "moov atom not found" in text or "Invalid data" in text:
                log("FFmpeg ERROR: битый mp4 файл!")
                await stop_ffmpeg()

            # общий фулл-лог
            log("FFmpeg stderr:\n", text)

    asyncio.create_task(watch_errors())


# --- Autostart default video ---
async def start_default_video():
    log("Автостарт основного видео…")
    await play_video(os.path.join(VIDEO_DIR, DEFAULT_VIDEO), loop=True)


# --- /list ---
@dp.message(F.text == "/list")
async def list_videos(message: types.Message):
    videos = [f for f in os.listdir(VIDEO_DIR) if f.endswith(".mp4")]
    if not videos:
        await message.answer("В папке нет видео.")
        return

    await message.answer(
        "Доступные видео:\n" +
        "\n".join([html.code(v) for v in videos])
    )


# --- /play <filename> ---
@dp.message(lambda m: m.text and m.text.startswith("/play "))
async def play_selected_video(message: types.Message):
    filename = message.text.split("/play ", 1)[1].strip()
    path = os.path.join(VIDEO_DIR, filename)

    if not os.path.exists(path):
        await message.answer(f"Видео {html.code(filename)} не найдено.")
        return

    await message.answer(f"Запускаю временное видео: {html.code(filename)}")
    print("Запускаю временное видео:", filename)

    # 1) запускаем временное видео
    await play_video(path, loop=False)

    # ждём завершения процесса
    if ffmpeg_process:
        try:
            await ffmpeg_process.wait()
        except Exception:
            pass

    # 2) возвращаемся к бесконечному стриму
    await play_video(os.path.join(VIDEO_DIR, DEFAULT_VIDEO), loop=True)
    await message.answer("Временное видео закончилось — включаю основное.")


# --- Видео загрузка ---
@dp.message(F.content_type == ContentType.VIDEO)
async def upload_video(message: types.Message):
    global pending_upload_name
    user_id = message.from_user.id
    video = message.video

    if video.mime_type != "video/mp4":
        await message.answer("Можно загружать только MP4.")
        return

    # имя выбрал через /upload ?
    if user_id in pending_upload_name:
        filename = pending_upload_name.pop(user_id)
    else:
        filename = video.file_name or f"{video.file_unique_id}.mp4"

    path = os.path.join(VIDEO_DIR, filename)

    try:
        await message.answer("Скачивание...")
        await bot.download(video.file_id, destination=path)
        await message.answer(f"Видео сохранено как {html.code(filename)}")
    except Exception as e:
        await message.answer("Ошибка при скачивании файла.")
        print("[Download ERROR]", e)

@dp.message(lambda m: m.text and m.text.startswith("/upload "))
async def choose_upload_name(message: types.Message):
    global pending_upload_name
    name = message.text.split("/upload ", 1)[1].strip()

    if not name.endswith(".mp4"):
        await message.answer("Название файла должно заканчиваться на .mp4")
        return

    pending_upload_name[message.from_user.id] = name
    await message.answer(f"Ок, загружай видео. Оно сохранится как {html.code(name)}")

@dp.message(F.text == "/stoptemp")
async def stop_temp_video(message: types.Message):
    await message.answer("Останавливаю временное видео...")
    await stop_ffmpeg()  # стоп текущего стрима
    await play_video(os.path.join(VIDEO_DIR, DEFAULT_VIDEO), loop=True)
    await message.answer("Основное видео снова в эфире.")

# --- Main ---
async def main():
    asyncio.create_task(start_default_video())
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(stop_ffmpeg())