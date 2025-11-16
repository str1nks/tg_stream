from yt_dlp import YoutubeDL
import subprocess

url = "https://www.youtube.com/watch?v=G-kF940PFE4"
ydl_opts = {"format": "best", "cookiefile": "cookies.txt"}

with YoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(url, download=False)
direct = info["url"]
print(direct)

subprocess.run([
    "ffmpeg", "-re", "-i", direct,
    "-c:v", "libx264", "-preset", "veryfast",
    "-c:a", "aac", "-b:a", "160k",
    "-f", "flv", "rtmps://dc4-1.rtmp.t.me/s/3114622344:MN4WNnEPwg7OPDCHqzw9Nw"
])
