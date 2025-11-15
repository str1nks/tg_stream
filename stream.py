import subprocess

input_file = "video.mp4"
server = "rtmps://dc4-1.rtmp.t.me/s/"
stream_key = "3114622344:MN4WNnEPwg7OPDCHqzw9Nw"

stream_url = f"{server}{stream_key}"

cmd = [
    "ffmpeg",
    "-re",
    "-stream_loop", "-1",    # бесконечный луп
    "-i", input_file,
    "-vcodec", "libx264",
    "-preset", "veryfast",
    "-acodec", "aac",
    "-ar", "44100",
    "-b:a", "128k",
    "-f", "flv",
    stream_url
]

subprocess.run(cmd)
