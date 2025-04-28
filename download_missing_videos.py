import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import yt_dlp

failure_log_lock = Lock()


def download_video(video_url, vid, num_videos, videos_dir):
    videoname = video_url.split("=")[-1]
    video_path = os.path.join(videos_dir, f"{videoname}.mp4")
    print(f"[INFO] Downloading {vid + 1}/{num_videos}: {videoname} ...")
    if not os.path.exists(video_path):
        try:
            # pytube is unstable, use yt_dlp instead
            ydl_opts = {
                "format": "bestvideo[height<=480]",
                "check_formats": True,
                "outtmpl": video_path,
                "cookiefile": "./cookies-yt.txt",
            }

            # Initialize yt_dlp and download the video
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
        except Exception:
            with failure_log_lock:
                failure_log = open("failed_videos_new.txt", "a")
                failure_log.writelines(video_url + "\n")
                failure_log.close()
            return
    else:
        print(
            f"[INFO] Video {vid + 1}/{num_videos}: {videoname} already exists in {videos_dir}, skipping..."
        )


class DataDownloader:
    def __init__(self, missing_videos_file, videos_dir):
        print("[INFO] Loading data list ... ", end="")
        self.missing_videos_file = missing_videos_file
        self.videos_dir = videos_dir

        os.makedirs(self.videos_dir, exist_ok=True)
        self.list_data = []
        with open(self.missing_videos_file, "r") as f:
            for line in f:
                self.list_data.append(line.strip())

        print(f"[INFO] {len(self.list_data)} movies to be downloaded that are missing")

    def run(self):
        num_videos = len(self.list_data)
        print(f"[INFO] Start downloading {num_videos} movies")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    download_video,
                    video_url,
                    vid,
                    num_videos,
                    self.videos_dir,
                )
                for vid, video_url in enumerate(self.list_data)
            ]
            for future in futures:
                future.result()
        print("[INFO] Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--missing_videos_file",
        type=str,
        default="./missing_videos.txt",
        help="path to the missing videos file",
    )
    parser.add_argument(
        "--videos_dir",
        type=str,
        default="./videos",
        help="path to the videos directory",
    )
    args = parser.parse_args()

    videos_dir = Path(args.videos_dir)
    assert Path(
        args.missing_videos_file
    ).exists(), f"Missing videos file {args.missing_videos_file} does not exist"

    downloader = DataDownloader(str(args.missing_videos_file), str(videos_dir))

    downloader.run()
