import argparse
import glob
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import yt_dlp
from skimage import io
from skimage.transform import resize

failure_log_lock = Lock()


def download_and_process(
    data, vid, num_videos, mode, output_root, process_pool, videos_dir
):
    videoname = data.url.split("=")[-1]
    video_path = os.path.join(videos_dir, f"{videoname}.mp4")
    print(f"[INFO] Downloading {vid + 1}/{num_videos}: {videoname} ...")
    if not os.path.exists(video_path):
        try:
            # pytube is unstable, use yt_dlp instead
            ydl_opts = {
                "format": "bestvideo[height<=480]",
                "outtmpl": video_path,
                "cookiefile": "./cookies-yt.txt",
            }

            # Initialize yt_dlp and download the video
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([data.url])
        except Exception:
            with failure_log_lock:
                failure_log = open("failed_videos_" + mode + ".txt", "a")
                failure_log.writelines(data.url + "\n")
                failure_log.close()
            return
    else:
        print(
            f"[INFO] Video {vid + 1}/{num_videos}: {videoname} already exists in {videos_dir}, skipping..."
        )

    # Submit to process pool
    futures = []
    for seq_id in range(len(data)):
        futures.append(
            process_pool.submit(process, data, seq_id, video_path, output_root)
        )

    for future in futures:
        future.result()

    os.remove(videoname)


class Data:
    def __init__(self, url, seqname, list_timestamps):
        self.url = url
        self.list_seqnames = []
        self.list_list_timestamps = []

        self.list_seqnames.append(seqname)
        self.list_list_timestamps.append(list_timestamps)

    def add(self, seqname, list_timestamps):
        self.list_seqnames.append(seqname)
        self.list_list_timestamps.append(list_timestamps)

    def __len__(self):
        return len(self.list_seqnames)


def process(data, seq_id, video_path, output_root):
    seqname = data.list_seqnames[seq_id]
    output_dir = f"{output_root}/{seqname}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Create
    list_str_timestamps = []
    for timestamp in data.list_list_timestamps[seq_id]:
        timestamp = int(timestamp / 1000)
        str_hour = str(int(timestamp / 3600000)).zfill(2)
        str_min = str(int(int(timestamp % 3600000) / 60000)).zfill(2)
        str_sec = str(int(int(int(timestamp % 3600000) % 60000) / 1000)).zfill(2)
        str_mill = str(int(int(int(timestamp % 3600000) % 60000) % 1000)).zfill(3)
        _str_timestamp = str_hour + ":" + str_min + ":" + str_sec + "." + str_mill
        list_str_timestamps.append(_str_timestamp)

    if len(list_str_timestamps) == len(glob.glob(output_dir + "/*.png")):
        print(
            f"[WARNING] The output dir {output_dir} has already existed with the same number of frames, skipping..."
        )
        return False

    command_parts = ["ffmpeg -loglevel error"]
    for idx, (str_timestamp, timestamp) in enumerate(
        zip(list_str_timestamps, data.list_list_timestamps[seq_id])
    ):
        command_parts.extend(
            [
                f"-ss {str_timestamp}",
                f"-i {video_path}",
                f"-vframes 1",
                f"-f image2",
                f"-map {idx}:v:0",
                f"{output_dir}/{timestamp}.png",
            ]
        )
    command = " ".join(command_parts)
    try:
        os.system(command)
    except Exception as err:
        print(f"[ERROR] Failed to process {data.url}: {err}")
        shutil.rmtree(output_dir)
        return True

    png_list = glob.glob(output_dir + "/*.png")

    for pngname in png_list:
        image = io.imread(pngname)
        if int(image.shape[1] / 2) >= 500:
            image = resize(
                image,
                (int(image.shape[0] / 2), int(image.shape[1] / 2)),
                anti_aliasing=True,
            )
            image = (image * 255).astype("uint8")
            io.imsave(pngname, image)
    return False


class DataDownloader:
    def __init__(self, dataroot, output_root, videos_dir, mode="test"):
        print("[INFO] Loading data list ... ", end="")
        self.dataroot = dataroot
        self.output_root = output_root
        self.videos_dir = videos_dir
        self.list_seqnames = sorted(glob.glob(dataroot + "/*.txt"))
        self.mode = mode

        os.makedirs(self.output_root, exist_ok=True)
        os.makedirs(self.videos_dir, exist_ok=True)

        self.list_data = []
        for txt_file in self.list_seqnames:
            dir_name = txt_file.split("/")[-1]
            seq_name = dir_name.split(".")[0]

            # extract info from txt
            seq_file = open(txt_file, "r")
            lines = seq_file.readlines()
            youtube_url = ""
            list_timestamps = []
            for idx, line in enumerate(lines):
                if idx == 0:
                    youtube_url = line.strip()
                else:
                    timestamp = int(line.split(" ")[0])
                    list_timestamps.append(timestamp)
            seq_file.close()

            isRegistered = False
            for i in range(len(self.list_data)):
                if youtube_url == self.list_data[i].url:
                    isRegistered = True
                    self.list_data[i].add(seq_name, list_timestamps)
                else:
                    pass

            if not isRegistered:
                self.list_data.append(Data(youtube_url, seq_name, list_timestamps))

        # self.list_data.reverse()
        print(f"[INFO] {len(self.list_data)} movies are used in {self.mode} mode")

    def run(self):
        num_videos = len(self.list_data)
        print(f"[INFO] Start downloading {num_videos} movies")
        with ProcessPoolExecutor(max_workers=16) as process_pool:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(
                        download_and_process,
                        data,
                        vid,
                        num_videos,
                        self.mode,
                        self.output_root,
                        process_pool,
                        self.videos_dir,
                    )
                    for vid, data in enumerate(self.list_data)
                ]
                for future in futures:
                    future.result()
        print("[INFO] Done!")

    def show(self):
        print("########################################")
        global_count = 0
        for data in self.list_data:
            print(" URL : {}".format(data.url))

            for idx in range(len(data)):
                print(" SEQ_{} : {}".format(idx, data.list_seqnames[idx]))
                print(" LEN_{} : {}".format(idx, len(data.list_list_timestamps[idx])))
                global_count = global_count + 1
            print("----------------------------------------")

        print("TOTAL : {} sequnces".format(global_count))
        print("########################################")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="test", help="test or train")
    parser.add_argument(
        "--dataroot",
        type=str,
        default="./RealEstate10K",
        help="path to the dataset file location",
    )
    parser.add_argument(
        "--output_root",
        type=str,
        default="./dataset",
        help="path to the output directory",
    )
    args = parser.parse_args()
    mode = args.mode

    dataroot = Path(args.dataroot) / mode
    output_root = Path(args.output_root) / mode
    videos_dir = Path(args.videos_dir)

    downloader = DataDownloader(str(dataroot), str(output_root), str(videos_dir), mode)

    downloader.show()
    downloader.run()
