import argparse
import logging
import os
import re
from datetime import datetime
from time import sleep

import pytz
import youtube_dl
from TikTokApi import TikTokApi
from numpy import random

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def _format_timestamp_iso(tz, timestamp):
    return datetime.fromtimestamp(int(timestamp), tz).isoformat()[:-6].replace(":", "_")


def _format_bytes(num):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%sB" % (num, unit)
        num /= 1024.0
    return "%.1f%s" % (num, "YiB",)


class Ripper:
    def __init__(self, username, download_path, skip_downloaded, timezone_code, sleep_min, sleep_max):
        """Initialize a Ripper
        :param username: TikTok user to rip
        :param download_path: Path in which to create user folder and download videos
        :param skip_downloaded: If True, skip downloading videos whose IDs are already in the download folder on disk
        :param timezone_code: Formatted like UTC
        :param sleep_min: Minimum delay between downloads, in Seconds
        :param sleep_max: Maximum delay between downloads, in Seconds
        """
        self.api = TikTokApi().get_instance()
        self.username = username
        self.download_path = download_path
        self.skip_downloaded = skip_downloaded
        self.tz = pytz.timezone(timezone_code)
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        logger.info("Fetching video list of user @" + username + " with TikTokApi")
        self.video_count = self.api.get_user(self.username)["userInfo"]["stats"]["videoCount"]
        # Supposedly the by_username method is limited to around 2000, according to TikTokApi documentation
        if self.video_count > 1900:
            logger.warning("TikTokApi may encounter issues if a user has posted ~2000 videos. Video count: " +
                           str(self.video_count))
        self.videos = self.api.by_username(self.username, count=self.video_count)
        self.fallback_counter = 0
        self.ytdl_downloaderror_counter = 0
        self.other_error_counter = 0
        logger.debug("Ripper init complete")

    def __repr__(self):
        """Override str() when used on a Ripper object
        :return: String representation of this Ripper instance
        """
        return "Username: " + str(self.username) + \
               ", Video count in metadata: " + str(self.video_count) + \
               ", Videos IDs found: " + str(len(self.videos))

    @staticmethod
    def _download_with_ytdl(file_path, video_url):
        ydl_opts = {"outtmpl": file_path}
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

    def _download_with_api(self, file_path, video_url):
        logger.debug("Downloading video with TikTokApi: " + video_url)
        video_bytes = self.api.get_Video_By_Url(video_url)
        size = len(video_bytes)
        if size < 1024:
            raise AssertionError("Only " + _format_bytes(size) + " received")
        logger.debug("Writing " + _format_bytes(size) + " video to file: " + file_path)
        with open(file_path, "wb") as f:
            f.write(video_bytes)

    @staticmethod
    def _format_video_url(tiktok_object):
        return "https://www.tiktok.com/@{}/video/{}?lang=en".format(tiktok_object["author"]["uniqueId"],
                                                                    tiktok_object["id"])

    def _format_file_name(self, timestamp, video_id):
        return "{}_{}.mp4".format(_format_timestamp_iso(self.tz, timestamp), video_id)

    @staticmethod
    def _parse_file_name(file_name: str):
        match = re.search(r"(.+)_(.+?)\.mp4", file_name, re.IGNORECASE)
        if match:
            return {"timestamp": datetime.strptime(match.group(1), '%Y-%m-%dT%H_%M_%S'), "id": match.group(2)}
        else:
            return None

    def download_video(self, file_path, video_url, video_creation_time):
        """Download a single one of the user's videos
        :param file_path: path to file download location
        :param video_url: TikTok video URL
        :param video_creation_time: timestamp of video creation
        :return: True if success, False if failure
        """
        logger.debug("Downloading video created at " + _format_timestamp_iso(self.tz, video_creation_time) + " from "
                     + video_url + " to " + file_path)
        failed = False
        try:
            self._download_with_api(file_path, video_url)
        except Exception as e:
            logger.debug("Video download failed using TikTokApi: " + str(e))
            failed = True
        if not os.path.isfile(file_path):
            failed = True
            logger.debug("No file was created by TikTokApi at " + file_path)
        elif os.stat(file_path).st_size < 1024:
            failed = True
            try:
                os.remove(file_path)
                logger.debug("Deleted malformed TikTokApi download at " + file_path)
            except Exception as ee:
                logger.error("Unable to delete malformed TikTokApi download at " + str(ee))
        if failed:
            sleep_time = random.uniform(self.sleep_min, self.sleep_max)
            logger.info("Sleeping for: " + str(sleep_time) + " seconds")
            sleep(sleep_time)
            try:
                logger.debug("Falling back to YouTube-dl")
                self.fallback_counter += 1
                self._download_with_ytdl(file_path, video_url)
                if not os.path.isfile(file_path):
                    raise AssertionError("No file was created by YouTube-dl at " + file_path)
                elif os.stat(file_path).st_size < 1024:
                    try:
                        os.remove(file_path)
                        logger.debug("Deleted malformed YouTube-dl download at " + file_path)
                    except Exception as ee:
                        raise AssertionError("Malformed file was created at " + file_path +
                                             " and could not be removed: " + str(ee))
                    raise AssertionError("Malformed file was created at " + file_path + " and was removed")
                failed = False
            except youtube_dl.utils.DownloadError as ee:
                logger.error("YouTube-dl DownloadError: " + str(ee))
                self.ytdl_downloaderror_counter += 1
                failed = True
            except Exception as ee:
                logger.error("Video download failed with YouTube-dl: " + str(ee))
                self.other_error_counter += 1
                failed = True
        if not failed:
            try:
                os.utime(file_path, (video_creation_time, video_creation_time))
            except Exception as e:
                logger.debug("Unable to set utime of " + str(video_creation_time) + " on file " + file_path +
                             ", Error: " + str(e))
            return True
        return False

    def download_all(self):
        """Download all of the user's videos
        :return: a Dict with a list of successful IDs, failed IDs, and skipped (already downloaded) IDs
        """
        download_path = os.path.join(self.download_path, self.username)
        already_downloaded = []
        successful_downloads = []
        failed_downloads = []
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        elif not os.path.isdir(download_path):
            raise NotADirectoryError("Download path is not a directory: " + download_path)
        elif self.skip_downloaded:
            for item in os.listdir(download_path):
                file_path = str(os.path.join(download_path, item))
                if os.path.isfile(file_path):
                    parsed_file = self._parse_file_name(os.path.basename(file_path))
                    if parsed_file is not None:
                        already_downloaded.append(parsed_file["id"])
        for index, item in enumerate(self.videos):
            # Don't download it if the user has set that option, and the tiktok already exists on the disk
            if item["id"] in already_downloaded:
                logger.info("Already downloaded video with id: " + item["id"])
                continue
            file_name = self._format_file_name(item["createTime"], item["id"])
            file_path = os.path.join(download_path, file_name)
            logger.info("Downloading video: " + file_name + " (" + str(index + 1) + "/" + str(len(self.videos)) + ")")
            video_url = self._format_video_url(item)
            success = self.download_video(file_path, video_url, item["createTime"])
            if success:
                successful_downloads.append(video_url)
            else:
                failed_downloads.append(video_url)
        logger.info("Processed all {} videos".format(self.video_count))
        logger.debug("Fallback counter: " + str(self.fallback_counter))
        logger.debug("YouTube-dl DownloadError counter: " + str(self.fallback_counter))
        logger.debug("Other error counter: " + str(self.other_error_counter))
        return {"successful_downloads": successful_downloads,
                "failed_downloads": failed_downloads,
                "skipped_downloads": already_downloaded}


if __name__ == '__main__':
    # Define launch arguments
    parser = argparse.ArgumentParser(
        description='''A TikTok ripper based on TikTokApi and YouTube-dl''',
        epilog="""Have fun!""")
    user_arg = "user"
    download_dir_arg = "download_dir"
    skip_existing_arg = "skip_existing"
    timezone_arg = "timezone"
    delay_min_arg = "delay_min"
    delay_max_arg = "delay_max"
    parser.add_argument(user_arg, type=str,
                        help="Target username to rip")
    parser.add_argument("--" + download_dir_arg, type=str, required=False, default="_rips",
                        help="Path to the directory where videos should be downloaded")
    parser.add_argument("--" + skip_existing_arg, type=bool, required=False, default=True,
                        help="Skip videos which are already in the download directory")
    parser.add_argument("--" + timezone_arg, type=str, required=False, default="UTC",
                        help="Override UTC with another timezone code")
    parser.add_argument("--" + delay_min_arg, type=int, required=False, default=1,
                        help="The minimum sleep delay between downloads (in Seconds)")
    parser.add_argument("--" + delay_max_arg, type=int, required=False, default=3,
                        help="The maximum sleep delay between downloads (in Seconds)")
    logger.debug("Parsing launch arguments")
    args = parser.parse_args()
    user: str = args.__dict__[user_arg]
    download_dir = os.path.join(args.__dict__[download_dir_arg])
    skip_existing: bool = args.__dict__[skip_existing_arg]
    timezone: str = args.__dict__[timezone_arg]
    delay_min: str = args.__dict__[delay_min_arg]
    delay_max: str = args.__dict__[delay_max_arg]
    # Configure logging to the console and to a file
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    if not os.path.exists("logs"):
        os.makedirs("logs")
    file_handler = logging.FileHandler(
        "logs/RipTok_log_" + _format_timestamp_iso(pytz.timezone(timezone), datetime.now().timestamp()) + ".log")
    file_formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)  # Increased logging detail in the file
    logger.addHandler(file_handler)
    # Log all the launch arguments to aid with debugging
    logger.debug(user_arg + ": " + user)
    logger.debug(download_dir_arg + ": " + download_dir)
    logger.debug(skip_existing_arg + ": " + str(skip_existing))
    logger.debug(timezone_arg + ": " + timezone)
    logger.debug(delay_min_arg + ": " + str(delay_min))
    logger.debug(delay_max_arg + ": " + str(delay_max))
    if user.startswith("@"):  # handle @username format
        user = user.replace("@", "", 1)
        logger.debug("Stripped @ from " + user)
    logger.info("Starting rip of TikTok user @" + user + " to " + download_dir)
    rip = Ripper(user, download_dir, skip_existing, timezone, delay_min, delay_max)
    logger.info(str(rip))
    result = rip.download_all()
    logger.info("Downloaded " + str(len(result["successful_downloads"])) + "/" + str(rip.video_count) +
                " videos. " + str(len(result["failed_downloads"])) + " failed, and " +
                str(len(result["skipped_downloads"])) + " were already downloaded.")
