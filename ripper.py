from TikTokApi import TikTokApi
import youtube_dl
import pytz
from datetime import datetime
import os
from numpy import random
from time import sleep
import argparse

"""
TODO:
maybe externalise things like download path, sleeping time, timezone, 
overwrite function
!!! logging !!!
"""

parser = argparse.ArgumentParser(
	description='''TikTok Ripper based on TikTokApi.''',
	epilog="""Have fun!""")
# parser.add_argument('--user', type=str, default="test", help='Here be username (without @)')
parser.add_argument('user', type=str, default="test", help='Here be username (without @)')
args = parser.parse_args()


class RipTikTok:
	def __init__(self, username):
		self.api = TikTokApi()
		self.username = username
		self.video_count = self.api.get_user(self.username)["userInfo"]["stats"]["videoCount"]
		self.user_tiktoks = self.api.by_username(self.username, count=self.video_count)
		self.tz = pytz.timezone('UTC')
		self.fallback_counter = 0
		self.error404_counter = 0
		print("init complete")

	@staticmethod
	def save_video(file_name, video_bytes):
		with open(file_name, 'wb') as f:
			f.write(video_bytes)

	@staticmethod
	def save_with_ytdl(file_path, video_url):
		ydl_opts = {'outtmpl': file_path}
		with youtube_dl.YoutubeDL(ydl_opts) as ydl:
			ydl.download([video_url])

	@staticmethod
	def video_url(tiktok_object):
		return "https://www.tiktok.com/@{}/video/{}?lang=en".format(tiktok_object['author']['uniqueId'],
																	tiktok_object['id'])

	def isoformat(self, timestamp):
		t = int(timestamp)
		return datetime.fromtimestamp(t, self.tz).isoformat()[:-6].replace(':', '_')

	def verify_download(self, file_path, video_url):
		try:
			if os.stat(file_path).st_size < 1024:  # if file malformed (too small) delete file and use ytdl
				print("Verification failed. Falling back to Youtube-dl!")
				os.remove(file_path)
				self.fallback_counter += 1
				self.save_with_ytdl(file_path, video_url)
				print()
		except OSError:
			print("I'm not sure what is failing here, but meh")
			if not os.path.isfile(file_path):
				try:
					print("Oh, ok, that was that!")
					self.save_with_ytdl(file_path, video_url)
				except youtube_dl.utils.DownloadError:
					print("Error 404 - not found")
					self.error404_counter += 1
					pass
			pass

	def download_all(self):
		download_path = os.path.join("_rips", self.username)
		if not os.path.isdir(download_path):
			os.makedirs(download_path)
		for index, item in enumerate(self.user_tiktoks):
			print("Downloading video:", index + 1, "/", str(len(self.user_tiktoks)))

			file_name = "{}_{}.mp4".format(str(self.isoformat(item["createTime"])), item['id'])
			print(file_name)

			file_path = os.path.join(download_path, file_name)
			video_url = self.video_url(item)
			try:
				if not os.path.isfile(file_path):  # checks if file exist already by checking standard name
					video_bytes = self.api.get_Video_By_Url(video_url)
					self.save_video(file_path, video_bytes)
					os.utime(file_path, (item["createTime"], item["createTime"]))

					print("Done!\n")

					sleeptime = random.uniform(1, 3)
					print("Sleeping for:", sleeptime, "seconds\n")
					sleep(sleeptime)
				else:
					print("File already exist!\n")

			except Exception:
				print("-----------Video download failed\n")
				try:
					self.verify_download(file_path, video_url)  # last chance to download
				except Exception:
					print("Not even ytdl will save you, muhahahaha")

			self.verify_download(file_path, video_url)  # checking if file is good

		print("\nAll {} videos downloaded!".format(self.video_count))
		print("Fallback counter:", self.fallback_counter)
		print("Error 404 counter:", self.error404_counter)

	def debug_print(self):
		print("Username: " + str(self.username))
		print("Videos count in metadata: " + str(self.video_count))
		print("Videos IDs found: " + str(len(self.user_tiktoks)))
		# print(str(self.user_tiktoks[0]))
		print("----------------------------------------------\n")


if __name__ == '__main__':
	print("Ripper starting!")
	user = args.__dict__["user"]  # username only without "@"
	rip = RipTikTok(user)
	rip.debug_print()
	rip.download_all()
