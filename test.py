import os
import platform
import sys, time, re
from io import BytesIO
from os import path

DST_BUCKET = "prodsdsimg"
SRC_EXT = ".pdf"
TMP_FILE = "/tmp/1.pdf"

rlt = os.popen('ls /tmp').readlines()
# print(rlt)

rlt = os.popen("convert /tmp/1.pdf -density 225 -background white -alpha remove -resize 1000x4000 /tmp/1.png").readlines()
# print(rlt)

rlt = os.popen('ls /tmp').readlines()
# print(rlt)

index = 0
dstKey = 'a'
for file in os.listdir("/tmp"):
  if file.startswith('1-') and file.endswith(".png"):
    img_file = open('/tmp/' + file, 'rb')
    buffer = img_file.read()
    img_file.close()
    print()

    index += 1