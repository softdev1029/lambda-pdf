import os
import boto3
from os import path

s3 = boto3.resource('s3')

DST_BUCKET = "prodsdsimg"
SRC_EXT = ".pdf"
TMP_FILE = "/tmp/1.pdf"

def handler(event, context):
  # Read options from the event.
  print("Reading options from event")

  srcBucket = event['Records'][0]['s3']['bucket']['name']
  srcKey = event['Records'][0]['s3']['object']['key']

  print("Read... {}/{}".format(srcBucket, srcKey))

  # dst bucket
  dstBucket = srcBucket#DST_BUCKET
  dstKey = srcKey.replace(SRC_EXT, '')

  # Sanity check: validate that source and destination are different buckets.
  # if (srcBucket == dstBucket) :
  #   print("Destination bucket must not match source bucket.")
  #   return

  # Infer the type.
  type = path.splitext(srcKey)[1].lower()

  # Grabs the source file
  obj = s3.Object(
      bucket_name=srcBucket,
      key=srcKey,
  )
  obj_body = obj.get()['Body'].read()
  file = open('/tmp/1.pdf', 'w')
  file.write(obj_body)
  file.close()

  # Checking the extension and
  # Defining the buffer format
  if type not in ['.pdf']:
    print('skipping non-pdf ' + srcKey)

  # Converting PDF to Image
  rlt = os.popen('ls /tmp').readlines()
  print(rlt)

  rlt = os.popen("convert /tmp/1.pdf -density 225 -background white -alpha remove -resize 1000x4000 /tmp/1.png").readlines()
  print(rlt)

  rlt = os.popen('ls /tmp').readlines()
  print(rlt)

  index = 0
  for file in os.listdir("/tmp"):
    if file.startswith('1-') and file.endswith(".png"):
      img_file = open('/tmp/' + file, 'rb')
      buffer = img_file.read()
      img_file.close()

      img_name = "{}-{}.png".format(dstKey, index)
      # Uploading the image
      obj = s3.Object(
        bucket_name=dstBucket,
        key=img_name,
      )
      obj.put(Body=buffer)

      print("Uploaded... {}/{}".format(dstBucket, img_name))

      index += 1