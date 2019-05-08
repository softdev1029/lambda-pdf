import os
import boto3
from os import path

s3 = boto3.resource('s3')

DST_BUCKET = "prodsdsimg"
SRC_EXT = ".pdf"
DST_EXT = ".png"
TMP_DIR = "/tmp"

def handler(event, context):

  # Read options from the event.
  print("Reading options from event")
  print(event)

  try:
    srcBucket = event['tasks'][0]['s3BucketArn']
    srcBucket = srcBucket.replace('arn:aws:s3:::', '')
    srcKey = event['tasks'][0]['s3Key']
  except Exception as e:
    print("Failed reading options. {}".format(e))
    return

  print("Read... {}/{}".format(srcBucket, srcKey))

  # dst bucket
  dstBucket = DST_BUCKET
  dstKey = srcKey.replace(SRC_EXT, '')

  # Sanity check: validate that source and destination are different buckets.
  if (srcBucket == dstBucket) :
    print("Destination bucket must not match source bucket.")
    return

  # Grabs the source file
  try:
    obj = s3.Object(
        bucket_name=srcBucket,
        key=srcKey,
    )
    obj_body = obj.get()['Body'].read()
    pdf_file = srcKey.replace('/', '-')
    src_name = pdf_file.replace(SRC_EXT, '')
    file = open(TMP_DIR + '/' + pdf_file, 'wb')
    file.write(obj_body)
    file.close()
  except Exception as e:
    print("Failed downloading source file. {}".format(e))
    return

  # Checking the extension and
  # Defining the buffer format
  if not srcKey.endswith(SRC_EXT):
    print('skipping non-pdf ' + srcKey)
    return

  try:
    # Converting PDF to Image
    rlt = os.popen('ls ' + TMP_DIR).readlines()
    print(rlt)

    png_file = pdf_file.replace(SRC_EXT, DST_EXT)
    rlt = os.popen("convert {}/{} -density 225 -background white -alpha remove -resize 1000x4000 {}/{}".format(TMP_DIR, pdf_file, TMP_DIR, png_file)).readlines()
    print(rlt)

    rlt = os.popen('ls ' + TMP_DIR).readlines()
    print(rlt)

    os.remove(TMP_DIR + '/' + pdf_file)
  except Exception as e:
    print("Failed converting. {}".format(e))
    return

  for file in os.listdir(TMP_DIR):
    if file.startswith(src_name) and file.endswith(DST_EXT):
      try:
        img_file = open(TMP_DIR + '/' + file, 'rb')
        buffer = img_file.read()
        os.remove(TMP_DIR + '/' + file)
        img_file.close()

        index = file.replace(src_name + '-', '')
        index = index.replace(DST_EXT, '')
        img_name = "{}-{}{}".format(dstKey, index, DST_EXT)
        # Uploading the image
        obj = s3.Object(
          bucket_name=dstBucket,
          key=img_name,
        )
        obj.put(Body=buffer)

        print("Uploaded... {}/{}".format(dstBucket, img_name))
      except Exception as e:
        print("Failed uploading {} file={}".format(e, file))
  print("Done all action")