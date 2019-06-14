import os
import boto3
from os import path

SQS_URL = 'https://sqs.us-east-1.amazonaws.com/542921843245/page_segments_in'
SQS_REGION = 'us-east-1'
DST_BUCKET = "prodsdsimg"
SRC_EXT = ".pdf"
DST_EXT = ".png"
TMP_DIR = "/tmp"
VERSION = 2
MADE_TIME = '06-14 09:27 AM'

s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name=SQS_REGION)

def handler(event, context):

  # Read options from the event.
  print("<<< <<< <<< Starting to process S3 Add Event (Version: {}, Made Time: {})".format(VERSION, MADE_TIME))
  print(event)

  try:
    srcBucket = event['Records'][0]['s3']['bucket']['name']
    srcKey = event['Records'][0]['s3']['object']['key']
  except Exception as e:
    print("Failed reading options. {} >>> >>> >>>".format(e))
    return

  print("Reading {}/{}".format(srcBucket, srcKey))

  # Checking the extension
  if not srcKey.endswith(SRC_EXT):
    print('Skipping non-pdf {} >>> >>> >>>'.format(srcKey))
    return

  # dst bucket
  dstBucket = DST_BUCKET
  dstKey = srcKey.replace(SRC_EXT, '')

  # Sanity check: validate that source and destination are different buckets.
  # if (srcBucket == dstBucket) :
  #   print("Destination bucket must not match source bucket >>> >>> >>>")
  #   return

  # Grabs the source file
  try:
    obj = s3.Object(
        bucket_name=srcBucket,
        key=srcKey,
    )
    obj_body = obj.get()['Body'].read()
  except Exception as e:
    print("Failed downloading source file. {} >>> >>> >>>".format(e))
    return

  try:
    pdf_file = srcKey.replace('/', '-')
    src_name = pdf_file.replace(SRC_EXT, '')
    file = open(TMP_DIR + '/' + pdf_file, 'wb')
    file.write(obj_body)
    file.close()
  except Exception as e:
    print("Failed writing source file. {} : {} >>> >>> >>>".format(e, TMP_DIR + '/' + pdf_file))
    return

  print("Start to convert {}".format(srcKey))
  try:
    # Converting PDF to Image
    rlt = os.popen('ls ' + TMP_DIR).readlines()
    print(rlt)

    rlt = os.popen('./convert --version').readlines()
    print(rlt)

    png_file = pdf_file.replace(SRC_EXT, DST_EXT)

    exists = os.path.isfile(TMP_DIR + '/' + pdf_file)
    if exists:
      #rlt = os.popen("convert -density 225 -background white -alpha remove -resize 1000x4000 {}/{} {}/{}".format(TMP_DIR, pdf_file, TMP_DIR, png_file)).readlines()
      rlt = os.popen("convert -colorspace RGB -density 300 -quality 100 {}/{} {}/{}".format(TMP_DIR, pdf_file, TMP_DIR, png_file)).readlines()
      print(rlt)
    else:
      print("Not found file={}/{} >>> >>> >>>".format(TMP_DIR, pdf_file))
      return

    rlt = os.popen('ls ' + TMP_DIR).readlines()
    print(rlt)

    os.remove(TMP_DIR + '/' + pdf_file)
  except Exception as e:
    print("Failed converting. {} >>> >>> >>>".format(e))
    return

  sqs_msg_imgs = []

  for file in os.listdir(TMP_DIR):
    if file.startswith(src_name) and file.endswith(DST_EXT):
      try:
        img_file = open(TMP_DIR + '/' + file, 'rb')
        buffer = img_file.read()
        os.remove(TMP_DIR + '/' + file)
        img_file.close()
      except Exception as e:
        print("Failed reading {} file={}".format(e, TMP_DIR + '/' + file))

      try:
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

        img = {
          "bucket": DST_BUCKET,
          "key": "test_images/" + img_name
        };
        sqs_msg_imgs.append(img)
      except Exception as e:
        print("Failed uploading {} file={}".format(e, file))
  # Send message to SQS queue
  msg = {
    "images": sqs_msg_imgs
  }
  msg_str = json.dumps(msg)
  print("Start to send the SQS message {}".format(msg_str))
  try:
    response = sqs.send_message(
        QueueUrl=SQS_URL,
        DelaySeconds=10,
        MessageBody=(
          msg_str
        )
    )
  except Exception as e:
    print("Failed sending message {} message={}".format(e, msg_str))
  print("Done all action >>> >>> >>>")