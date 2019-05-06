// dependencies
var async = require('async');
var AWS = require('aws-sdk');
const fs = require('fs');

// Enable ImageMagick integration.
var gm = require('gm').subClass({ imageMagick: true });
var util = require('util');
var pdf2png = require('pdf2png-mp');

pdf2png.ghostscriptPath = "/usr/bin";

// constants
var MAX_WIDTH  = 320;
var MAX_HEIGHT = 320;

// get reference to S3 client 
var s3 = new AWS.S3();

exports.handler = function(event, context) {
  // Read options from the event.
  console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
  var srcBucket = event.Records[0].s3.bucket.name;

  // Object key may have spaces or unicode non-ASCII characters.
  var srcKey    =  decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));  
  var isTest = srcBucket.indexOf('-test') >= 0;

  var dstBucket = "prodsdimg";
  var dstKey    = srcKey;

  // Sanity check: validate that source and destination are different buckets.
  if (srcBucket == dstBucket) {
    console.error("Destination bucket must not match source bucket.");
    return;
  }

  // Infer the image type.
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.error('unable to infer image type for key ' + srcKey);
    return;
  }

  var imageType = typeMatch[1].toLowerCase();
  if (imageType != "jpg" && imageType != "png" && imageType != "pdf") {
    console.log('skipping non-image ' + srcKey);
    return;
  }


  // Download the image from S3, transform, and upload to a different S3 bucket.
  async.waterfall([
      function download(next) {

        // Download the image from S3 into a buffer.
        s3.getObject({
          Bucket: srcBucket,
          Key: srcKey
        },
        next);
      },

      function transform(response, next) {

        console.log(`Start transform, type=${imageType}`);

        filePath = "/tmp/1.pdf";
        fs.writeFileSync(filePath, response.Body.toString());
        console.log(`${filePath} has been created!`);
        
        if (imageType == "pdf") {
          console.log("Start transform PDF to PNG");
          pdf2png.convert("/tmp/1.pdf", { quality: 50}, function(resp) {

            console.log('sdfsdfdsfsdfsdfds');
            if (!resp.success) {
              console.error('fail to convert pdf to img');

              next(resp.error);

              return;
            }

            console.log("Yayy the pdf got converted, now I'm gonna save it!");
            resp.data.forEach(function(item, index) {
              //next(null, "image/png", item, index);
              // Stream the transformed image to a different S3 bucket.
              console.log('Start uploading... index=' + index);

              var ext = '';
              if (imageType == "pdf") {
                newKey = dstKey + index + '.png';
                dstBucket = srcBucket;
              }

              console.log('before upload 2:'+newKey);
              s3.putObject({
                Bucket: dstBucket,
                Key: newKey,
                Body: item,
                ContentType: "image/png",
                ACL:'public-read'
              }, (err, data) => {
                if (err) console.error(err);
                console.log(`uploaded ${dstBucket}/${newKey}`);
              });
            });
            //next();
          });
        }
      },

    ], function (err) {

      if (err) {

        console.error(
            'Unable to resize ' + srcBucket + '/' + srcKey +
            ' and upload to ' + dstBucket + '/' + dstKey +
            ' due to an error: ' + err
            );

      } else {

        console.log(
            'Successfully  ' + srcBucket + '/' + srcKey +
            ' and uploaded to ' + dstBucket + '/' + dstKey
          );
      }
      //context.done();
    }
  );
};