// dependencies
const async = require('async');
const AWS = require('aws-sdk');
const fs = require('fs');

// Enable ImageMagick integration.
const gm = require('gm').subClass({ imageMagick: true });
const util = require('util');
const pdf2png = require('pdf2png-mp');

pdf2png.ghostscriptPath = "/usr/bin";

// get reference to S3 client 
const s3 = new AWS.S3();

const DST_BUCKET = "prodsdsimg";
const SRC_EXT = ".pdf";
const TMP_FILE = "/tmp/1.pdf";

exports.handler = function(event, context) {

  // Read options from the event.
  console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
  var srcBucket = event.Records[0].s3.bucket.name;

  // Object key may have spaces or unicode non-ASCII characters.
  var srcKey    =  decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));

  var dstBucket = DST_BUCKET;
  var dstKey    = srcKey.replace(SRC_EXT, '');
  // dstKey = dstKey.replace('pdf', 'img');

  // Sanity check: validate that source and destination are different buckets.
  if (srcBucket == dstBucket) {
    console.error("Destination bucket must not match source bucket.");
    return;
  }

  // Infer the type.
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.error('unable to infer the source file type for key ' + srcKey);
    return;
  }

  var type = typeMatch[1].toLowerCase();
  if (type != "pdf") {
    console.log('skipping non-pdf ' + srcKey);
    return;
  }

  // Download the pdf file from S3, transform, and upload to a different S3 bucket.
  async.waterfall([
      function download(next) {

        // Download the file from S3 into a buffer.
        s3.getObject({
          Bucket: srcBucket,
          Key: srcKey
        },
        next);
      },

      function transform(response, next) {

        console.log(`Start transform, type=${type}`);

        filePath = TMP_FILE;
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
        }
        
        fs.writeFileSync(filePath, response.Body.toString());
        console.log(`${filePath} has been created for ${srcKey}`);
      
        console.log("Start transform PDF to PNG");
        pdf2png.convert(filePath, { quality: 50}, function(resp) {

          if (!resp.success) {
            console.error(`fail to convert pdf to img for ${srcKey}`);

            next(resp.error);

            return;
          }

          console.log("Converted from PDF to PNG, now we are saving PNGs to S3");
          resp.data.forEach(function(item, index) {

            // Stream the transformed image to a different S3 bucket.
            console.log(`Start uploading... index=${index} for ${srcKey}`);

            newKey = dstKey + "-" + index + '.png';

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