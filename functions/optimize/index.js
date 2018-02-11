'use strict';

const zlib = require('zlib');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();

console.info('Starting optimize function');

exports.handle = function (e, ctx, cb) {
  // Determine input parameters from SNS event or CLI payload
  let sourceKey = e['sourceKey'];
  let sourceBucket = e['sourceBucket'];
  const sourceKeySpace = e['sourceKeySpace'] || 'realtime';
  const destKeySpace = e['destKeySpace'] || 'realtime-gzipped';
  if (sourceKey === undefined || sourceBucket === undefined) {
    const record = JSON.parse(e['Records'][0]['Sns']['Message'])['Records'][0];
    sourceBucket = record['s3']['bucket']['name'];
    sourceKey = record['s3']['object']['key'];
  }

  console.info(`Optimizing object: ${sourceBucket}/${sourceKey}`);

  console.info(`Getting object: ${sourceBucket}/${sourceKey}`);
  const getParams = {
    Bucket: sourceBucket,
    Key: sourceKey
  };
  s3.getObject(getParams, (err, data) => {
    if (err) {
      return cb(err);
    }

    console.info(`Zipping object: ${sourceBucket}/${sourceKey}`);
    zlib.gzip(data.Body, (err, buffer) => {
      if (err) {
        return cb(err);
      }

      // Write to same bucket but new key space
      const destKey = `${sourceKey.replace(sourceKeySpace, destKeySpace)}.gz`;
      const writeParams = {
        Bucket: sourceBucket,
        Key: destKey,
        Body: buffer
      };

      console.info(`Writing object: ${sourceBucket}/${destKey}`);
      s3.putObject(writeParams, (err, data) => {
        if (err) {
          return cb(err);
        }

        cb(null, { success: true });
      });
    });
  });
};
