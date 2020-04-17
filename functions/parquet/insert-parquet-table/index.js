'use strict';

const aws = require('aws-sdk');
const AthenaExpress = require('athena-express')

const fetchesDatabase = process.env.DATABASE;
const s3BucketLocation = process.env.S3_BUCKET_LOCATION

const athenaExpressConfig = {
    aws,
    db: fetchesDatabase,
    s3: s3BucketLocation,
    getStats: true,
    skipResults: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

console.info('Starting insert parquet table function');

exports.handler = async function (e, ctx, cb) {

    const parquetTable = process.env.PARQUET_TABLE
    const tempKeySpace = process.env.TEMP_KEYSPACE

    //TODO: Get list of files in temp location in bucket

    //TODO: Run INSERT_INTO query

    //TODO: Delete files from temp location in bucket

    try {
        return cb(null, {})
    }
    catch (error) {
        return cb(error)
    }
}