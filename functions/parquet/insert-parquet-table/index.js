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
    const tempTable = process.env.TEMP_TABLE

    //TODO: Get list of files in temp location in bucket

    const insertIntoQuery =
        `INSERT INTO ${parquetTable} 
        SELECT date.utc AS date_utc, date.local AS date_local, location, country, value, unit, city, attribution, averagingperiod, coordinates, sourcename, sourcetype, mobile, parameter
        FROM ${tempTable};`

    //TODO: Delete files from temp location in bucket

    try {
        console.info("Starting to run insert_into query")
        let results = await athenaExpress.query(insertIntoQuery)
        return cb(null, results)
    }
    catch (error) {
        return cb(error)
    }
}