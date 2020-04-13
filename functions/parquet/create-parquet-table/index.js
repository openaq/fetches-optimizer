'use strict';

const aws = require('aws-sdk');
const AthenaExpress = require('athena-express')

const fetchesDatabase = process.env.DATABASE;
const s3BucketLocation = process.env.S3_BUCKET_LOCATION

const athenaExpressConfig = {
    aws,
    db: fetchesDatabase,
    s3: s3BucketLocation,
    retry: 30000, // wait 30 seconds before re-checking query status
    getStats: true,
    skipResults: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

console.info('Starting create parquet table function');

exports.handler = async function (e, ctx, cb) {

    const fetchesTable = process.env.FETCHES_TABLE
    const parquetTable = process.env.PARQUET_TABLE

    //TODO: Add trigger for weekly run

    //TODO: Delete table and folder before re-creating it

    const ctasQuery =
        `CREATE TABLE ${parquetTable} 
        WITH (format = 'PARQUET', partitioned_by = ARRAY['parameter']) 
        AS SELECT date.utc as date_utc, date.local as date_local, location, country, value, unit, city, attribution, averagingperiod, coordinates, sourcename, sourcetype, mobile, parameter 
        FROM ${fetchesTable};`

    try {
        console.info("Starting to run ctas query")
        let results = await athenaExpress.query(ctasQuery);
        return cb(null, results)
    }
    catch (error) {
        return cb(error)
    }
}