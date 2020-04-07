'use strict';

const AWS = require('aws-sdk');
const athena = new AWS.Athena();

console.info('Starting create parquet table function');

exports.handler = function (e, ctx, cb) {
    let fetchesDatabase = process.env.DATABASE
    let fetchesTable = process.env.FETCHES_TABLE
    let parquetTable = process.env.PARQUET_TABLE
    let s3BucketLocation = process.env.S3_BUCKET_LOCATION

    //TODO: Add trigger for weekly run

    //TODO: Delete table and folder before re-creating it

    console.info("Starting to run ctas query")
    const params = {
        QueryString:
            `CREATE TABLE ${parquetTable} 
            WITH (format = 'PARQUET', partitioned_by = ARRAY['parameter']) 
            AS SELECT date.utc as date_utc, date.local as date_local, location, country, value, unit, city, attribution, averagingperiod, coordinates, sourcename, sourcetype, mobile, parameter 
            FROM ${fetchesTable};`,
        QueryExecutionContext: {
            Database: `${fetchesDatabase}`
        },
        ResultConfiguration: {
            OutputLocation: `${s3BucketLocation}`
        }
    }
    athena.startQueryExecution(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        //TODO: Proper error handling
        else console.log("successful", data); // successful response to start the query
    });
}