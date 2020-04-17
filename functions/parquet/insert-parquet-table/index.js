'use strict';

const aws = require('aws-sdk');
const AthenaExpress = require('athena-express')
const s3 = new aws.S3()

const fetchesDatabase = process.env.DATABASE;
const s3Bucket = process.env.S3_BUCKET
const s3ParquetPrefix = process.env.S3_PARQUET_PREFIX

const athenaExpressConfig = {
    aws,
    db: fetchesDatabase,
    s3: `s3://${s3Bucket}/${s3ParquetPrefix}`,
    getStats: true,
    skipResults: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

console.info('Starting insert parquet table function');

exports.handler = async function (e, ctx, cb) {

    const s3TempPrefix = process.env.S3_TEMP_PREFIX
    const parquetTable = process.env.PARQUET_TABLE
    const tempTable = process.env.TEMP_TABLE

    const listBucketParams = {
        Bucket: s3Bucket,
        Prefix: s3TempPrefix
    }

    const insertIntoQuery =
        `INSERT INTO ${parquetTable} 
        SELECT date.utc AS date_utc, date.local AS date_local, location, country, value, unit, city, attribution, averagingperiod, coordinates, sourcename, sourcetype, mobile, parameter
        FROM ${tempTable};`

    try {
        let fileObjects = await s3.listObjectsV2(listBucketParams).promise()
        let fileKeys = fileObjects.Contents.map(object => object.Key)
        console.info(`Files found: ${fileKeys}`)

        console.info("Starting to run insert_into query")
        let results = await athenaExpress.query(insertIntoQuery)

        let objectKeys = fileKeys.map(key => { return { Key: key } })
        const deleteBucketParams = {
            Bucket: s3Bucket,
            Delete: {
                Objects: objectKeys,
                Quiet: true
            },
        }
        console.info(`Deleting files: ${fileKeys}`)
        let deletedObjects = await s3.deleteObjects(deleteBucketParams).promise()

        return cb(null, { queryResults: results, failedObjects: deletedObjects })
    }
    catch (error) {
        return cb(error)
    }
}