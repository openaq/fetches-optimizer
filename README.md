# fetches-optimizer

This is a simple piece of code intended to be run as a Lambda function when new data is inserted into `s3://openaq-fetches/realtime`. Currently, it's purpose is to gzip new objects and save them to `s3://openaq-fetches/realtime-gzipped`. This will make aggregations (via Athena) quicker and less expensive.

## Running in batch

In addition to being triggered by SNS when new data is added to `s3://openaq-fetches/realtime`, the code can also be run in batch by creating a manifest file.

If you have a file of the form

```
realtime/2018-02-10/1518286521.ndjson.gz
realtime/2018-02-10/1518287191.ndjson.gz
realtime/2018-02-10/1518293192.ndjson.gz
...
```

you can invoke the Lambda function via the AWS CLI like

```
<manifest.txt xargs -n1 -P 10 -I % aws lambda invoke --function-name fetches-optimizer_optimize:current --payload '{"sourceBucket":"openaq-fetches", "sourceKey":"'%'"}' --region us-east-1 --invocation-type Event -
```

This will spin up a whole bunch of Lambda functions based on the input manifest file.