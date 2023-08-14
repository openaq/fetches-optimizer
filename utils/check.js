'use strict';
// Simply utility to check if a file was gzipped

import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda"

const s3 = new S3Client()
const lambda = new LambdaClient()
const Bucket = "openaq-fetches"

const dates = ["2023-08-09", "2023-08-10", "2023-08-11"]
const upload = []

for(let i=0;i<dates.length;i++) {

  let d = dates[i]

  let existing = await s3.send(new ListObjectsV2Command({
    Bucket: Bucket,
    MaxKeys: 1000,
    Prefix: `realtime-gzipped/${d}`
  })).then( r => {
    return r.Contents.map(f => f.Key)
  })

  let keys = await s3.send(new ListObjectsV2Command({
    Bucket: Bucket,
    MaxKeys: 1000,
    Prefix: `realtime/${d}`
  })).then( r => {
    return r.Contents.map(f => f.Key)
  })

  keys.map( k => {
    let k2 = `${k.replace("realtime","realtime-gzipped")}.gz`
    if(!existing.includes(k2)) {
      upload.push(k)
    }
  })

}

console.log(`Uploading ${upload.length} files`)

upload.map( key => {
  console.log(`Invoking for ${key}`)
  let job = new InvokeCommand({
    FunctionName: "fetches-optimizer",
    Payload: JSON.stringify({
      sourceBucket: Bucket,
      sourceKey: key
    }),
  })
  lambda.send(job)
    .then( res => {
      console.log(res)
    })
})
