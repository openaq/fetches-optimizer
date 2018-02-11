/*
 * Script to help compare gzipped and non-zipped key spaces on S3
 *
 * Usage: node utils/compare.js
 *
 * Expects data to be in files like
 *
 * 2018-02-10/1518286521.ndjson.gz
 * 2018-02-10/1518287191.ndjson.gz
 * 2018-02-10/1518293192.ndjson.gz
 * ...
 *
 * Will output a file called missing.txt that'll contain missing manifest
 *
 */
'use strict';

const fs = require('fs');

// Read in original files
const original = fs.readFileSync('./manifest.txt', {encoding: 'utf-8'});
const zipped = fs.readFileSync('./manifest_gzip.txt', {encoding: 'utf-8'});

// Stick original in an object
let baseObj = {};
original.split('\n').forEach((m) => {
  baseObj[m] = false;
});

// Loop over zipped and mark as present
zipped.split('\n').forEach((m) => {
  const objName = m.replace('.gz', '');
  baseObj[objName] = true;
});

// Collect all the false items
let missing = [];
for (let m in baseObj) {
  if (baseObj[m] === false) {
    missing.push(`realtime/${m}`);
  }
}

// Output to file
console.info(`There are ${missing.length} missing objects, wrote to missing.txt`);
fs.writeFileSync('./missing.txt', missing.join('\n'));