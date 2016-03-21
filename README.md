The purpose of this library is to ease running complex batch jobs with PostgreSQL.

Among others, you can parallelize queries over a set of keys (useful for partitions) and load CSV with Linux user permissions.


Usage

var PGBatch = require('pg-batch');
var pgBatch = new PGBatch(config) //config is pgConfig json
pgBatch.runNodeCommand({...})
pgBatch.runFile()

TODO
Improve documentation
Improve code style
Use template strings