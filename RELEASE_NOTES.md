#### 0.1.0 - Jan 01, 2014
* Initial release

#### 0.2.0-beta - Mar 10, 2014
Breaking changes:
- record processor processes batch at a time
- record processor needs to respond with status and whether or not to place checkpoint after each batch
- kinesis app takes in a processor factory instead of processor instance

Bug fixes:
- fixed bug with wrong type of exception being checked in DynamoDBUtils.createTable
- fixed bug where ShardId attribute is assumed to be always present even when there's no data in the table
- handle exceptions other than ResourceNotFound during init state table phase

Minor:
- added stubs for dynamodb and kinesis
- added unit tests

#### 0.2.0 - Sep 30, 2014
Breaking changes:
- record processor processes batch at a time
- record processor needs to respond with status and whether or not to place checkpoint after each batch
- kinesis app takes in a processor factory instead of processor instance

Bug fixes:
- fixed bug with wrong type of exception being checked in DynamoDBUtils.createTable
- fixed bug where ShardId attribute is assumed to be always present even when there's no data in the table
- handle exceptions other than ResourceNotFound during init state table phase

Minor:
- added stubs for dynamodb and kinesis
- added unit tests
- updated log4net and AWSSDK dependencies to latest