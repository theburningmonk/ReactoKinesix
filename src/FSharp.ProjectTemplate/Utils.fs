namespace ReactoKinesix.Utils

open System

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open ReactoKinesix.Model

module internal Helpers =
    let x = 42

[<AutoOpen>]
module internal DynamoDBHelpers =
    let private shardIdAttr, lastHeartbeatAttr, workerIdAttr = "ShardId", "LastHeartbeat", "WorkerId"
    let private dateTimeFormat = "yyyy-MM-dd HH:mm:ss.fffffff"

    /// Returns the list of tables that currently exist in DynamoDB
    let getTables (dynamoDB : IAmazonDynamoDB) =
        let req = new ListTablesRequest()
        dynamoDB.ListTables(req).TableNames

    /// Initializes the application state table if necessary and returns the table name
    let initStateTable appName (config : ReactoKinesixConfig) (dynamoDB : IAmazonDynamoDB) = 
        let appTableName = sprintf "%s%s" appName config.DynamoDBTableSuffix

        match getTables dynamoDB |> Seq.exists ((=) appTableName) with
        | false -> appTableName
        | _     -> 
            let req = new CreateTableRequest(TableName = appTableName)
            req.KeySchema.Add(new KeySchemaElement(AttributeName = shardIdAttr, KeyType = KeyType.HASH))
            req.ProvisionedThroughput.ReadCapacityUnits  <- config.DynamoDBReadThroughput
            req.ProvisionedThroughput.WriteCapacityUnits <- config.DynamoDBWriteThroughput
        
            // TODO : handle exception when table already exists
            let res = dynamoDB.CreateTable(req)

            res.TableDescription.TableName

    /// Puts a shard into the shard conditionally against the worker ID so that if another worker has
    /// already added the shard then we don't proceed
    let createShard tableName shardId workerId (dynamoDB : IAmazonDynamoDB) =
        let req = new PutItemRequest(TableName = tableName)
        req.Item.Add(shardIdAttr, new AttributeValue(S = shardId))
        req.Item.Add(workerIdAttr, new AttributeValue(S = workerId))
        req.Item.Add(lastHeartbeatAttr, new AttributeValue(S = DateTime.UtcNow.ToString(dateTimeFormat)))
        
        req.Expected.Add(workerIdAttr, new ExpectedAttributeValue(Exists = false))

        dynamoDB.PutItem(req) |> ignore

    /// Updates the heartbeat value for the specified shard conditionally against the worker ID
    /// so that if for some reason another worker has taken over this shard then we shall stop
    /// processing this shard
    let updateHeartbeat tableName shardId workerId (dynamoDB : IAmazonDynamoDB) =
        let req = new UpdateItemRequest(TableName = tableName)
        req.Key.Add(shardIdAttr, new AttributeValue(S = shardId))
        req.Key.Add(lastHeartbeatAttr, new AttributeValue(S = DateTime.UtcNow.ToString(dateTimeFormat)))
        
        req.Expected.Add(workerIdAttr, new ExpectedAttributeValue(Value = new AttributeValue(S = workerId), Exists = true))

        dynamoDB.UpdateItem(req) |> ignore