namespace ReactoKinesix.Tests

open System
open System.Collections.Generic
open System.Linq

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

[<AutoOpen>]
module ModelExtensions =
    type ProvisionedThroughput with
        member this.ToDescription () =
            new ProvisionedThroughputDescription(ReadCapacityUnits = this.ReadCapacityUnits,        
                                                 WriteCapacityUnits = this.WriteCapacityUnits)

type Key =
    | Hash          of string           // hash key value
    | HashAndRange  of string * string  // range key value

type KeyType =
    | Hash          of string           // hash key attribute name
    | HashAndRange  of string * string  // hash and range key attribute name

type DynamoDBItem (keyType : KeyType, req : PutItemRequest) =
    let key = 
        match keyType with
        | KeyType.Hash hkeyName 
            -> Key.Hash req.Item.[hkeyName].S
        | KeyType.HashAndRange (hkeyName, rkeyName) 
            -> Key.HashAndRange(req.Item.[hkeyName].S, req.Item.[rkeyName].S)

    let attributes = req.Item

    member this.Key        = key
    member this.Attributes = attributes

    member this.Update (req : UpdateItemRequest) =
        req.AttributeUpdates
        |> Seq.iter (fun (KeyValue(key, update)) -> 
            match update.Action.Value with
            | "ADD" | "PUT" -> attributes.[key] <- update.Value
            | "DELETE" -> attributes.Remove key |> ignore)

type DynamoDBTable (req : CreateTableRequest) =
    let created = DateTime.UtcNow

    let (|HashKeyElem|RangeKeyElem|) (elem : KeySchemaElement) =
        match elem.KeyType.Value with
        | "HASH"  -> HashKeyElem elem.AttributeName
        | "RANGE" -> RangeKeyElem elem.AttributeName

    let keyType = match req.KeySchema.ToArray() with
                  | [| HashKeyElem attrName |] 
                    -> KeyType.Hash attrName
                  | [| HashKeyElem hkeyName; RangeKeyElem rkeyName |]
                  | [| RangeKeyElem rkeyName; HashKeyElem hkeyName |] 
                    -> KeyType.HashAndRange (hkeyName, rkeyName)

    let getKey (attributes : Dictionary<string, AttributeValue>) = 
        match keyType with
        | KeyType.Hash hkeyName 
            -> Key.Hash (attributes.[hkeyName].S)
        | KeyType.HashAndRange (hkeyName, rkeyName) 
            -> Key.HashAndRange (attributes.[hkeyName].S, attributes.[rkeyName].S)

    let items         = new Dictionary<Key, DynamoDBItem>()
    let conditionalCheck (item : DynamoDBItem) (expected : Dictionary<string, ExpectedAttributeValue>) =
        expected
        |> Seq.iter (fun (KeyValue(attr, expectedVal)) ->
            match expectedVal.Exists, item.Attributes.TryGetValue attr with
            | false, (true, _) -> 
                raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ConditionalCheckFailedException>()
            | true, (false, _) ->
                raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ConditionalCheckFailedException>()
            | true, (true, attrVal) when expectedVal.Value <> attrVal ->
                raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ConditionalCheckFailedException>()
            | _ -> ())

    member this.Items = items
    member val Status = TableStatus.CREATING with get, set

    member this.GetItem (req : GetItemRequest) =
        let key = getKey req.Key

        if not <| items.ContainsKey key then
            raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceNotFoundException>()

        let item = items.[key]
        req.AttributesToGet 
        |> Seq.append (req.Key.Keys)
        |> Seq.distinct
        |> Seq.choose (fun attr -> match item.Attributes.TryGetValue attr with
                                   | true, attrValue -> Some(attr, attrValue)
                                   | _ -> None)
        |> Seq.toDict

    member this.PutItem (req : PutItemRequest) =
        let item = new DynamoDBItem(keyType, req)
        if items.ContainsKey item.Key then
            conditionalCheck items.[item.Key] req.Expected

        items.[item.Key] <- item

    member this.UpdateItem (req : UpdateItemRequest) =
        let key = getKey req.Key
        
        if not <| items.ContainsKey key then
            raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceNotFoundException>()

        let item = items.[key]        
        conditionalCheck item req.Expected
        item.Update req

    member this.DeleteItem (req : DeleteItemRequest) =
        let key = getKey req.Key
        
        if not <| items.ContainsKey key then
            raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceNotFoundException>()

        let item = items.[key]        
        conditionalCheck item req.Expected

        items.Remove(key) |> ignore

    member this.TableDescription = 
        let desc = new TableDescription()
        desc.AttributeDefinitions   <- req.AttributeDefinitions
        desc.CreationDateTime       <- created
        desc.ItemCount              <- int64 items.Count
        desc.KeySchema              <- req.KeySchema
        desc.ProvisionedThroughput  <- req.ProvisionedThroughput.ToDescription()
        desc.TableName              <- req.TableName
        desc.TableStatus            <- this.Status

        desc

type DynamoDBStub () =
    let tables = new Dictionary<string, DynamoDBTable>()

    let getTable tableName =
        if not <| tables.ContainsKey tableName then 
            raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceNotFoundException>()

        tables.[tableName]

    member this.Tables = tables

    interface IAmazonDynamoDB with
        // #region BatchGetItem

        member this.BatchGetItem (_items : Dictionary<string, KeysAndAttributes>) : BatchGetItemResponse = 
            raise <| NotImplementedException()

        member this.BatchGetItem (_items : Dictionary<string, KeysAndAttributes>, _returnCapacity) : BatchGetItemResponse = 
            raise <| NotImplementedException()

        member this.BatchGetItem (_req : BatchGetItemRequest) : BatchGetItemResponse = 
            raise <| NotImplementedException()

        member this.BatchGetItemAsync (_req, _) = 
            raise <| NotImplementedException()

        // #endregion

        // #region BatchWriteItem

        member this.BatchWriteItem (_req : Dictionary<string, List<WriteRequest>>) : BatchWriteItemResponse = 
            raise <| NotImplementedException()

        member this.BatchWriteItem (_req : BatchWriteItemRequest) : BatchWriteItemResponse = 
            raise <| NotImplementedException()

        member this.BatchWriteItemAsync (_req, _) = 
            raise <| NotImplementedException()

        // #endregion

        //#region CreateTable

        member this.CreateTable (_tableName, _schema, _attrDefs, _throughput) = raise <| NotImplementedException()

        member this.CreateTable (req) =
            if tables.ContainsKey req.TableName then 
                raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceInUseException>()
            
            let table = new DynamoDBTable(req)
            tables.Add(req.TableName, table)

            new CreateTableResponse(TableDescription = table.TableDescription)

        member this.CreateTableAsync (req, _) =
            async { return (this :> IAmazonDynamoDB).CreateTable(req) } |> Async.StartAsTask

        //#endregion

        //#region DeleteItem

        member this.DeleteItem (_tableName, key)              = raise <| NotImplementedException()
        member this.DeleteItem (_tableName, _key, _returnVal) = raise <| NotImplementedException()

        member this.DeleteItem (req : DeleteItemRequest) =
            let table = getTable req.TableName
            table.DeleteItem req

            new DeleteItemResponse()

        member this.DeleteItemAsync (req, _) = 
            async { return (this :> IAmazonDynamoDB).DeleteItem req } |> Async.StartAsTask

        //#endregion

        //#region DeleteTable

        member this.DeleteTable (tableName : string) =
            let req = new DeleteTableRequest(TableName = tableName)
            (this :> IAmazonDynamoDB).DeleteTable(req)

        member this.DeleteTable (req : DeleteTableRequest) =
            let table = getTable req.TableName
            if table.Status = TableStatus.CREATING || table.Status = TableStatus.UPDATING then 
                raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceInUseException>()
            
            table.Status <- TableStatus.DELETING
            tables.Remove(req.TableName) |> ignore

            new DeleteTableResponse(TableDescription = table.TableDescription)
        
        member this.DeleteTableAsync (req, _) =
            async { return (this :> IAmazonDynamoDB).DeleteTable(req) } |> Async.StartAsTask

        //#endregion

        //#region DescribeTable

        member this.DescribeTable tableName =
            let table = getTable tableName            
            new DescribeTableResponse(Table = table.TableDescription)

        member this.DescribeTable (req : DescribeTableRequest) =
            let table = getTable req.TableName
            new DescribeTableResponse(Table = table.TableDescription)

        member this.DescribeTableAsync (req, _) =
            async { return (this :> IAmazonDynamoDB).DescribeTable(req) } |> Async.StartAsTask

        //#endregion

        //#region GetItem
        
        member this.GetItem(_tableName, _attributes) = 
            raise <| NotImplementedException()

        member this.GetItem(_tableName, _attributes, _consistentRead) = 
            raise <| NotImplementedException()

        member this.GetItem req =
            let table = getTable req.TableName
            let item  = table.GetItem req

            new GetItemResponse(Item = item)

        member this.GetItemAsync (req, _) =
            async { return (this :> IAmazonDynamoDB).GetItem req } |> Async.StartAsTask

        //#endregion

        //#region ListTables

        member this.ListTables () =
            new ListTablesResponse(TableNames = (tables.Keys |> Seq.toResizeArray))

        member this.ListTables (limit : int) =
            new ListTablesResponse(TableNames = (tables.Keys |> Seq.take limit |> Seq.toResizeArray))

        member this.ListTables (exclusiveStartTableName : string) =
            let tableNames = tables.Keys 
                             |> Seq.skipWhile ((<>) exclusiveStartTableName) 
                             |> Seq.toResizeArray
            new ListTablesResponse(TableNames = tableNames)

        member this.ListTables (exclusiveStartTableName : string, limit : int) =
            let tableNames = tables.Keys 
                             |> Seq.skipWhile ((<>) exclusiveStartTableName)
                             |> Seq.take limit
                             |> Seq.toResizeArray
            new ListTablesResponse(TableNames = tableNames)

        member this.ListTables (req : ListTablesRequest) = (this :> IAmazonDynamoDB).ListTables()

        member this.ListTablesAsync (req, _) =
            async { return (this :> IAmazonDynamoDB).ListTables() } |> Async.StartAsTask

        //#endregion

        //#region PutItem

        member this.PutItem (_tableName, _item)             = raise <| NotImplementedException()
        member this.PutItem (_tableName, _item, _returnVal) = raise <| NotImplementedException()

        member this.PutItem (req : PutItemRequest) = 
            let table = getTable req.TableName
            table.PutItem req

            new PutItemResponse()

        member this.PutItemAsync (req, _) =
            async { return (this :> IAmazonDynamoDB).PutItem req } |> Async.StartAsTask

        //#endregion
        
        //#region UpdateItem

        member this.UpdateItem (_tableName, _key, _attributeUpdates) =
            raise <| NotImplementedException()

        member this.UpdateItem (_tableName, _key, _attributeUpdates, _returnValue) =
            raise <| NotImplementedException()

        member this.UpdateItem req =
            let table = getTable req.TableName

            let res = new UpdateItemResponse()
            res

        member this.UpdateItemAsync (req, _) = 
            async { return (this :> IAmazonDynamoDB).UpdateItem req } |> Async.StartAsTask

        //#endregion

        member this.Query req                   = raise <| NotImplementedException()
        member this.QueryAsync (req, _)         = raise <| NotImplementedException()

        member this.Scan (_tableName : string, _attributes : List<string>, _scanFilter : Dictionary<string, Condition>) = 
            raise <| NotImplementedException()
        
        member this.Scan (_tableName : string, _scanFilter : Dictionary<string, Condition>) : ScanResponse = 
            raise <| NotImplementedException()

        member this.Scan (_tableName : string, _attributes : List<string>) : ScanResponse = 
            raise <| NotImplementedException()

        member this.Scan (_req : ScanRequest) = 
            raise <| NotImplementedException()

        member this.ScanAsync (_req, _) = 
            raise <| NotImplementedException()

        member this.UpdateTable (_tableName, _throughput) = raise <| NotImplementedException()
        member this.UpdateTable _req           = raise <| NotImplementedException()
        member this.UpdateTableAsync (_req, _) = raise <| NotImplementedException()

        member this.Dispose () = ()