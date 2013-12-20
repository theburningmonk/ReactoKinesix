namespace ReactoKinesix

open Amazon
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

type ReactoKinesix(awsKey     : string, 
                   awsSecret  : string, 
                   region     : RegionEndpoint,
                   appName    : string,
                   streamName : string,
                   ?config    : ReactoKinesixConfig) =
    let client   = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
    let dynamoDB = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region) 
    let config   = defaultArg config <| new ReactoKinesixConfig()




    do ()