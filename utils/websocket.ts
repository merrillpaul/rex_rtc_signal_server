import { dbClient } from './db';
import { ApiGatewayManagementApi } from 'aws-sdk/clients/all';

export class Client {
    private client: ApiGatewayManagementApi;
    constructor(config: any = null) {
        if (config) {
            this.setupClient(config)
        }
    }

    async setupClient(config: any) {
        // fetch config from db if none provided and we do not have a client
        if (typeof config !== 'object' && !this.client) {
            const item = await dbClient.dynamodbClient.get({
                TableName: dbClient.dbConfig.Table,
                Key: {
                    [dbClient.dbConfig.Primary.Key]: 'APPLICATION',
                    [dbClient.dbConfig.Primary.Range]: 'WS_CONFIG'
                }
            }).promise();
            console.log(item)
            config = item.Item;
            config.fromDb = true;
        }

        if (!this.client) {
            if (config.requestContext.apiId) {
                config.requestContext.domainName = `${config.requestContext.apiId}.execute-api.${process.env.API_REGION}.amazonaws.com`
            }

            this.client = new ApiGatewayManagementApi({
                apiVersion: "2018-11-29",
                endpoint: `https://${config.requestContext.domainName}/${config.requestContext.stage}`
            });

            // temporarily we update dynamodb with most recent info
            // after CF support this can go away, we just do this so a single deployment makes this work
            if (config.fromDb !== true) {
                await dbClient.dynamodbClient.put({
                    TableName: dbClient.dbConfig.Table,
                    Item: {
                        [dbClient.dbConfig.Primary.Key]: 'APPLICATION',
                        [dbClient.dbConfig.Primary.Range]: 'WS_CONFIG',
                        requestContext: {
                            domainName: config.requestContext.domainName,
                            stage: config.requestContext.stage
                        }
                    }
                }).promise();
            }
        }
    }

    async send(connection, payload) {
        await this.setupClient(connection)

        let ConnectionId = connection;
        if (typeof connection === 'object') {
            ConnectionId = connection.requestContext.connectionId;
        }

        console.log(connection, payload)
        await this.client.postToConnection({
            ConnectionId,
            Data: JSON.stringify(payload)
        }).promise().catch(async err => {
            console.log(JSON.stringify(err))

            if (err.statusCode === 410) {
                // unsub all channels connection was in
                const subscriptions = await dbClient.fetchConnectionSubscriptions(ConnectionId);

                console.log(`[wsClient][send][postToConnection] Found stale connection, deleting ${ConnectionId}:`);
                console.log('[wsClient][send][postToConnection] Unsubscribe from channels:');
                console.log(JSON.stringify(subscriptions, null, 2));

                const unsubscribes = subscriptions.map(async subscription =>
                    dbClient.dynamodbClient.delete({
                        TableName: dbClient.dbConfig.Table,
                        Key: {
                            [dbClient.dbConfig.Channel.Connections.Key]: `${dbClient.dbConfig.Channel.Prefix}${dbClient.dbConfig.parseEntityId(subscription[dbClient.dbConfig.Channel.Primary.Key])}`,
                            [dbClient.dbConfig.Channel.Connections.Range]: `${dbClient.dbConfig.Connection.Prefix}${ConnectionId}`
                        }
                    }).promise()
                );

                await Promise.all(unsubscribes);
            }
        });

        return true;
    }

}