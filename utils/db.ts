import { ItemList, DocumentClient } from 'aws-sdk/clients/dynamodb';

const ddb = new DocumentClient();
const db = {
    Table: process.env.APPLICATION_TABLE,
    Primary: {
        Key: 'pk',
        Range: 'sk'
    },
    Connection: {
        Primary: {
            Key: 'pk',
            Range: 'sk'
        },
        Channels: {
            Index: 'reverse',
            Key: 'sk',
            Range: 'pk'
        },
        Prefix: 'CONNECTION|',
        Entity: 'CONNECTION'
    },
    Channel: {
        Primary: {
            Key: 'pk',
            Range: 'sk'
        },
        Connections: {
            Key: 'pk',
            Range: 'sk'
        },
        Messages: {
            Key: 'pk',
            Range: 'sk'
        },
        Prefix: 'CHANNEL|',
        Entity: 'CHANNEL'
    },
    Message: {
        Primary: {
            Key: 'pk',
            Range: 'sk'
        },
        Prefix: 'MESSAGE|',
        Entity: 'MESSAGE'
    }
}

const channelRegex = new RegExp(`^${db.Channel.Entity}\|`);
const messageRegex = new RegExp(`^${db.Message.Entity}\|`);
const connectionRegex = new RegExp(`^${db.Connection.Entity}\|`);

class DynamoDbClient {

    dynamodbClient: DocumentClient = ddb;
    dbConfig: any = db;

    parseEntityId(target: any) {
        console.log('ENTITY ID A ', target)

        if (typeof target === 'object') {
            // use from raw event, only needed for connectionId at the moment
            target = target.requestContext.connectionId;
        } else {
            // strip prefix if set so we always get raw id
            target = target
                .replace(channelRegex, '')
                .replace(messageRegex, '')
                .replace(connectionRegex, '');
        }

        return target.replace('|', '');
    }

    async fetchConnectionSubscriptions(connection: any): Promise<ItemList> {
        const connectionId = this.parseEntityId(connection)
        const results = await ddb.query({
            TableName: db.Table,
            IndexName: db.Connection.Channels.Index,
            KeyConditionExpression: `${
                db.Connection.Channels.Key
                } = :connectionId and begins_with(${
                db.Connection.Channels.Range
                }, :channelEntity)`,
            ExpressionAttributeValues: {
                ":connectionId": `${db.Connection.Prefix}${
                    connectionId
                    }`,
                ":channelEntity": db.Channel.Prefix
            }
        }).promise();

        return results.Items;
    }

    async fetchChannelSubscriptions(channel: any): Promise<ItemList> {
        const channelId = this.parseEntityId(channel)
        const results = await ddb.query({
            TableName: db.Table,
            KeyConditionExpression: `${
                db.Channel.Connections.Key
                } = :channelId and begins_with(${
                db.Channel.Connections.Range
                }, :connectionEntity)`,
            ExpressionAttributeValues: {
                ":channelId": `${db.Channel.Prefix}${channelId}`,
                ":connectionEntity": db.Connection.Prefix
            }
        }).promise();

        return results.Items;
    }
}

export const dbClient = new DynamoDbClient();