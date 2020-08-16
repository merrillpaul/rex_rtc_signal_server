import { dbClient } from './utils/db';
import { Client } from './utils/websocket';
import * as sanitize from 'sanitize-html';
import { Handler, Callback } from 'aws-lambda';

const wsClient = new Client();

const success = {
    statusCode: 200,
    body: JSON.stringify({ msg: 'success' })
};

export const channelManager: Handler = async (event: any, context, callback: Callback) => {
    const action = JSON.parse(event.body).action;
    switch (action) {
        case "subscribeChannel":
            await subscribeChannel(event, context, callback);
            break;
        case "unsubscribeChannel":
            await unsubscribeChannel(event, context, callback);
            break;
        default:
            break;
    }

    return success
}

export const subscribeChannel: Handler = async (event: any, context, callback: Callback) => {
    const channelId = JSON.parse(event.body).channelId;
    await dbClient.dynamodbClient.put({
        TableName: dbClient.dbConfig.Table,
        Item: {
            [dbClient.dbConfig.Channel.Connections.Key]: `${dbClient.dbConfig.Channel.Prefix}${channelId}`,
            [dbClient.dbConfig.Channel.Connections.Range]: `${dbClient.dbConfig.Connection.Prefix}${
                dbClient.parseEntityId(event)
                }`
        }
    }).promise();

    // Instead of broadcasting here we listen to the dynamodb stream
    // just a fun example of flexible usage
    // you could imagine bots or other sub systems broadcasting via a write the db
    // and then streams does the rest
    return success;
};

export const unsubscribeChannel: Handler = async (event, context, callback: Callback) => {
    const channelId = JSON.parse(event.body).channelId;
    const item = await dbClient.dynamodbClient.delete({
        TableName: dbClient.dbConfig.Table,
        Key: {
            [dbClient.dbConfig.Channel.Connections.Key]: `${dbClient.dbConfig.Channel.Prefix}${channelId}`,
            [dbClient.dbConfig.Channel.Connections.Range]: `${dbClient.dbConfig.Connection.Prefix}${
                dbClient.parseEntityId(event)
                }`
        }
    }).promise();
    return success;
};

export const sendMessage: Handler = async (event, context, callback: Callback) => {
    // save message for future history
    // saving with timestamp allows sorting
    // maybe do ttl?

    const body = JSON.parse(event.body);
    const messageId = `${dbClient.dbConfig.Message.Prefix}${Date.now()}`;
    const name = body.name
        .replace(/[^a-z0-9\s-]/gi, "")
        .trim()
        .replace(/\+s/g, "-");
    const content = sanitize(body.content, {
        allowedTags: ["ul", "ol", "b", "i", "em", "strike", "pre", "strong", "li"],
        allowedAttributes: {}
    });

    // save message in database for later
    const item = await dbClient.dynamodbClient.put({
        TableName: dbClient.dbConfig.Table,
        Item: {
            [dbClient.dbConfig.Message.Primary.Key]: `${dbClient.dbConfig.Channel.Prefix}${body.channelId}`,
            [dbClient.dbConfig.Message.Primary.Range]: messageId,
            ConnectionId: `${event.requestContext.connectionId}`,
            Name: name,
            Content: content
        }
    }).promise();

    const subscribers = await dbClient.fetchChannelSubscriptions(body.channelId);
    const results = subscribers.map(async subscriber => {
        const subscriberId = dbClient.parseEntityId(
            subscriber[dbClient.dbConfig.Channel.Connections.Range]
        );
        if (event.requestContext.connectionId === subscriberId) {
            return;
        }
        return wsClient.send(subscriberId, {
            event: "channel_message",
            channelId: body.channelId,
            name,
            content
        });
    });

    await Promise.all(results);
    return success;
}

export const broadcast: Handler = async (event, context, callback: Callback) => {
    // info from table stream, we'll learn about connections
    // disconnections, messages, etc
    // get all connections for channel of interest
    // broadcast the news
    const results = event.Records.map(async record => {
        switch (record.dynamodb.Keys[dbClient.dbConfig.Primary.Key].S.split("|")[0]) {
            // Connection entities
            case dbClient.dbConfig.Connection.Entity:
                break;

            // Channel entities (most stuff)
            case dbClient.dbConfig.Channel.Entity:
                // figure out what to do based on full entity model

                // get secondary ENTITY| type by splitting on | and looking at first part
                switch (record.dynamodb.Keys[dbClient.dbConfig.Primary.Range].S.split("|")[0]) {
                    // if we are a CONNECTION
                    case dbClient.dbConfig.Connection.Entity: {
                        let eventType = "sub";
                        if (record.eventName === "REMOVE") {
                            eventType = "unsub";
                        } else if (record.eventName === "UPDATE") {
                            // currently not possible, and not handled
                            break;
                        }

                        // A connection event on the channel
                        // let all users know a connection was created or dropped
                        const channelId = dbClient.parseEntityId(
                            record.dynamodb.Keys[dbClient.dbConfig.Primary.Key].S
                        );
                        const subscribers = await dbClient.fetchChannelSubscriptions(channelId);
                        const results = subscribers.map(async subscriber => {
                            const subscriberId = dbClient.parseEntityId(
                                subscriber[dbClient.dbConfig.Channel.Connections.Range]
                            );
                            return wsClient.send(
                                subscriberId, // really backwards way of getting connection id
                                {
                                    event: `subscriber_${eventType}`,
                                    channelId,
                                    count: subscribers.length,
                                    // sender of message "from id"
                                    subscriberId: dbClient.parseEntityId(
                                        record.dynamodb.Keys[dbClient.dbConfig.Primary.Range].S
                                    )
                                }
                            );
                        });

                        await Promise.all(results);
                        break;
                    }

                    // If we are a MESSAGE
                    case dbClient.dbConfig.Message.Entity: {
                        if (record.eventName !== "INSERT") {
                            return success;
                        }

                        // We could do interesting things like see if this was a bot
                        // or other system directly adding messages to the dynamodb table
                        // then send them out, otherwise assume it was already blasted out on the sockets
                        // and no need to send it again!
                        break;
                    }
                    default:
                        break;
                }

                break;
            default:
                break;
        }
    });

    await Promise.all(results);
    return success;
}

export const connectionManager: Handler = async (event, context, callback: Callback) => {
    // we do this so first connect EVER sets up some needed config state in db
    // this goes away after CloudFormation support is added for web sockets
    console.log(`RX Websocket connection manager init`);
    await wsClient.setupClient(event);
    console.log(`RX Websocket After setting up client`);
    if (event.requestContext.eventType === "CONNECT") {
        // sub general channel
        await subscribeChannel(
            {
                ...event,
                body: JSON.stringify({
                    action: "subscribe",
                    channelId: "General"
                })
            },
            context,
            callback
        );

        return success;
    } else if (event.requestContext.eventType === "DISCONNECT") {
        // unsub all channels connection was in
        const subscriptions = await dbClient.fetchConnectionSubscriptions(event);
        const unsubscribes = subscriptions.map(async subscription =>
            // just simulate / reuse the same as if they issued the request via the protocol
            unsubscribeChannel(
                {
                    ...event,
                    body: JSON.stringify({
                        action: "unsubscribe",
                        channelId: dbClient.parseEntityId(subscription[dbClient.dbConfig.Channel.Primary.Key])
                    })
                },
                context,
                callback
            )
        );

        await Promise.all(unsubscribes);
        return success;
    }
};


export const defaultMessage: Handler = async (event, _context, callback: Callback) => {
    await wsClient.send(event, {
        event: "error",
        message: "invalid action type"
    });

    return success;
};
