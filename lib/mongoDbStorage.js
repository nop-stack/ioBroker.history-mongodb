'use strict';

const { t } = require('@iobroker/adapter-core/build/esm/i18n');
const { MongoClient } = require('mongodb');

class MongoDbStorage {
    constructor(adapter, config) {
        this.config = config;
        this.adapter = adapter;
        this.client = null;
        this.db = null;
        this.collection = null;
    }

    async connect() {
        try {
            const url = this.config.url || 'mongodb://localhost:27017';
            this.adapter.log.debug('Attempting to connect to MongoDB at:', url);
            this.adapter.log.debug('Connection config:', JSON.stringify({
                database: this.config.database || 'iobroker_history',
                collection: this.config.collection || 'states'
            }));
            
            this.client = await MongoClient.connect(url);
            this.adapter.log.info('MongoDB client connected successfully');
            
            this.db = this.client.db(this.config.database || 'iobroker_history');
            this.adapter.log.debug('Database selected:', this.db.databaseName);
            
            this.collection = this.db.collection(this.config.collection || 'states');
            this.adapter.log.debug('Collection selected:', this.collection.collectionName);
            
            return true;
        } catch (error) {
            throw new Error(`MongoDB connection failed: ${error.message}`);
        }
    }

    async store(id, state) {
        try {
            const entry = {
                id,
                state: state.val,
                ts: state.ts,
                ack: state.ack,
                from: state.from,
                q: state.q,
                c: state.c
            };
            if (this.collection) {
                this.collection.insertOne(entry);
                return true;
            }
            throw new Error('No MongoDB collection available');
        } catch (error) {
            throw new Error(`Failed to store data in MongoDB: ${error.message}`);
        }
    }

    async getHistory(id, options) {
        try {
            const query = { id };
            if (options.start) {
                query.ts = { $gte: options.start };
            }
            if (options.end) {
                query.ts = { ...query.ts, $lte: options.end };
            }
            
            const cursor = this.collection?.find(query).sort({ ts: -1 });
            if (options.limit) {
                cursor?.limit(options.limit);
            }
            
            return cursor?.toArray();
        } catch (error) {
            throw new Error(`Failed to get history from MongoDB: ${error.message}`);
        }
    }

    async close() {
        if (this.client) {
            await this.client.close();
        }
    }
}

module.exports = MongoDbStorage;
