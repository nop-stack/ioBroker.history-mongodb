'use strict';

const { MongoClient } = require('mongodb');

class MongoDbStorage {
    constructor(adapter) {
        this.config = adapter.config;
        this.adapter = adapter;
        this.client = null;
        this.db = null;
        this.collection = null;
    }

    connect() {
        const url = this.config.url || 'mongodb://localhost:27017';
        this.adapter.log.debug('Attempting to connect to MongoDB at:', url);
        this.adapter.log.debug('Connection config:', JSON.stringify({
            database: this.config.database || 'iobroker_history',
            collection: this.config.collection || 'states'
        }));
        
        return MongoClient.connect(url).then(client => {
            this.client = client;
            this.adapter.log.info('MongoDB client connected successfully');
            this.db = this.client.db(this.config.database || 'iobroker_history');
            this.adapter.log.debug('Database selected:', this.db.databaseName);
            this.collection = this.db.collection(this.config.collection || 'states');
            this.adapter.log.debug('Collection selected:', this.collection.collectionName);

            return true;
        }).catch(err => {
            this.adapter.log.error('MongoDB connection error:', err);
        });
    }

    store(id, state) {
        
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
            this.collection.insertOne(entry).then(() => {
                this.adapter.log.debug(`Stored state for ${id} in MongoDB`);
            }).catch(err => {
                this.adapter.log.error('Error storing state in MongoDB:', err);
            });

            return true;
        }

        this.adapter.log.error('MongoDB collection is not initialized');
        return false;
    }

    getHistory(id, options) {
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
    }

    close() {
        if (this.client) {
            this.client.close();
        }
    }
}

module.exports = MongoDbStorage;
