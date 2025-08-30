'use strict';

const MongoDbStorage = require('./mongoDbStorage');

class StateManager {
    constructor(adapter) {
        this.adapter = adapter;
        this.storage = null;
    }

    async initialize() {
        const config = {
            url: this.adapter.config.mongoUrl || 'mongodb://localhost:27017',
            database: this.adapter.config.mongoDatabase || 'iobroker_history',
            collection: this.adapter.config.mongoCollection || 'states'
        };

        this.storage = new MongoDbStorage(config);
        
        try {
            await this.storage.connect();
            this.adapter.log.info('Successfully connected to MongoDB');
            return true;
        } catch (error) {
            this.adapter.log.error(`Failed to connect to MongoDB: ${error.message}`);
            return false;
        }
    }

    async storeState(id, state) {
        try {
            await this.storage.store(id, state);
            return true;
        } catch (error) {
            this.adapter.log.error(`Failed to store state in MongoDB: ${error.message}`);
            return false;
        }
    }

    async getHistory(id, options) {
        try {
            return await this.storage.getHistory(id, options);
        } catch (error) {
            this.adapter.log.error(`Failed to get history from MongoDB: ${error.message}`);
            return [];
        }
    }

    async close() {
        if (this.storage) {
            await this.storage.close();
        }
    }
}

module.exports = StateManager;
