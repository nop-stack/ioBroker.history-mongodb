'use strict';

const MongoDbStorage = require('./mongoDbStorage');

class StateManager {
    constructor(adapter) {
        this.adapter = adapter;
        this.storage = null;
    }

    async initialize() {
        this.adapter.log.debug('Initializing StateManager');
        this.adapter.log.debug('Adapter config:', JSON.stringify(this.adapter.config));
        
        const config = {
            url: this.adapter.config.mongoUrl || 'mongodb://localhost:27017',
            database: this.adapter.config.mongoDatabase || 'iobroker_history',
            collection: this.adapter.config.mongoCollection || 'states'
        };
        
        this.adapter.log.debug('Creating MongoDB storage with config:', JSON.stringify(config));
        this.storage = new MongoDbStorage(this.adapter);
        
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
            if (this.storage) {
                await this.storage.store(id, state);
                return true;
            }
            return false;
        } catch (error) {
            this.adapter.log.error(`Failed to store state in MongoDB: ${error.message}`);
            return false;
        }
    }

    async getHistory(id, options) {
        try {
            if (this.storage) {
                return await this.storage.getHistory(id, options);
            }
            return [];
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
