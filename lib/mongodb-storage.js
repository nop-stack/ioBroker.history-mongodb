const { MongoClient } = require('mongodb');

/**
 * MongoDB Storage Klasse für ioBroker History Adapter
 */
class MongoDBStorage {
    /**
     * @param {string} url - MongoDB Connection URL
     * @param {string} dbName - Name der Datenbank
     */
    constructor(url, dbName) {
        this.url = url;
        this.dbName = dbName;
        this.client = null;
        this.db = null;
        this.bulkOps = [];
        this.bulkTimer = null;
    }

    /**
     * Verbindung zur MongoDB herstellen
     * @param {string} [url] - Optional: Neue MongoDB URL
     * @param {string} [dbName] - Optional: Neuer Datenbankname
     * @returns {Promise<boolean>} true wenn erfolgreich verbunden
     */
    async connect(url, dbName) {
        try {
            this.url = url || this.url;
            this.dbName = dbName || this.dbName;
            
            if (!this.url || !this.dbName) {
                throw new Error('MongoDB URL and database name are required');
            }

            this.client = await MongoClient.connect(this.url);
            this.db = this.client.db(this.dbName);
            
            // Erstelle Index für bessere Performance
            if (this.db) {
                await this.db.collection('history').createIndex({ id: 1, ts: 1 });
            }
            return true;
        } catch (err) {
            if (this.client) {
                await this.client.close().catch(() => {});
            }
            this.client = null;
            this.db = null;
            return false;
        }
    }

    /**
     * Prüft ob eine aktive Verbindung besteht
     * @returns {boolean} true wenn verbunden
     */
    isConnected() {
        return !!(this.client && this.db);
    }

    /**
     * Verbindung schließen und Ressourcen freigeben
     */
    async close() {
        try {
            await this.flushBulkOps();
            if (this.client) {
                await this.client.close().catch(() => {});
            }
        } finally {
            this.client = null;
            this.db = null;
            this.bulkOps = [];
            if (this.bulkTimer) {
                clearTimeout(this.bulkTimer);
                this.bulkTimer = null;
            }
        }
    }

    /**
     * Speichert einen Wert in der Historie
     * @param {string} id - ID des Datenpunkts
     * @param {object} state - Zustand des Datenpunkts
     * @returns {Promise<boolean>} true wenn erfolgreich gespeichert
     */
    async store(id, state) {
        if (!this.isConnected() || !this.db) {
            return false;
        }
        
        try {
            const entry = {
                id,
                ts: new Date(state.ts),
                val: state.val,
                ack: state.ack || false,
                from: state.from,
                q: state.q || 0
            };

            this.bulkOps.push({
                insertOne: { document: entry }
            });

            if (this.bulkOps.length >= 1000) {
                await this.flushBulkOps();
            } else if (!this.bulkTimer) {
                this.bulkTimer = setTimeout(() => this.flushBulkOps(), 1000);
            }

            return true;
        } catch (err) {
            return false;
        }
    }

    /**
     * Alias für store() - Kompatibilität
     */
    async storeValue(id, state) {
        return this.store(id, state);
    }

    /**
     * Alias für store() - Kompatibilität
     */
    async storeState(id, state) {
        return this.store(id, state);
    }

    /**
     * Schreibt gepufferte Operationen in die Datenbank
     */
    async flushBulkOps() {
        if (!this.isConnected() || !this.db || this.bulkOps.length === 0) {
            return;
        }

        const ops = [...this.bulkOps];
        this.bulkOps = [];

        try {
            if (this.bulkTimer) {
                clearTimeout(this.bulkTimer);
                this.bulkTimer = null;
            }

            const collection = this.db.collection('history');
            await collection.bulkWrite(ops, { ordered: false });
        } catch (err) {
            // Im Fehlerfall: Operationen wieder zur Liste hinzufügen
            this.bulkOps = [...ops, ...this.bulkOps];
        }
    }

    /**
     * Liest Historiendaten aus der Datenbank
     * @param {string} id - ID des Datenpunkts
     * @param {object} options - Abfrageoptionen
     * @returns {Promise<Array>} Array mit Historiendaten
     */
    async getHistory(id, options = {}) {
        if (!this.isConnected() || !this.db) {
            return [];
        }

        try {
            const collection = this.db.collection('history');
            const query = { id };

            if (options.start || options.end) {
                query.ts = {};
                if (options.start) {
                    query.ts.$gte = new Date(options.start);
                }
                if (options.end) {
                    query.ts.$lte = new Date(options.end);
                }
            }

            const result = await collection.find(query)
                .sort({ ts: options.sort || 1 })
                .toArray();

            return result.map(entry => ({
                ts: entry.ts.getTime(),
                val: entry.val,
                ack: entry.ack,
                from: entry.from,
                q: entry.q
            }));
        } catch (err) {
            return [];
        }
    }
}

module.exports = MongoDBStorage;