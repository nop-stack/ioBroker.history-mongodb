'use strict';

const logger = {
    debug: function(...args) {
        console.log('[DEBUG]', new Date().toISOString(), ...args);
    },
    info: function(...args) {
        console.log('[INFO]', new Date().toISOString(), ...args);
    },
    warn: function(...args) {
        console.warn('[WARN]', new Date().toISOString(), ...args);
    },
    error: function(...args) {
        console.error('[ERROR]', new Date().toISOString(), ...args);
    }
};

module.exports = logger;
