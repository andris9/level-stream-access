'use strict';

const levelup = require('levelup');
const stream = require('stream');
const SeqIndex = require('seq-index');

const Writable = stream.Writable;
const Readable = stream.Readable;
const Transform = stream.Transform;

class LevelWriteStream extends Writable {
    constructor(db, key, options) {
        super(options);
        this.key = key;
        this._db = db;
        this._initial = options && options.append ? options.append : true;
        this.seqIndex = new SeqIndex();
    }

    /**
     * Generates an unique sequential ID based on current time
     * @return {String} Unique ID
     */
    genIndex() {
        return this.key + ' ' + this.seqIndex.short();
    }

    _write(chunk, encoding, callback) {
        // empty chunk, do nothing
        if (!chunk || !chunk.length) {
            return callback();
        }

        // convert string chunk into Buffer
        if (typeof chunk === 'string') {
            chunk = Buffer.from(chunk, encoding);
        }

        let writeChunk = err => {
            if (err) {
                return callback(err);
            }
            this._db.put(this.genIndex(), chunk, {
                sync: false
            }, callback);
        };

        if (this._initial) {
            // delete old chunks for the same key before starting to write a new one
            this._initial = false;
            streamDelete(this._db, this.key, err => {
                if (err) {
                    return callback(err);
                }

                // store system metadata for this stream
                // this key can be later used to check if the stream for an ID exists or not
                this._db.put(this.key + ' #', {
                    created: Date.now()
                }, {
                    valueEncoding: 'json'
                }, err => {
                    if (err) {
                        return callback(err);
                    }
                    writeChunk();
                });

            });
        } else {
            writeChunk();
        }
    }
}

class LevelReadStream extends Readable {
    constructor(db, key, options) {
        super(options);
        this.key = key;
        this._db = db;
        this._index = (options && options.startIndex || '/').toString();
        this._keyStream = this._db.createReadStream({
            gt: key + ' ' + this._index,
            lt: key + ' :',
            fillCache: false,
            valueEncoding: {
                decode: val => val,
                encode: val => val,
                buffer: true,
                type: 'mybufenc'
            }
        });
        this._keyStream.pause();

        this._keyStream.on('data', data => {
            this._index = data.key;
            if (!this.push(data.value)) {
                this._keyStream.pause();
            }
        });

        this._keyStream.on('end', () => {
            this.push(null);
        });

        this._keyStream.on('error', err => {
            this.emit('error', err);
        });
    }

    _read() {
        this._keyStream.resume();
    }
}

class LevelStoreStream extends Transform {
    constructor(db, key, options) {
        super(options);
        this.key = key;
        this._db = db;
        this._initial = options && options.append ? options.append : true;
        this.seqIndex = new SeqIndex();
    }

    /**
     * Generates an unique sequential ID based on current time
     * @return {String} Unique ID
     */
    genIndex() {
        return this.key + ' ' + this.seqIndex.short();
    }

    _transform(chunk, encoding, callback) {
        // empty chunk, do nothing
        if (!chunk || !chunk.length) {
            return callback();
        }

        // convert string chunk into Buffer
        if (typeof chunk === 'string') {
            chunk = Buffer.from(chunk, encoding);
        }

        let writeChunk = err => {
            if (err) {
                return callback(err);
            }
            this._db.put(this.genIndex(), chunk, {
                sync: false
            }, err => {
                if (err) {
                    return callback(err);
                }
                return callback(null, chunk);
            });
        };

        if (this._initial) {
            // delete old chunks for the same key before starting to write a new one
            this._initial = false;
            streamDelete(this._db, this.key, err => {
                if (err) {
                    return callback(err);
                }

                // store system metadata for this stream
                // this key can be later used to check if the stream for an ID exists or not
                this._db.put(this.key + ' #', {
                    created: Date.now()
                }, {
                    valueEncoding: 'json'
                }, err => {
                    if (err) {
                        return callback(err);
                    }
                    writeChunk();
                });

            });
        } else {
            writeChunk();
        }
    }
}

/**
 * Deletes all stored chunks of a stream (including the metadata chunk)
 *
 * @param {Object} db Leveldb database instance
 * @param {String} key Stream key to delete
 * @param {Function} callback Function to run with the count of deleted chunks
 */
function streamDelete(db, key, callback) {
    let iterator = db.db.iterator({
        gt: key + ' ',
        lt: key + ' :',
        keys: true,
        values: false,
        fillCache: false,
        keyAsBuffer: false,
        valueAsBuffer: false
    });

    let ops = [];

    let iterate = () => {
        iterator.next((err, key) => {
            if (err) {
                return callback(err);
            }
            if (!key) {
                return iterator.end(err => {
                    if (err) {
                        return callback(err);
                    }
                    if (!ops.length) {
                        return callback(null, 0);
                    }
                    db.batch(ops, err => {
                        if (err) {
                            return callback(err);
                        }
                        return callback(null, ops.length);
                    });
                });
            }
            ops.push({
                type: 'del',
                key
            });
            setImmediate(iterate);
        });
    };
    setImmediate(iterate);
}

/**
 * Sets metadata for a stream
 *
 * @param {Object} db Database object
 * @param {String} key Stream key
 * @param {Object} value Data to store as JSON
 * @param {Function} callback Function to run once data is stored
 */
function streamSetMeta(db, key, data, callback) {
    db.get(key + ' #', err => {
        if (err) {
            if (err.name === 'NotFoundError') {
                return callback(null, false);
            }
            return callback(err);
        }
        db.put(key + ' *', data, {
            valueEncoding: 'json'
        }, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, true);
        });
    });
}

/**
 * Gets metadata for a stream
 *
 * @param {Object} db Database object
 * @param {String} key Stream key
 * @param {Function} callback Function to run with the retrieved metadata object or false
 */
function streamGetMeta(db, key, callback) {
    db.get(key + ' #', {
        valueEncoding: 'json'
    }, (err, data) => {
        if (err) {
            if (err.name === 'NotFoundError') {
                return callback(null, false);
            }
            return callback(err);
        }
        db.get(key + ' *', {
            valueEncoding: 'json'
        }, (err, userData) => {
            if (err) {
                if (err.name === 'NotFoundError') {
                    return callback(null, false);
                }
                return callback(err);
            }
            Object.keys(data).forEach(key => {
                userData[key] = data[key];
            });
            return callback(null, userData);
        });
    });
}

/**
 * Prepends data to a stored stream
 *
 * @param {Object} db Database object
 * @param {String} key Stream key
 * @param {Buffer} value Data to prepend
 * @param {Function} callback [description]
 */
function streamPrepend(db, key, chunk, callback) {
    if (typeof chunk === 'string') {
        chunk = Buffer.from(chunk);
    }
    if (!chunk || typeof chunk !== 'object' || typeof chunk.slice !== 'function') {
        return setImmediate(() => callback(new Error('Value must be a Buffer instance')));
    }
    // find the first chunk of the stream
    let iterator = db.db.iterator({
        gt: key + ' /',
        lt: key + ' :',
        keys: true,
        values: false,
        fillCache: false,
        keyAsBuffer: false,
        valueAsBuffer: false
    });

    iterator.next((err, key) => {
        if (err) {
            return callback(err);
        }
        iterator.end(err => {
            if (err) {
                return callback(err);
            }
            if (!key) {
                return callback(null, false);
            }

            let keyParts = key.split(' ');
            let index = parseInt(keyParts.pop(), 16);
            let keyPrefix = keyParts.join(' ');

            db.put(keyPrefix + ' ' + (--index).toString(16), chunk, {
                sync: false
            }, err => {
                if (err) {
                    return callback(err);
                }
                return callback(null, true);
            });
        });
    });
}

// Expose to the world!
module.exports = db => {
    if (typeof db === 'string') {
        db = levelup(db);
    }
    return {
        createReadStream: (key, options) => new LevelReadStream(db, key, options),
        createWriteStream: (key, options) => new LevelWriteStream(db, key, options),
        createStoreStream: (key, options) => new LevelStoreStream(db, key, options),
        delete: (key, callback) => streamDelete(db, key, callback),
        prepend: (key, value, callback) => streamPrepend(db, key, value, callback),
        setMeta: (key, data, callback) => streamSetMeta(db, key, data, callback),
        getMeta: (key, callback) => streamGetMeta(db, key, callback)
    };
};
