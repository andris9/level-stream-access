'use strict';

const levelup = require('levelup');
const stream = require('stream');
const Writable = stream.Writable;
const Readable = stream.Readable;
const Transform = stream.Transform;

function levelDelete(db, key, callback) {
    let keys = db.createKeyStream({
        gt: key + ' ',
        lt: key + ' :'
    });
    let returned = false;
    let deleted = 0;

    keys.on('readable', () => {
        if (returned) {
            return;
        }
        let key, ops = [];
        while ((key = keys.read()) !== null) {
            ops.push({
                type: 'del',
                key
            });
        }
        if (ops.length) {
            deleted += ops.length;
            db.batch(ops, err => {
                if (returned) {
                    return;
                }
                if (err) {
                    returned = true;
                    return callback(err);
                }
            });
        }
    });

    keys.on('error', err => {
        if (returned) {
            return;
        }
        returned = true;
        callback(err);
    });

    keys.on('end', () => {
        if (returned) {
            return;
        }
        returned = true;
        callback(null, deleted);
    });
}

class LevelWriteStream extends Writable {
    constructor(db, key, options) {
        super(options);
        this.key = key;
        this._db = db;
        this._seqCounter = 0;
        this._initial = options && options.append ? options.append : true;
    }

    /**
     * Generates an unique sequential ID based on current time
     * @return {String} Unique ID
     */
    _genIndex() {
        let index = this._seqCounter++;
        return this.key + ' ' + (Date.now() * 0x1000 + (index & 0xfff)); // eslint-disable-line no-bitwise
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
            this._db.put(this._genIndex(), chunk, {
                sync: true
            }, callback);
        };

        if (this._initial) {
            // delete old chunks for the same key before starting to write a new one
            this._initial = false;
            levelDelete(this._db, this.key, writeChunk);
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
        this._index = (options && options.startIndex || '').toString();
        this._keyStream = this._db.createReadStream({
            gt: key + ' ' + this._index,
            lt: key + ' :'
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
        this._seqCounter = 0;
        this._initial = options && options.append ? options.append : true;
    }

    /**
     * Generates an unique sequential ID based on current time
     * @return {String} Unique ID
     */
    _genIndex() {
        let index = this._seqCounter++;
        return this.key + ' ' + (Date.now() * 0x1000 + (index & 0xfff)); // eslint-disable-line no-bitwise
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
            this._db.put(this._genIndex(), chunk, {
                sync: true
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
            levelDelete(this._db, this.key, writeChunk);
        } else {
            writeChunk();
        }
    }
}

// Expose to the world!
module.exports = db => {
    if (typeof db === 'string') {
        db = levelup(db);
    }
    return {
        delete: (key, callback) => levelDelete(db, key, callback),
        createReadStream: (key, options) => new LevelReadStream(db, key, options),
        createWriteStream: (key, options) => new LevelWriteStream(db, key, options),
        createStoreStream: (key, options) => new LevelStoreStream(db, key, options)
    };
};
