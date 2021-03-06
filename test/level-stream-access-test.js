/* eslint no-unused-expressions:0, prefer-arrow-callback: 0 */
/* globals afterEach, beforeEach, describe, it */

'use strict';

const chai = require('chai');
const expect = chai.expect;
const fs = require('fs');
const crypto = require('crypto');
const levelup = require('levelup');
const levelStreamAccess = require('../lib/level-stream-access.js');
const db = levelup('/some/location', {
    db: require('memdown') // eslint-disable-line global-require
});
const levelStream = levelStreamAccess(db);

chai.config.includeStack = true;

describe('level-stream-access tests', function () {
    it('should write and read data from db using write/read', function (done) {
        let instream = fs.createReadStream(__dirname + '/fixtures/alice.txt');
        let writestream = levelStream.createWriteStream('test1');
        writestream.on('finish', function () {
            let md5 = crypto.createHash('md5');
            let readstream = levelStream.createReadStream('test1');
            readstream.on('data', function (chunk) {
                md5.update(chunk);
            });
            readstream.on('end', function () {
                expect(md5.digest('hex')).to.equal('ff5c6c94bcd0f12d6769d1b623d9503d');
                done();
            });
        });
        instream.pipe(writestream);
    });

    it('should write and read data from db using store', function (done) {
        let instream = fs.createReadStream(__dirname + '/fixtures/alice.txt');
        let storestream = levelStream.createStoreStream('test2');
        let md5 = crypto.createHash('md5');
        storestream.on('data', function (chunk) {
            md5.update(chunk);
        });
        storestream.on('end', function () {
            expect(md5.digest('hex')).to.equal('ff5c6c94bcd0f12d6769d1b623d9503d');
            done();
        });
        instream.pipe(storestream);
    });

    it('should set and get metadata', function (done) {
        let meta = {
            a: 1,
            b: [1, 2, 3]
        };
        levelStream.setMeta('non-existant-key', meta, (err, success) => {
            expect(err).to.not.exist;
            expect(success).to.be.false;
            levelStream.setMeta('test2', meta, (err, success) => {
                expect(err).to.not.exist;
                expect(success).to.be.true;
                levelStream.getMeta('test2', (err, stored) => {
                    expect(err).to.not.exist;
                    expect(meta === stored).to.be.false;
                    expect(meta.b).to.be.deep.equal(stored.b);
                    expect(stored.created).to.exist;
                    done();
                });
            });
        });
    });

    it('should delete existing keys', function (done) {
        levelStream.getMeta('test2', (err, meta) => {
            expect(err).to.not.exist;
            expect(meta).to.exist;
            levelStream.delete('test2', function (err, deleted) {
                expect(err).to.not.exist;
                expect(deleted).to.be.gt(0);
                levelStream.getMeta('test2', (err, meta) => {
                    expect(err).to.not.exist;
                    expect(meta).to.be.false;
                    levelStream.delete('test2', function (err, deleted) {
                        expect(err).to.not.exist;
                        expect(deleted).to.equal(0);
                        done();
                    });
                });
            });
        });
    });

    it('should write and prepend data to db using write/read', function (done) {
        let instream = fs.createReadStream(__dirname + '/fixtures/alice.txt');
        let writestream = levelStream.createWriteStream('test1');
        writestream.on('finish', function () {

            levelStream.prepend('test1', 'kolmas rida\n', (err, success) => {
                expect(err).to.not.exist;
                expect(success).to.be.true;

                levelStream.prepend('test1', 'teine rida\n', (err, success) => {
                    expect(err).to.not.exist;
                    expect(success).to.be.true;

                    levelStream.prepend('test1', 'esimene rida\n', (err, success) => {
                        expect(err).to.not.exist;
                        expect(success).to.be.true;

                        let md5 = crypto.createHash('md5');
                        let readstream = levelStream.createReadStream('test1');
                        readstream.on('data', function (chunk) {
                            md5.update(chunk);
                        });
                        readstream.on('end', function () {
                            expect(md5.digest('hex')).to.equal('027d88cb351db81d6fcd2b5ee50bddf2');
                            done();
                        });
                    });
                });
            });
        });
        instream.pipe(writestream);
    });
});
