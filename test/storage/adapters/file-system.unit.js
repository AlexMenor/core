'use strict';

var FileSystemStorageAdapter = 
require('../../../lib/storage/adapters/file-system');
var StorageItem = require('../../../lib/storage/item');
var expect = require('chai').expect;
var utils = require('../../../lib/utils');
var Contract = require('../../../lib/contract');
var AuditStream = require('../../../lib/audit-tools/audit-stream');
var sinon = require('sinon');
var os = require('os');
var rimraf = require('rimraf');
var path = require('path');
var TMP_DIR = path.join(os.tmpdir(), 'STORJ_FILESYSTEM_ADAPTER_TEST');
var mkdirp = require('mkdirp');
var EventEmitter = require('events').EventEmitter;

function tmpdir() {
  return path.join(TMP_DIR, 'test-' + Date.now() + '.db');
}

var store = null;
var hash = utils.rmd160('test');
var audit = new AuditStream(12);
var contract = new Contract();
var item = new StorageItem({
  hash: hash,
  shard: Buffer.from('test')
});

describe('FileSystemStorageAdapter', function() {

  before(function() {
    if (utils.existsSync(TMP_DIR)) {
      rimraf.sync(TMP_DIR);
    }
    mkdirp.sync(TMP_DIR);
    audit.end(Buffer.from('test'));
    store = new FileSystemStorageAdapter(tmpdir());
  });

  describe('@constructor', function() {

    it('should create instance without the new keyword', function() {
      expect(
        FileSystemStorageAdapter(tmpdir())
      ).to.be.instanceOf(FileSystemStorageAdapter);
    });

  });

  describe('#_validatePath', function() {

    it('should not make a directory that already exists', function() {
      expect(function() {
        var tmp = tmpdir();
        mkdirp.sync(tmp);
        FileSystemStorageAdapter.prototype._validatePath(tmp);
      }).to.not.throw(Error);
    });

  });

  describe('#_put', function() {

    it('should store the item', function(done) {
      item.contracts[hash] = contract;
      item.challenges[hash] = audit.getPrivateRecord();
      item.trees[hash] = audit.getPublicRecord();
      store._put(hash, item, function(err) {
        expect(err).equal(null);
        done();
      });
    });

    it('should bubble error if the underlying db#put fails', function(done) {
      var _put = sinon.stub(store._db, 'put').callsArgWith(
        3,
        new Error('Failed')
      );
      store._put(hash, item, function(err) {
        expect(err.message).equal('Failed');
        _put.restore();
        done();
      });
    });

  });

  describe('#_get', function() {

    it('should return the stored item', function(done) {
      store._get(hash, function(err, item) {
        expect(err).to.equal(null);
        expect(item).to.be.instanceOf(Object);
        done();
      });
    });

    it('should return error if the data is not found', function(done) {
      var _dbget = sinon.stub(store._db, 'get').callsArgWith(
        2,
        new Error('Not found')
      );
      store._get(hash, function(err) {
        expect(err.message).to.equal('Not found');
        _dbget.restore();
        done();
      });
    });
  });

  describe('#_peek', function() {

    it('should return the stored item', function(done) {
      store._peek(hash, function(err, item) {
        expect(err).to.equal(null);
        expect(item).to.be.instanceOf(Object);
        done();
      });
    });

    it('should return error if the data is not found', function(done) {
      var _dbpeek = sinon.stub(store._db, 'get').callsArgWith(
        2,
        new Error('Not found')
      );
      store._peek(hash, function(err) {
        expect(err.message).to.equal('Not found');
        _dbpeek.restore();
        done();
      });
    });

  });

  describe('#_keys', function() {

    it('should stream all of the keys', function(done) {
      var keyStream = store._keys();
      keyStream.on('data', function(key) {
        expect(key.toString()).to.equal(
          '5e52fee47e6b070565f74372468cdc699de89107'
        );
      }).on('end', done);
    });

    it('should bubble error if emitted from stream', function(done) {
      var emitter = new EventEmitter();
      var _createKeyStream = sinon.stub(store._db, 'createKeyStream').returns(
        emitter
      );
      var keyStream = store._keys();
      keyStream.on('error', function(err) {
        _createKeyStream.restore();
        expect(err.message).to.equal('Failed');
        done();
      })
      setImmediate(function() {
        emitter.emit('error', new Error('Failed'));
      });
    });

  });

  describe('#_del', function() {

    it('should return error if del fails', function(done) {
      var _dbdel = sinon.stub(store._db, 'del').callsArgWith(
        1,
        new Error('Failed to delete')
      );
      store._del(hash, function(err) {
        expect(err.message).to.equal('Failed to delete');
        _dbdel.restore();
        done();
      });
    });

    it('should return error if unlink fails', function(done) {
      var _storereset = sinon.stub(store._fs, 'unlink').callsArgWith(
        1,
        new Error('Failed to delete')
      );
      store._del(hash, function(err) {
        expect(err.message).to.equal('Failed to delete');
        _storereset.restore();
        done();
      });
    });

    it('should delete the shard if it exists', function(done) {
      store._del(hash, function(err) {
        expect(err).to.equal(null);
        done();
      });
    });

  });

  describe('#_size', function() {

    it('should bubble errors from size calculation', function(done) {
      var _approx = sinon.stub(store._db.db, 'approximateSize').callsArgWith(
        2,
        new Error('Failed')
      );
      store._isUsingDefaultBackend = true;
      store._size(function(err) {
        _approx.restore();
        expect(err.message).to.equal('Failed');
        done();
      });
    });

    it('should return the size of the store on disk', function(done) {
      var _approx = sinon.stub(store._db.db, 'approximateSize').callsArgWith(
        2,
        null,
        7 * 1024
      );
      var _stat = sinon.stub(store._fs, 'statSync').returns({size:400})

      store._isUsingDefaultBackend = true;
      store._size(function(err, shardsize, contractsize) {
        _approx.restore();
        _stat.restore();
        expect(shardsize).to.equal(400);
        expect(contractsize).to.equal(7 * 1024);
        done();
      });
    });
  });

  describe('#_open', function() {

    it('should callback null if already open', function(done) {
      store._open(function(err) {
        expect(err).to.equal(null);
        done();
      });
    });

    it('should open the db if closed', function(done) {
      store._isOpen = false;
      var open = sinon.stub(store._db, 'open').callsArgWith(0, null);
      store._open(function(err) {
        open.restore();
        expect(open.called).to.equal(true);
        expect(err).to.equal(null);
        done();
      });
    });

    it('should bubble error if db open fails', function(done) {
      store._isOpen = false;
      var open = sinon.stub(store._db, 'open').callsArgWith(
        0,
        new Error('Failed')
      );
      store._open(function(err) {
        store._isOpen = true;
        open.restore();
        expect(open.called).to.equal(true);
        expect(err.message).to.equal('Failed');
        done();
      });
    });

  });

  describe('#_close', function() {

    it('should callback null if already closed', function(done) {
      store._isOpen = false;
      store._close(function(err) {
        expect(err).to.equal(null);
        done();
      });
    });

    it('should close the db if open', function(done) {
      store._isOpen = true;
      var close = sinon.stub(store._db, 'close').callsArgWith(0, null);
      store._close(function(err) {
        close.restore();
        expect(close.called).to.equal(true);
        expect(err).to.equal(null);
        done();
      });
    });

    it('should bubble error if db close fails', function(done) {
      store._isOpen = true;
      var close = sinon.stub(store._db, 'close').callsArgWith(
        0,
        new Error('Failed')
      );
      store._close(function(err) {
        store._isOpen = true;
        close.restore();
        expect(close.called).to.equal(true);
        expect(err.message).to.equal('Failed');
        done();
      });
    });

  });

});

after(function() {
  rimraf.sync(TMP_DIR);
});
