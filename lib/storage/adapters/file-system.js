'use strict';

var inherits = require('util').inherits;
var StorageAdapter = require('../adapter');
var levelup = require('levelup');
var leveldown = require('leveldown');
var path = require('path');
var assert = require('assert');
var utils = require('../../utils');
var mkdirp = require('mkdirp');
const fs = require('fs');

/**
 * Implements an LevelDB/FS storage adapter interface
 * @extends {StorageAdapter}
 * @param {String} storageDirPath - Path to store the level db
 * @constructor
 * @license AGPL-3.0
 */
function FileSystemStorageAdapter(storageDirPath) {
  if (!(this instanceof FileSystemStorageAdapter)) {
    return new FileSystemStorageAdapter(storageDirPath);
  }

  this._validatePath(storageDirPath);

  this._path = storageDirPath;
  this._db = levelup(leveldown(path.join(this._path, 'contracts.db')), {
    maxOpenFiles: FileSystemStorageAdapter.MAX_OPEN_FILES,
  });
  this._isOpen = true;
  this._fs = fs;
}

FileSystemStorageAdapter.SIZE_START_KEY = '0';
FileSystemStorageAdapter.SIZE_END_KEY = 'z';
FileSystemStorageAdapter.MAX_OPEN_FILES = 1000;

inherits(FileSystemStorageAdapter, StorageAdapter);

/**
 * Validates the storage path supplied
 * @private
 */
FileSystemStorageAdapter.prototype._validatePath = function(storageDirPath) {
  if (!utils.existsSync(storageDirPath)) {
    mkdirp.sync(storageDirPath);
  }

  assert(utils.isDirectory(storageDirPath), 'Invalid directory path supplied');
};

/**
 * Implements the abstract {@link StorageAdapter#_get}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._get = function(key, callback) {
  const self = this;

  this._db.get(key, { fillCache: false }, function(err, value) {
    if (err) {
      return callback(err);
    }

    const result = JSON.parse(value);

    const pathToFile = path.join(self._path, key);

    /* FS Exists is deprecated */
    self._fs.stat(pathToFile, function(notExists) {
      result.shard = notExists
        ? self._fs.createWriteStream(pathToFile)
        : self._fs.createReadStream(pathToFile);

      callback(null, result);
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_peek}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._peek = function(key, callback) {
  this._db.get(key, { fillCache: false }, function(err, value) {
    if (err) {
      return callback(err);
    }

    callback(null, JSON.parse(value));
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_put}
 * @private
 * @param {String} key
 * @param {Object} item
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._put = function(key, item, callback) {
  var self = this;

  item.shard = null; // NB: Don't store any shard data here

  item.fskey = key;

  self._db.put(
    key,
    JSON.stringify(item),
    {
      sync: true,
    },
    function(err) {
      if (err) {
        return callback(err);
      }

      callback(null);
    }
  );
};

/**
 * Implements the abstract {@link StorageAdapter#_del}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._del = function(key, callback) {
  var self = this;
  var fskey = key;

  self._peek(key, function(err, item) {
    if (!err && item.fskey) {
      fskey = item.fskey;
    }

    self._db.del(key, function(err) {
      if (err) {
        return callback(err);
      }

      self._fs.unlink(path.join(self._path, fskey), function(err) {
        if (err) {
          return callback(err);
        }

        callback(null);
      });
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._flush = function(callback) {
  callback();
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._size = function(key, callback) {
  var self = this;

  if (typeof key === 'function') {
    callback = key;
    key = null;
  }

  this._db.db.approximateSize(
    FileSystemStorageAdapter.SIZE_START_KEY,
    FileSystemStorageAdapter.SIZE_END_KEY,
    function(err, contractDbSize) {
      if (err) {
        return callback(err);
      }
      self._fs.readdir(self._path, function(err, files){
        if (err){
          return callback(err)
        }

        let totalSize = 0;

        files.forEach((key) => {
          const stat = self._fs.statSync(path.join(self._path, key))
          totalSize += stat.size;
        })

        callback(null, totalSize, contractDbSize);
      })
    }
  );
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
FileSystemStorageAdapter.prototype._keys = function() {
  return this._db.createKeyStream();
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._open = function(callback) {
  var self = this;

  if (!this._isOpen) {
    return this._db.open(function(err) {
      if (err) {
        return callback(err);
      }

      self._isOpen = true;
      callback(null);
    });
  }

  callback(null);
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
FileSystemStorageAdapter.prototype._close = function(callback) {
  var self = this;

  if (this._isOpen) {
    return this._db.close(function(err) {
      if (err) {
        return callback(err);
      }

      self._isOpen = false;
      callback(null);
    });
  }

  callback(null);
};

module.exports = FileSystemStorageAdapter;
