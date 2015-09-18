/**
 * (C) Copyright 2015 Manuel Martins.
 *
 * This module is inspired by json_file_system.
 * (json_file_system is Copyright (c) 2014 Jalal Hejazi,
 *  Licensed under the MIT license.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Created by: ManuelMartins
 * Created on: 11-09-2015
 *
 */

'use strict';

var path = require('path'),
  _ = require('underscore'),
  async = require('async'),
  Collection = require('./lib/collection'),
  resources = require('./lib/resources.json'),
  IOHandler = require('./lib/ioHandler');

/**
 * JSON DB FS Main entry point
 *
 * @param {Object} options
 *        {String} options.path
 *        {String} options.inMemory
 * @constructor
 */
function JSONDBFS(options) {
  options = options || {};
  this._path = options.path || '/tmp/';
  this._inMemory = options.inMemory || false;
  this._ioHandler = new IOHandler(options);
}

/**
 * Initializes a connection to the specified collections.
 *
 * @param collections
 * @param callback
 */
JSONDBFS.prototype.connect = function (collections, callback) {
  var self = this;
  self._db = {};
  async.waterfall([
    function validatePath(next) {
      self._ioHandler.pathExists(self._path, function afterCheck(exists) {
        if (!exists) {
          console.log(resources.CONNECTION.INVALID_PATH + self._path);
          return callback(new Error(resources.CONNECTION.INVALID_PATH + self._path));
        }
        self._db.path = self._path;
        self._db._inMemory = self._inMemory;
        self._db._ioHandler = self._ioHandler;
        return next();
      });
    },
    function createCollections() {
      if (_.isArray(collections)) {
        async.each(collections, function create(collection, next) {
          var fullPath = path.join(self._path, collection + '.json');
          console.log(resources.CONNECTION.COLLECTION_CREATED + collection);
          self[collection] = new Collection({db: self, path: fullPath});
          self._ioHandler.pathExists(fullPath, function afterCheck(exists) {
            if (!exists) {
              self._ioHandler.writeFile(fullPath, function afterWriteFile(err) {
                return next(err);
              });
            } else {
              // the file exists we're good to go
              return next();
            }
          });
        }, function finalize(err) {
          if (err) {
            console.log(resources.CONNECTION.INVALID_COLLECTION_NAME);
            return callback(err);
          } else {
            console.log(resources.CONNECTION.SUCCESS + self._path);
            return callback(null, self);
          }
        });
      } else {
        console.log(resources.CONNECTION.INVALID_COLLECTION_NAME);
        return callback(new Error(resources.CONNECTION.INVALID_COLLECTION_NAME));
      }
    }
  ]);
};

module.exports = JSONDBFS;