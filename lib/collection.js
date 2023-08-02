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

require("babel-polyfill");

var util = require('./util');
var async = require('async');
var _ = require('underscore');
var DataHandler = require('./dataHandler');
var matchCriteria = require('json-criteria-ext').test;

/**
 * Defines a Collection.
 * Holds access methods to the current collection.
 *
 * @param {object} options
 * @param {string} options.db the database object
 * @param {string} options.file the path of the collections
 * @constructor
 */
function Collection(options) {
  options = options || {};
  this._dataHandler = new DataHandler(options);
}

/**
 * Filters the collection using mongodb criteria
 *
 * @param criteria the criteria to match
 * @param {object} options
 * @param {boolean} options.multi returns multiple if matches
 */
Collection.prototype.find = function find(criteria, options) {
  return new Promise((resolve, reject) => {
    if (typeof options === 'function') {
      options = undefined;
    }
    if (!options) {
      options = {
        multi: true
      };
    }
    if (typeof criteria === 'function' || (criteria && Object.keys(criteria).length == 0)) {
      criteria = undefined;
    }
    var self = this;
    self._dataHandler.get(function afterReadFile(err, documents) {
      if (err) {
        reject(err);
      } else if (!criteria) {
        resolve(documents);
      } else {
        var filteredDocuments = [];
        async.each(
          documents,
          function filter(document, next) {
            if (matchCriteria(document, criteria)) {
              if (!options.multi) {
                return next(document);
              }
              filteredDocuments.push(document);
            }
            return next();
          },
          function afterFiltering(result) {
            if (_.isError(result)) {
              reject(result);
            } else if (result) {
              resolve(result);
            } else {
              resolve(filteredDocuments);
            }
          }
        );
      }
    });
  });
};

/**
 * Filters the collection using mongodb criteria and returns the first matched document
 *
 * @param criteria the criteria to match
 * @returns {*}
 */
Collection.prototype.findOne = function findOne(criteria) {
  return new Promise((resolve, reject) => {
    if (typeof criteria === 'function' || (criteria && Object.keys(criteria).length == 0)) {
      criteria = undefined;
    }
    if (!criteria) {
      reject(('No criteria specified!'));
    } else {
      resolve(this.find(criteria, { multi: false }))
    }
  });
};


/**
 * Filters the collection using mongodb criteria updates the document(s) and returns the changed ones
 *
 * @param criteria th criteria to match
 * @param updateCriteria the criteria to update
 * @param {object} options
 * @param {boolean} options.multi updates multiple if matches
 * @param {boolean} options.retObj true to return the changed object(s), returns an array with one or more match depending on options
 * @returns {*}
 */
Collection.prototype.findAndModify = function findAndModify(criteria, updateCriteria, options) {
  return new Promise((resolve, reject) => {
    if (typeof options === 'function') {
      options = undefined;
    }
    if (!options) {
      options = {
        multi: true,
        retObj: true
      };
    }
    if (typeof criteria === 'function' || (criteria && Object.keys(criteria).length == 0)) {
      criteria = undefined;
    }
    if (typeof updateCriteria === 'function') {
      updateCriteria = undefined;
    }
    if (!criteria) {
      reject(('No criteria specified!'));
    }
    if (!updateCriteria) {
      reject(('No update criteria specified!'));
    }
    var self = this;
    self.update(criteria, updateCriteria, options)
      .then((result) => resolve(options.retObj ? updateCriteria : result))
      .catch((error) => reject(error));
  });
};
/**
 * Inserts a new document in the collection
 *
 * @param data the object to insert
 */
Collection.prototype.insert = function insert(data) {
  return new Promise((resolve, reject) => {
    if (typeof data === 'function' || !data) {
      data = undefined;
    }
    if (!data) {
      reject(('No data passed to persist!'));
    }
    var self = this;
    // generate unique internal id for each document
    data._id = util.generateUUID();
    self._dataHandler.lock(function afterLockFile(err) {
      if (err) {
        reject(err);
      }
      self._dataHandler.get(function afterReadFile(err, documents) {
        if (err) {
          self._dataHandler.unlock();
          reject(err);
        } else {
          documents.push(data);
          if (_.isArray(data)) {
            documents = _.flatten(documents);
          }
          self._dataHandler.set(documents, function afterWriteFile(err) {
            self._dataHandler.unlock();
            if (err) {
              reject(err);
            } else {
              resolve(data);
            }
          });
        }
      });
    });
  });
};

/**
 * Updates documents based on mongodb criteria
 *
 * @param criteria the criteria to match
 * @param updateCriteria the update criteria
 * @param {object} options
 * @param {boolean} options.multi update multiple if matches
 * @param {boolean} options.upsert insert if no matches were found to update
 * @param {boolean} options.retObj true to return the changed object(s), returns an array with one or more match depending on options
 */
Collection.prototype.update = function update(criteria, updateCriteria, options) {
  return new Promise((resolve, reject) => {
    if (typeof options === 'function') {
      callback = options;
      options = undefined;
    }
    if (!options) {
      options = {
        upsert: false,
        multi: true,
        retObj: false
      };
    }
    if (typeof criteria === 'function') {
      callback = criteria;
      criteria = undefined;
    }
    if (typeof updateCriteria === 'function') {
      callback = updateCriteria;
      updateCriteria = undefined;
    }
    if (!criteria) {
      reject('No criteria specified!');
    }
    if (!updateCriteria) {
      reject('No update criteria specified!');
    }
    var ret = {
      nMatched: 0,
      nModified: 0,
      nUpserted: 0
    };
    var self = this;
    self._dataHandler.lock(function afterLockFile(err) {
      if (err) {
        reject(err);
      }
      self._dataHandler.get(function afterReadFile(err, documents) {
        if (err) {
          self._dataHandler.unlock();
          reject(err);
        } else {
          var matchedDocuments = [];
          for (var i = 0; i < documents.length; i++) {
            var document = documents[i];
            if (matchCriteria(document, criteria)) {
              for (var propertyName in updateCriteria) {
                if (updateCriteria[propertyName] instanceof Object) {
                  if ('$inc' in updateCriteria[propertyName]) {
                    // Handle $inc operator to increment or decrement the property value
                    var incValue = updateCriteria[propertyName]['$inc'];
                    if (typeof document[propertyName] === 'number' && typeof incValue === 'number') {
                      document[propertyName] += incValue;
                    } else {
                      reject((`Cannot increment/decrement non-numeric value or with non-numeric value!::propertyName: ${propertyName} . document[propertyName]: ${document[propertyName]} typeof ${typeof document[propertyName]} .current value: ${incValue}.type ${typeof incValue}`));
                      return;
                    }
                  } else {
                    // Handle other operators if needed in the future
                    // For now, we only handle $inc
                    reject((`Unsupported update operator: ${Object.keys(updateCriteria[propertyName])}`));
                    return;
                  }
                } else {
                  // Handle regular property updates
                  document[propertyName] = updateCriteria[propertyName];
                }
              }
              ret.nModified++;
              ret.nMatched++;
              matchedDocuments.push(document);
              if (!options.multi) {
                break; // Stop after updating the first matched document
              }
            }
          }
          if (matchedDocuments.length > 0) {
            self._dataHandler.set(documents, function afterWriteFile(err) {
              self._dataHandler.unlock();
              if (err) {
                reject(err);
              } else {
                resolve(options.retObj ? matchedDocuments : ret);
              }
            });
          } else if (!criteria || Object.keys(criteria).length === 0) {
            // Handle the case when criteria is empty or not provided
            // Update all documents if criteria is empty
            var updatedDocuments = documents.map(doc => {
              for (var propertyName in updateCriteria) {
                if (updateCriteria[propertyName] instanceof Object && '$inc' in updateCriteria[propertyName]) {
                  var incValue = updateCriteria[propertyName]['$inc'];
                  if (typeof doc[propertyName] === 'number' && typeof incValue === 'number') {
                    doc[propertyName] += incValue;
                  } else {
                    reject((`Cannot increment / decrement non - numeric value or with non - numeric value!`));
                    return;
                  }
                } else {
                  doc[propertyName] = updateCriteria[propertyName];
                }
              }
              return doc;
            });
            self._dataHandler.set(updatedDocuments, function afterWriteFile(err) {
              self._dataHandler.unlock();
              if (err) {
                reject(err);
              } else {
                ret.nMatched = documents.length;
                ret.nModified = documents.length;
                resolve(options.retObj ? updatedDocuments : ret);
              }
            });
          } else {
            self._dataHandler.unlock();
            if (options.upsert) {
              self.insert(updateCriteria)
                .then(() => {
                  ret.nUpserted = 1;
                  resolve(options.retObj ? updateCriteria : ret);
                })
                .catch((error) => reject(error));
            } else {
              resolve(options.retObj ? updateCriteria : ret);
            }
          }
        }
      });
    });
  });
};


/**
 * Removes documents based on mongodb criteria
 *
 * @param criteria the criteria to match
 * @param {object} options
 * @param {boolean} options.multi remove multiple if matches
 * @returns {*}
 */
Collection.prototype.remove = function remove(criteria, options) {
  return new Promise((resolve, reject) => {
    if (typeof options === 'function') {
      options = {
        multi: true
      };
    }
    if (typeof criteria === 'function' || (criteria && Object.keys(criteria).length == 0)) {
      criteria = undefined;
    }
    if (!criteria) {
      reject(('No criteria specified!'));
    }
    var self = this;
    self._dataHandler.lock(function afterLockFile(err) {
      if (err) {
        reject(err);
      }
      self._dataHandler.get(function afterReadFile(err, documents) {
        if (err) {
          self._dataHandler.unlock();
          reject(err);
        } else {
          var filteredDocuments;
          if (!criteria || Object.keys(criteria).length === 0) {
            // Handle the case when criteria is empty or not provided
            // Remove all documents if criteria is empty
            filteredDocuments = [];
          } else {
            filteredDocuments = documents.filter((document) => !matchCriteria(document, criteria));
          }
          self._dataHandler.set(filteredDocuments, function afterWriteFile(err) {
            self._dataHandler.unlock();
            if (err) {
              reject(err);
            } else {
              resolve(true);
            }
          });
        }
      });
    });
  });
};



/**
 * Counts the number of documents in the collection
 *
 * @param criteria the criteria to match
 */
Collection.prototype.count = function count(criteria) {
  return new Promise((resolve, reject) => {
    if (typeof criteria === 'function' || (criteria && Object.keys(criteria).length == 0)) {
      criteria = undefined;
    }
    var self = this;
    if (!criteria) {
      self._dataHandler.get(function afterReadFile(err, documents) {
        if (err) {
          reject(err);
        } else {
          resolve(documents.length);
        }
      });
    } else {
      self.find(criteria)
        .then((filteredDocuments) => resolve(filteredDocuments.length))
        .catch((error) => reject(error));
    }
  });
};
// ...

/**
 * Inserts multiple documents into the collection
 *
 * @param dataArr An array of objects to insert
 */
Collection.prototype.insertMany = function insertMany(dataArr) {
  return new Promise((resolve, reject) => {
    if (!Array.isArray(dataArr) || dataArr.length === 0) {
      reject('Data should be a non-empty array of objects!');
    }
    var self = this;
    self._dataHandler.lock(function afterLockFile(err) {
      if (err) {
        reject(err);
      }
      self._dataHandler.get(function afterReadFile(err, documents) {
        if (err) {
          self._dataHandler.unlock();
          reject(err);
        }
        dataArr.forEach(function (data) {
          data._id = util.generateUUID();
          documents.push(data);
        });
        self._dataHandler.set(documents, function afterWriteFile(err) {
          self._dataHandler.unlock();
          if (err) {
            reject(err);
          }
          resolve(dataArr);
        });
      });
    });
  })
};

/**
 * Returns an array of distinct values for the given field in the collection
 *
 * @param field The field for which distinct values are to be found
 */
Collection.prototype.distinct = function distinct(field) {
  return new Promise((resolve, reject) => {
    if (!field || typeof field !== 'string') {
      reject('Field should be a non-empty string!');
    }
    var self = this;
    self._dataHandler.get(function afterReadFile(err, documents) {
      if (err) {
        reject(err);
      }
      var distinctValues = _.chain(documents)
        .map(function (document) {
          return document[field];
        })
        .uniq()
        .compact()
        .value();
      resolve(distinctValues);
    });
  })
};

/**
 * Updates the first document that matches the criteria
 *
 * @param criteria The criteria to match
 * @param updateCriteria The update criteria
 */
Collection.prototype.updateOne = function updateOne(criteria, updateCriteria, options) {
  return new Promise((resolve, reject) => {
    options = options || {};
    options.multi = false; // Always update a single document for updateOne
    options.upsert = options.upsert || false;

    this.update(criteria, updateCriteria, options)
      .then((result) => {
        if (options.new && options.retObj) {
          resolve(result);
        } else if (options.new && !options.retObj && result.nModified > 0) {
          // If options.new is true and not returning the objects, find and return the updated document
          this.findOne(criteria)
            .then((updatedDocument) => resolve(updatedDocument))
            .catch((error) => reject(error));
        } else {
          resolve(result);
        }
      })
      .catch((error) => reject(error));
  });
};
// Collection.prototype.updateOne = function updateOne(criteria, updateCriteria) {
//   return new Promise((resolve, reject) => {
//     var options = {
//       multi: false
//     };
//     resolve(this.update(criteria, updateCriteria, options));
//   });
// };

/**
 * Updates all documents that match the criteria
 *
 * @param criteria The criteria to match
 * @param updateCriteria The update criteria
 */
Collection.prototype.updateMany = function updateMany(criteria, updateCriteria, options) {
  return new Promise((resolve, reject) => {
    options = options || {};
    options.multi = true; // Always update multiple documents for updateMany
    options.upsert = options.upsert || false;

    this.update(criteria, updateCriteria, options)
      .then((result) => {
        if (options.new && options.retObj) {
          resolve(result);
        } else if (options.new && !options.retObj && result.nModified > 0) {
          // If options.new is true and not returning the objects, find and return the updated documents
          this.find(criteria)
            .then((updatedDocuments) => resolve(updatedDocuments))
            .catch((error) => reject(error));
        } else {
          resolve(result);
        }
      })
      .catch((error) => reject(error));
  });
};
// Collection.prototype.updateMany = function updateMany(criteria, updateCriteria) {
//   return new Promise((resolve, reject) => {
//     this.update(criteria, updateCriteria, { multi: true })
//       .then(resolve)
//       .catch(reject);
//   });
// };


/**
 * Deletes the first document that matches the criteria
 *
 * @param criteria The criteria to match
 */
Collection.prototype.deleteOne = function deleteOne(criteria) {
  return new Promise((resolve, reject) => {
    this.remove(criteria, { multi: false })
      .then(resolve)
      .catch(reject);
  });
};

/**
 * Deletes all documents that match the criteria
 *
 * @param criteria The criteria to match
 */
Collection.prototype.deleteMany = function deleteMany(criteria) {
  return new Promise((resolve, reject) => {
    this.remove(criteria, { multi: true })
      .then(resolve)
      .catch(reject);
  });
};

// ...

module.exports = Collection;
