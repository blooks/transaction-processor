

var TestDataManager = require('coyno-mockup-data').Manager;

var mongo = require('coyno-mongo');
var should = require('should');
var debug = require('debug')('coyno:transfers-tests');
var Q  = require('q');
var _ = require('lodash');

var testDataManager = new TestDataManager();


var generateJob = function() {
  var deferred = Q.defer();
  mongo.db.collection('transfers').find({}).toArray(function(err, transfers) {
    debug('Got transfers from mongo.', transfers);
    if (err) return deferred.reject(err);
    if (!(transfers.length > 0)) {
      deferred.reject(new Error('Generating Job for Coyno Transfer tests without transfers in DB.'));
    }
    var result = _.map(transfers, function(transfer) {
      return {
        id: transfer._id
      }
    });
    debug('Returning job.');
    deferred.resolve(result);
  });
  return deferred.promise;
};

describe('Tests for Package Coyno Transfers', function() {
  before(function(done){
    debug('Initialising DB.');
    testDataManager.initDB(function(err) {
      if (err) {
        return done(err);
      }
      debug('Filling DB.');
      testDataManager.fillDB(['wallets','transfers'], done);
    });
  });
  after(function(done) {
    testDataManager.closeDB(done);
  });
  describe('Unit tests', function () {
  });
  describe('Integration tests', function () {
    before(function(done){
      generateJob().then(function(job) {
        debug('Job', job);
      }).done(done);
    });
    describe('Update bitcoin wallet', function () {
      it('should update all transactions for bitcoin wallet', function (done) {
        done();
      });
    });
    describe('Update and add trades', function () {
      it('should print a lot of addresses', function (done) {
        done(null);
      });
    });
  })
});

