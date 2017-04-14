const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost/blooks-tx-proc-tests'

var TestDataManager = require('@blooks/test-data').Manager
var log = require('@blooks/log')
var Mongo = require('@blooks/mongo')
require('should')
var debug = require('debug')('coyno:transfers-tests')
var Q = require('q')
var _ = require('lodash')
var async = require('async')

var Processor = require('../lib/processor')
var Jobs = require('@blooks/jobs')

var testDataManager = new TestDataManager(MONGO_URL)
var mongo = new Mongo(MONGO_URL)

var generateJob = function () {
  var deferred = Q.defer()
  mongo.db.collection('transfers').find({}).toArray(function (err, transfers) {
    debug('Got transfers from mongo.', transfers)
    if (err) return deferred.reject(err)
    if (!(transfers.length > 0)) {
      deferred.reject(new Error('Generating Job for Coyno Transfer tests without transfers in DB.'))
    }
    var result = _.map(transfers, function (transfer) {
      return {
        id: transfer._id
      }
    })
    debug('Returning job.')
    deferred.resolve(result)
  })
  return deferred.promise
}

describe('Tests for Package Coyno Transfers', function () {
  before(function (done) {
    debug('Initialising DB.')
    async.series([
      testDataManager.start.bind(testDataManager),
      (callback) => {
        testDataManager.emptyDB([ 'wallets', 'transfers', 'addresses' ], callback)
      },
      (callback) => {
        testDataManager.fillDB([ 'wallets', 'transfers', 'addresses' ], callback)
      },
      mongo.start.bind(mongo)
    ], done)
  })
  after(function (done) {
    async.parallel([
      testDataManager.stop.bind(testDataManager),
      mongo.stop.bind(mongo)
    ], done)
  })
  describe('Unit tests', function () {
    before((done) => {
      done()
    })
    after((done) => {
      done()
    })
    it('should process transactions', (done) => {
      var transactions = testDataManager.getTransfers()
      var userId = transactions[0].userId
      var processor = new Processor({userId, mongoConnection: mongo})
      processor.process(transactions, done)
    })
  })
  describe('Integration tests', function () {
  })
})
