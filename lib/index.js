'use strict'

var kue = require('kue')
var log = require('@blooks/log')
var Mongo = require('@blooks/mongo')
var TransactionProcessor = require('./processor')
var Jobs = require('@blooks/jobs')

class TransactionProcessorService {
  constructor (mongoUrl, redisUrl) {
    this._mongo = new Mongo(mongoUrl)
    this._jobs = new Jobs(redisUrl)
    this._queue = kue.createQueue({
      redis: redisUrl
    })
  }
  _loadTransactions (addressStrings, userId, callback) {
    return this._mongo.db.collection('transfers').find({
      $and: [
        { 'userId': userId },
        {
          $or: [ { 'details.inputs': { $elemMatch: { note: { $in: addressStrings } } } },
            { 'details.outputs': { $elemMatch: { note: { $in: addressStrings } } } } ]
        } ]
    }).toArray(callback)
  }
  start (callback) {
    this._mongo.start((err) => {
      if (err) {
        throw err
      }
      this._queue.jobs.process('addresses.connectTransactions', (job, done) => {
        if (!job.data.addresses) {
          log.error('Got job without addresses')
          return done('No addresses to work on.')
        }
        if (!job.data.userId) {
          log.error('Job without user id.')
          return done('Invalid User Id.')
        }
        if (!job.data.walletId) {
          log.error('Job without wallet id.')
          return done('No Wallet Id provided.')
        }
        var transactionProcessor = new TransactionProcessor({ userId: job.data.userId })

        log.info({ numAddresses: job.data.addresses.length }, 'Processing transactions ')
        try {
          this._loadTransactions(job.data.addresses, job.data.userId, (err, transactions) => {
            if (err) {
              log.error('Mongo Error')
              return done(err)
            }
            if (transactions.length < 1) {
              log.warn('No transactions found for addresses')
              return done(null)
            }
            transactionProcessor.process(transactions, (err) => {
              if (err) {
                log.error(err)
                return done(err)
              }
              this._jobs.addresses.update({ addresses: job.data.addresses, userId: job.data.userId })
              done(null)
            })
          })
        } catch (err) {
          log.error(err)
          return done(err)
        }
      })
      callback()
    })
  }
}

module.exports = TransactionProcessorService
