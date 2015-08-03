'use strict';

var kue = require('coyno-kue');
var log = require('coyno-log');
var mongo = require('coyno-mongo');
var TransactionProcessor = require('./processor');
var Dispatcher = require('coyno-dispatcher');
var async = require('async');
var assert = require('chai').assert;

require('../config');


var loadTransactions = function (addressStrings, userId, callback) {
    return mongo.db.collection('transfers').find({
      $and: [
        {'userId': userId},
        {
          $or: [{'details.inputs': {$elemMatch: {note: {$in: addressStrings}}}},
            {'details.outputs': {$elemMatch: {note: {$in: addressStrings}}}}]
        }]
    }).toArray(callback);
  };


mongo.start(function (err) {
  if (err) {
    throw err;
  }
  kue.jobs.process('addresses.connectTransactions', function (job, done) {
    if (!job.data.addresses) {
      log.error('Got job without addresses');
      return done('No addresses to work on.');
    }
    if (!job.data.userId) {
      log.error('Job without user id.');
      return done('Invalid User Id.');
    }
    if (!job.data.walletId) {
      log.error('Job without wallet id.');
      return done('No Wallet Id provided.');
    }
    var transactionProcessor = new TransactionProcessor({userId: job.data.userId});

    log.info({numAddresses: job.data.addresses.length}, 'Processing transactions ');
    try {
        loadTransactions(job.data.addresses, job.data.userId, function (err, transactions) {
        if (err) {
          log.error('Mongo Error');
          return done(err);
        }
          if (transactions.length < 1) {
            log.warn('No transactions found for addresses');
            return done(null);
          }
        transactionProcessor.process(transactions, function (err) {
          if (err) {
            log.error(err);
            return done(err);
          }
          Dispatcher.addresses.update({addresses: job.data.addresses, userId: job.data.userId});
          done(null);
        });
      });
    } catch(err) {
      log.error(err);
      return done(err);
    }
  });
});
