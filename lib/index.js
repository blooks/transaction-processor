'use strict';

var kue = require('coyno-kue');
var log = require('coyno-log');
var mongo = require('coyno-mongo');
var TransactionProcessor = require('./processor');
var Dispatcher = require('coyno-dispatcher');
require('../config');


var loadTransactions = function (addressStrings, userId, callback) {
  return mongo.db.collection('transfers').find({
    $and: [
      {'userId': userId},
      {
        $or: [{'details.inputs': {$elemMatch: {note: {$in: addressStrings}}}},
          {'details.outputs': {$elemMatch: {note: {$in: addressStrings}}}}]
      }]
  }).toArray(function(err, transactions) {
    if (err) {
      log.error(err);
      return callback(err);
    }
    callback(null, transactions);
  });
};

mongo.start(function(err) {
  if (err) {
    throw err;
  }
  kue.jobs.process('addresses.connectTransactions', function(job, done){
    if (!job.data.addresses) {
      log.error('Got job without addresses');
      return done('No addresses to work on.');
    }
    if (!job.data.userId) {
      log.error('Job without user id.');
      return done('Invalid User Id.');
    }
    var transactionProcessor = new TransactionProcessor({userId: job.data.userId});

    log.info({numAddresses: job.data.addresses.length}, 'Fetching transactions ');
    loadTransactions(job.data.addresses, job.data.userId, function(err, transactions) {
      if (err) {
        log.error('Mongo Error');
        return done(err);
      }
      else if (transactions.length < 1) {
        log.warn('No addresses found');
        return done('No addresses found.');
      }
      transactionProcessor.process(transactions, function(err) {
        if (err) {
          log.error(err);
          return done(err);
        }
        Dispatcher.addresses.update({addresses: job.data.addresses, userId: job.data.userId});
        done(null);
      });
    });
  });
});
