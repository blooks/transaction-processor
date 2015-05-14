'use strict';

require('../config');
var _ = require('lodash');
var async = require('async');
var log = require('coyno-log').child({component: 'TransactionProcessor'});
var mongo = require('coyno-mongo');
var Coynverter = require('coyno-converter');
var coynverter = new Coynverter(process.env.MONGO_URL);


var TransactionProcessor = function (doc) {
  if (!(this instanceof TransactionProcessor)) {
    return new TransactionProcessor(doc);
  }
  return _.extend(this, doc);
};

TransactionProcessor.prototype.saveTransactionsToMongo = function (callback) {
  log.debug('Saving Transactions to database');
  var collection = mongo.db.collection('transfers');
  var bulk = collection.initializeUnorderedBulkOp();
  this.transactions.forEach(function (transaction) {
    bulk.find({_id: transaction._id}).update({
      $set: {
        'details.inputs': transaction.details.inputs,
        'details.outputs': transaction.details.outputs,
        updatedAt: transaction.updatedAt,
        representation: transaction.representation,
        baseVolume: transaction.baseVolume
      }
    });
  });
  bulk.execute(callback);
};

TransactionProcessor.prototype.process = function (transactions, callback) {
  this.transactions = transactions;
  //Get a list of all addresses touched by these transactions
  this.affectedAddresses = _(transactions).invoke(function () {
    return _([this.details.inputs, this.details.outputs])
      .flatten(true)
      .pluck('note')
      .valueOf();
  })
    .flatten()
    .uniq()
    .valueOf();

  async.waterfall([
      this.getAddressesMapping.bind(this),
      this.connectNodes.bind(this),
      this.updateAttributes.bind(this)],
    function (err, result) {
      if (err) {
        log.error(err);
        return callback(err);
      }
      return this.saveTransactionsToMongo(callback);
    }.bind(this));
};

TransactionProcessor.prototype.getAddressesMapping = function (callback) {
  return mongo.db.collection('bitcoinaddresses').find({
    $and: [
      {userId: this.userId},
      {address: {$in: this.affectedAddresses}}
    ]
  }, {address: 1}).toArray(function (err, addresses) {
    if (err) {
      log.error(err);
      return callback(err);
    }

    var addressToIdMap = _.zipObject(_.pluck(addresses, 'address'), _.pluck(addresses, '_id'));
    return callback(null, addressToIdMap);
  }.bind(this));
};

TransactionProcessor.prototype.connectNodes = function (addressesToIdsMapping, callback) {
  async.each(this.transactions, function (transaction, callback) {
    transaction.details.inputs.forEach(function (input) {
      if (addressesToIdsMapping[input.note]) {
        input.nodeId = addressesToIdsMapping[input.note];
      }
    });
    transaction.details.outputs.forEach(function (output) {
      if (addressesToIdsMapping[output.note]) {
        output.nodeId = addressesToIdsMapping[output.note];
      }
    });
    callback();
  }.bind(this), callback);
};


function mapWalletInfo(inoutput, callback) {
  mongo.db.collection('bitcoinaddresses').findOne({_id: inoutput.nodeId}, function (err, doc) {
    if (err || !doc) {
      return callback(err, inoutput);
    }
    mongo.db.collection('bitcoinwallets').findOne({_id: doc.walletId}, function (err, doc) {
      if (err || !doc) {
        return callback(err, inoutput);
      }

      callback(null, _.extend({}, inoutput, {wallet: {id: doc._id, label: doc.label}}));
    });
  });
}

function updateInOutputs(transaction, callback) {
  async.parallel({
    inputs: function (callback) {
      async.map(transaction.details.inputs, mapWalletInfo, callback);
    },
    outputs: function (callback) {
      async.map(transaction.details.outputs, mapWalletInfo, callback);
    }
  }, callback);
}

function sumAmountReduce(total, inoutput) {
  return total + inoutput.amount;
}


TransactionProcessor.prototype.updateAttributes = function (callback) {
  async.eachSeries(this.transactions, function (transaction, callback) {
    transaction.updatedAt = this.date;
    updateInOutputs(transaction, function (err, details) {
      if (err) {
        return callback(err);
      }

      //TODO: check the reasoning behind those with multiple sources (coin mix)

      var senderNode = (_(details.inputs).filter('wallet').first() || {}).wallet;
      var recipientNode = (_(details.outputs).filter(function (output) {
        return output.wallet && output.wallet.id !== (senderNode && senderNode.id);
      }).first() || {}).wallet;

      var inputsValue = _.reduce(transaction.details.inputs, sumAmountReduce, 0);
      var outputsValue = _.reduce(transaction.details.outputs, sumAmountReduce, 0);
      //Add all outputs that do not go to this wallet.
      var amount = details.outputs.reduce(function (total, output) {
        if ((!senderNode && !output.wallet) ||
          (senderNode && output.wallet && output.wallet.id === senderNode.id)) {
          return total;
        }
        else {
          return total + output.amount;
        }
      }, 0);
      //Substract all inputs that do not come from this wallet
      amount -= details.inputs.reduce(function (total, input) {
        if ((!senderNode && !input.wallet) ||
          (senderNode && input.wallet && input.wallet.id === senderNode.id)) {
          return total;
        }
        else {
          return total + input.amount;
        }
      }, 0);

      // all inputs and outputs are from user wallet
      if (senderNode && !recipientNode &&
        _.filter(details.inputs, 'wallet').length === details.inputs.length &&
        _.filter(details.outputs, 'wallet').length === details.outputs.length) {

        recipientNode = details.outputs[0].wallet;
        amount = outputsValue;
      }

      transaction.representation = {
        fee: inputsValue - outputsValue,
        senderLabels: [senderNode ? senderNode.label : 'External'],
        recipientLabels: [recipientNode ? recipientNode.label : 'External'],
        type: senderNode ?
          (recipientNode ? 'internal' : 'outgoing') :
          (recipientNode ? 'incoming' : 'orphaned'),
        amount: amount
      };

      var date = new Date(transaction.date);
      async.mapSeries(['EUR', 'USD'], function (currency, callback) {
        coynverter.convert('BTC', currency, amount, date, function (err, resp) {
          var exchangeRate = {};
          exchangeRate[currency] = Math.round(resp);
          callback(null, exchangeRate);
        });
      }.bind(this), function (err, baseVolume) {
        if (err) {
          return callback(err);
        }

        transaction.baseVolume = baseVolume;

        callback();
      });
    }.bind(this));
  }.bind(this), callback);
};


module.exports = TransactionProcessor;
