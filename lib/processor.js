'use strict';

require('../config');
var _ = require('lodash');
var async = require('async');
var log = require('coyno-log');
var mongo = require('coyno-mongo');
var Coynverter = require('coyno-converter');
var coynverter = new Coynverter(process.env.MONGO_URL);
var assert = require('chai').assert;
var Dispatcher = require('coyno-dispatcher');

var TransactionProcessor = function (doc) {
  if (!(this instanceof TransactionProcessor)) {
    return new TransactionProcessor(doc);
  }
  assert.isDefined(doc.userId);
  return _.extend(this, doc);
};

TransactionProcessor.prototype._saveWallets = function (callback) {
  var collection = mongo.db.collection('bitcoinwallets');
  assert.isAbove(this.affectedWalletObjects.length, 0, 'No wallets affected');
  async.forEach(this.affectedWalletObjects,
  function(wallet, callback) {
    async.map([{
        chain: 'main'
      }, {
        chain: 'change'
      }],
      function (params, callback) {
        var fieldPath = 'derivationParams.' + params.chain + '.lastUsed';
        var query = {
          $set: {}
        };
        query["$set"][fieldPath] = wallet.derivationParams[params.chain].lastUsed;
        log.debug({query: query});
        collection.update({_id: wallet._id}, query, callback);
      },
      callback
    );
  },function(err) {
      if (err) {
        log.error(err, 'Wallet save failed.');
        return callback(err);
      }
      this.affectedWalletObjects.forEach(function(wallet) {
        Dispatcher.wallet.update({walletId: wallet._id, userId: wallet.userId});
      });
      return callback(err);
    }.bind(this));
};

TransactionProcessor.prototype._saveTransactions = function (callback) {
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
        baseVolume: transaction.baseVolume,
        hidden: false
      }
    });
  });
  bulk.execute(callback);
};

TransactionProcessor.prototype.save = function (callback) {
  async.parallel([
    this._saveTransactions.bind(this),
    this._saveWallets.bind(this)
  ], callback);
};

TransactionProcessor.prototype.getAffectedAddressesObjects = function (callback) {
  log.debug('Getting affected addresses');
  return mongo.db.collection('bitcoinaddresses').find({
    address: {$in: this.affectedAddresses},
    userId: this.userId
  }).toArray(
    function (err, addresses) {
      if (err) {
        return callback(err);
      }
      this.affectedAddressesObjects = addresses;
      callback(null, addresses);
    }.bind(this)
  );
};

TransactionProcessor.prototype.getAffectedWalletObjects = function (affectedAddressesObjects, callback) {
  var walletIds = _(affectedAddressesObjects).pluck('walletId').uniq().value();
  log.debug({walletIds: walletIds},'Getting affected wallets.');
  return mongo.db.collection('bitcoinwallets').find({
    '_id': {$in: walletIds},
    'userId': this.userId
  }).toArray(function (err, wallets) {
      if (err) {
        return callback(err);
      }
      log.debug({wallets: wallets},'Found affected wallets.');
      assert.isArray(wallets);
      assert.isAbove(wallets.length, 0, 'No wallets found!');
      this.affectedWalletObjects = wallets;
      callback(err);
    }.bind(this)
  );
};

TransactionProcessor.prototype.process = function (transactions, callback) {
  this.transactions = transactions;
  this.wallets = [];
  this.affectedAddressesObjects = [];
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
      this.getAffectedAddressesObjects.bind(this),
      this.getAffectedWalletObjects.bind(this),
      this.getAddressesMapping.bind(this),
      this.connectNodes.bind(this),
      this.updateAttributes.bind(this)],
    function (err) {
      if (err) {
        log.error(err);
        return callback(err);
      }
      return this.save(callback);
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

TransactionProcessor.prototype.updateDerivationParams = function (address) {
  var addressObject = _.find(this.affectedAddressesObjects, {'address': address});
  if (addressObject.order === -1) {
    log.debug('Skipping derivation param update for single address');
    return;
  }
  assert.isDefined(addressObject.derivationParams);
  assert.isDefined(addressObject.derivationParams.chain);
  assert.isDefined(addressObject.derivationParams.order);
  var chain = addressObject.derivationParams.chain;
  var order = addressObject.derivationParams.order;
  var wallet = _.find(this.affectedWalletObjects, {'_id': addressObject.walletId});
  assert.isDefined(wallet._id);
  wallet.derivationParams[chain].lastUsed = parseInt(Math.max(order, wallet.derivationParams[chain].lastUsed), 10);
  log.debug({wallet: wallet}, 'Updated wallet derivation params');
};

TransactionProcessor.prototype.connectNodes = function (addressesToIdsMapping, callback) {
  log.debug('Connecting nodes.');
  var self = this;
  async.each(this.transactions, function (transaction, callback) {
    transaction.details.inputs.forEach(function (input) {
      if (addressesToIdsMapping[input.note]) {
        input.nodeId = addressesToIdsMapping[input.note];
        self.updateDerivationParams(input.note);
      }
    });
    transaction.details.outputs.forEach(function (output) {
      if (addressesToIdsMapping[output.note]) {
        output.nodeId = addressesToIdsMapping[output.note];
        self.updateDerivationParams(output.note);
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
