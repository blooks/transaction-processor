'use strict'

var _ = require('lodash')
var async = require('async')
var log = require('@blooks/log')
var assert = require('chai').assert


function sumAmountReduce (total, inoutput) {
  return total + inoutput.amount
}

class TransactionProcessor {
  constructor ({userId, mongoConnection, jobs, converter}) {
    this._mongo = mongoConnection
    this._jobs = jobs
    this._converter = converter
    this._userId = userId
  }

  _updateInOutputs (transaction, callback) {
  async.parallel({
    inputs: (callback) => {
      async.map(transaction.details.inputs, this._mapWalletInfo.bind(this), callback)
    },
    outputs: (callback) => {
      async.map(transaction.details.outputs, this._mapWalletInfo.bind(this), callback)
    }
  }, callback)
}
  _mapWalletInfo (inoutput, callback) {
    this._mongo.db.collection('bitcoinaddresses').findOne({ _id: inoutput.nodeId }, (err, doc) => {
      if (err || !doc) {
        return callback(err, inoutput)
      }
      this._mongo.db.collection('bitcoinwallets').findOne({ _id: doc.walletId }, (err, doc) => {
        if (err || !doc) {
          return callback(err, inoutput)
        }
        callback(null, _.extend({}, inoutput, { wallet: { id: doc._id, label: doc.label } }))
      })
    })
  }

  _saveWallets (callback) {
    var collection = this._mongo.db.collection('bitcoinwallets')
    assert.isAbove(this.affectedWalletObjects.length, 0, 'No wallets affected')
    async.forEach(this.affectedWalletObjects,
      (wallet, callback) => {
        async.map([ {
            chain: 'main'
          }, {
            chain: 'change'
          } ],
          (params, callback) => {
            var fieldPath = 'derivationParams.' + params.chain + '.lastUsed'
            var query = {
              $set: {}
            }
            query[ '$set' ][ fieldPath ] = wallet.derivationParams[ params.chain ].lastUsed
            log.debug({ query: query })
            collection.update({ _id: wallet._id }, query, callback)
          },
          callback
        )
      }, (err) => {
        if (err) {
          log.error(err, 'Wallet save failed.')
          return callback(err)
        }
        if (this._jobs) {
          return async.eachSeries(this.affectedWalletObjects,
            (wallet, callback) => {
            this._jobs.addJob('wallet.update',{ walletId: wallet._id, userId: wallet.userId }, callback)
          }, callback)
        } else {
          log.warn('No jobs object defined. Not triggering further wallet update jobs.')
        }
        return callback(err)
      })
  }

  _saveTransactions (callback) {
    log.debug('Saving Transactions to database')
    var collection = this._mongo.db.collection('transfers')
    var bulk = collection.initializeUnorderedBulkOp()
    this.transactions.forEach((transaction) => {
      bulk.find({ _id: transaction._id }).update({
        $set: {
          'details.inputs': transaction.details.inputs,
          'details.outputs': transaction.details.outputs,
          updatedAt: transaction.updatedAt,
          representation: transaction.representation,
          baseVolume: transaction.baseVolume,
          hidden: false
        }
      })
    })
    bulk.execute(callback)
  }

  save (callback) {
    async.parallel([
      this._saveTransactions.bind(this),
      this._saveWallets.bind(this)
    ], callback)
  }

  getAffectedAddressesObjects (callback) {
    log.debug('Getting affected addresses')
    return this._mongo.db.collection('bitcoinaddresses').find({
      address: { $in: this.affectedAddresses },
      userId: this._userId
    }).toArray(
      (err, addresses) => {
        if (err) {
          return callback(err)
        }
        this.affectedAddressesObjects = addresses
        callback(null, addresses)
      }
    )
  }

  getAffectedWalletObjects (affectedAddressesObjects, callback) {
    var walletIds = _(affectedAddressesObjects).map('walletId').uniq().valueOf()
    log.debug({ walletIds: walletIds }, 'Getting affected wallets.')
    return this._mongo.db.collection('bitcoinwallets').find({
      '_id': { $in: walletIds },
      'userId': this._userId
    }).toArray((err, wallets) => {
      if (err) {
        return callback(err)
      }
      log.debug({ wallets: wallets }, 'Found affected wallets.')
      assert.isArray(wallets)
      assert.isAbove(wallets.length, 0, 'No wallets found!')
      this.affectedWalletObjects = wallets
      callback(err)
    })
  }

  process (transactions, callback) {
    this.transactions = transactions
    this.wallets = []
    this.affectedAddressesObjects = []
    // Get a list of all addresses touched by these transactions
    this.affectedAddresses = _.chain(transactions).map((transaction) => {
      return _.chain([ transaction.details.inputs, transaction.details.outputs ])
        .flatten(true)
        .map('note')
        .valueOf()
    })
      .flatten()
      .uniq()
      .valueOf()

    async.waterfall(
      [ this.getAffectedAddressesObjects.bind(this),
        this.getAffectedWalletObjects.bind(this),
        this.getAddressesMapping.bind(this),
        this.connectNodes.bind(this),
        this.updateAttributes.bind(this) ],
      (err) => {
        if (err) {
          log.error(err)
          return callback(err)
        }
        return this.save(callback)
      })
  }

  getAddressesMapping (callback) {
    return this._mongo.db.collection('bitcoinaddresses').find({
      $and: [
        { userId: this._userId },
        { address: { $in: this.affectedAddresses } }
      ]
    }, { address: 1 }).toArray((err, addresses) => {
      if (err) {
        log.error(err)
        return callback(err)
      }

      var addressToIdMap = _.zipObject(_.map(addresses, 'address'), _.map(addresses, '_id'))
      return callback(null, addressToIdMap)
    })
  }

  updateDerivationParams (address) {
    var addressObject = _.find(this.affectedAddressesObjects, { 'address': address })
    if (addressObject.order === -1) {
      log.debug('Skipping derivation param update for single address')
      return
    }
    assert.isDefined(addressObject.derivationParams)
    assert.isDefined(addressObject.derivationParams.chain)
    assert.isDefined(addressObject.derivationParams.order)
    var chain = addressObject.derivationParams.chain
    var order = addressObject.derivationParams.order
    var wallet = _.find(this.affectedWalletObjects, { '_id': addressObject.walletId })
    assert.isDefined(wallet._id)
    wallet.derivationParams[ chain ].lastUsed = parseInt(Math.max(order, wallet.derivationParams[ chain ].lastUsed), 10)
    log.debug({ wallet: wallet }, 'Updated wallet derivation params')
  }

  connectNodes (addressesToIdsMapping, callback) {
    log.debug('Connecting nodes.')
    var self = this
    async.each(this.transactions, (transaction, callback) => {
      transaction.details.inputs.forEach((input) => {
        if (addressesToIdsMapping[ input.note ]) {
          input.nodeId = addressesToIdsMapping[ input.note ]
          self.updateDerivationParams(input.note)
        }
      })
      transaction.details.outputs.forEach((output) => {
        if (addressesToIdsMapping[ output.note ]) {
          output.nodeId = addressesToIdsMapping[ output.note ]
          self.updateDerivationParams(output.note)
        }
      })
      callback()
    }, callback)
  }

  updateAttributes (callback) {
    async.eachSeries(this.transactions, (transaction, callback) => {
      transaction.updatedAt = this.date
      this._updateInOutputs(transaction, (err, details) => {
        if (err) {
          return callback(err)
        }

        // TODO: check the reasoning behind those with multiple sources (coin mix)

        var senderNode = (_(details.inputs).filter('wallet').first() || {}).wallet
        var recipientNode = (_(details.outputs).filter((output) => {
          return output.wallet && output.wallet.id !== (senderNode && senderNode.id)
        }).first() || {}).wallet

        var inputsValue = _.reduce(transaction.details.inputs, sumAmountReduce, 0)
        var outputsValue = _.reduce(transaction.details.outputs, sumAmountReduce, 0)
        // Add all outputs that do not go to this wallet.
        var amount = details.outputs.reduce((total, output) => {
          if ((!senderNode && !output.wallet) ||
            (senderNode && output.wallet && output.wallet.id === senderNode.id)) {
            return total
          }
          return total + output.amount
        }, 0)
        // Substract all inputs that do not come from this wallet
        amount -= details.inputs.reduce((total, input) => {
          if ((!senderNode && !input.wallet) ||
            (senderNode && input.wallet && input.wallet.id === senderNode.id)) {
            return total
          }
          return total + input.amount
        }, 0)

        // all inputs and outputs are from user wallet
        if (senderNode && !recipientNode &&
          _.filter(details.inputs, 'wallet').length === details.inputs.length &&
          _.filter(details.outputs, 'wallet').length === details.outputs.length) {
          recipientNode = details.outputs[ 0 ].wallet
          amount = outputsValue
        }

        transaction.representation = {
          fee: inputsValue - outputsValue,
          senderLabels: [ senderNode ? senderNode.label : 'External' ],
          recipientLabels: [ recipientNode ? recipientNode.label : 'External' ],
          type: senderNode ?
            (recipientNode ? 'internal' : 'outgoing') :
            (recipientNode ? 'incoming' : 'orphaned'),
          amount: amount
        }

        var date = new Date(transaction.date)
        if (this._converter) {
          return async.mapSeries([ 'EUR', 'USD' ], (currency, callback) => {
            this._converter.convert('XBT', currency, amount, date, (err, resp) => {
              var exchangeRate = {}
              exchangeRate[ currency ] = Math.round(resp)
              callback(null, exchangeRate)
            })
          }, (err, baseVolume) => {
            if (err) {
              return callback(err)
            }

            transaction.baseVolume = baseVolume

            callback()
          })
        }
        return callback()
      })
    }, callback)
  }

}

module.exports = TransactionProcessor
