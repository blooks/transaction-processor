'use strict'
var TransactionProcessor = require('./index.js')
var log = require('@blooks/log')

var redisUrl = process.env.REDIS_URL
var mongoUrl = process.env.MONGO_URL
var redisHost = process.env.REDIS_HOST
var redisPort = process.env.REDIS_PORT
if (redisHost && redisPort) {
  redisUrl = 'redis://' + redisHost + ':' + redisPort
}
if (!redisUrl || !mongoUrl) {
  throw new Error('Need to set MONGO_URL and REDIS_URL as env variables.')
}

var transactionFetcher = new TransactionProcessor(mongoUrl, redisUrl)
transactionFetcher.start(function (err) {
  if (err) {
    return log.error(err)
  }
  log.info('Transaction Fetcher started.')
})
