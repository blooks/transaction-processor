const superagent = require('superagent')

const convert = (fromAsset, toCurrency, amount, date, callback) => {
  superagent.get(`https://api.blooks.io/prices/v1/${toCurrency}`).type('json').query({
    date
  }).then(({body}) => {
    const fromAssetPrice = body.prices.find(price => {
      return price.currency === fromAsset
    }).price
    callback(null, amount * fromAssetPrice)
  }).catch(callback)
}

const converter = {
  convert
}

module.exports = converter
