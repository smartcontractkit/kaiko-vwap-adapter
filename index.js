const request = require('request-promise')

const createRequest = async (input, callback) => {
  const coin = input.data.coin || 'eth'
  const options = {
    headers: {
      'X-Api-Key': process.env.API_KEY
    },
    qs: {
      interval: '1d',
      limit: '1'
    },
    json: true
  }
  const markets = await getMarkets(coin)
  const rawResults = await queryMarkets(coin, markets, options)
  const conversions = await getConversions(markets, options)
  const vwap = calculateResults(rawResults, conversions)
  const allData = {
    results: rawResults,
    conversions: conversions,
    result: vwap
  }
  callback(200, {
    jobRunID: input.id,
    data: allData,
    result: vwap,
    statusCode: 200
  })
}

// Get all the markets for a given coin
const getMarkets = async (coin) => {
  const quotes = []
  const allMarkets = await request({
    url: 'https://reference-data-api.kaiko.io/v1/instruments',
    json: true
  })
  // If base_asset is the coin we want, add the quote_asset to the array
  for (let i = 0; i < allMarkets.data.length; i++) {
    if (allMarkets.data[i].base_asset === coin.toLowerCase()) {
      quotes.push(allMarkets.data[i].quote_asset)
    }
  }
  return [...new Set(quotes)] // remove duplicates
}

// Get the rate for each market of a given coin
const queryMarkets = async (coin, markets, options) => {
  const responses = []
  const baseUrl = 'https://us.market-api.kaiko.io/v1/data/trades.v1/spot_direct_exchange_rate/'
  for (let i = 0; i < markets.length; i++) {
    const url = baseUrl + coin.toLowerCase() + '/' + markets[i].toLowerCase() + '/recent'
    options.url = url
    responses.push(await request(options))
  }
  return responses
}

// Get the rate in USD for non-USD markets
const getConversions = async (markets, options) => {
  const conversions = {}
  const baseUrl = 'https://us.market-api.kaiko.io/v1/data/trades.v1/spot_direct_exchange_rate/'
  for (let i = 0; i < markets.length; i++) {
    if (markets[i] !== 'usd') {
      const url = baseUrl + markets[i].toLowerCase() + '/usd/recent'
      options.url = url
      conversions[markets[i]] = await request(options)
    }
  }
  return conversions
}

// Get the VWAP of all markets converted to USD
const calculateResults = (rawResults, conversions) => {
  const prices = []
  const volumes = []
  for (let i = 0; i < rawResults.length; i++) {
    // Convert the price and volume to USD
    if (rawResults[i].query.quote_asset !== 'usd') {
      const price = +rawResults[i].data[0].price * +conversions[rawResults[i].query.quote_asset].data[0].price
      const volume = +rawResults[i].data[0].volume * +conversions[rawResults[i].query.quote_asset].data[0].volume
      prices.push(price)
      volumes.push(volume)
    } else {
      prices.push(+rawResults[i].data[0].price)
      volumes.push(+rawResults[i].data[0].volume)
    }
  }
  return weightedMean(prices, volumes)
}

const weightedMean = (prices, volumes) => {
  return sumArrayValues(prices.map((price, index) => price * volumes[index])) / sumArrayValues(volumes)
}

const sumArrayValues = (values) => {
  return values.reduce((p, c) => p + c, 0)
}

exports.gcpservice = (req, res) => {
  createRequest(req.body, (statusCode, data) => {
    res.status(statusCode).send(data)
  })
}

exports.handler = (event, context, callback) => {
  createRequest(event, (statusCode, data) => {
    callback(null, data)
  })
}

exports.handlerv2 = (event, context, callback) => {
  createRequest(JSON.parse(event.body), (statusCode, data) => {
    callback(null, {
      statusCode: statusCode,
      body: JSON.stringify(data),
      isBase64Encoded: false
    })
  })
}

module.exports.createRequest = createRequest
