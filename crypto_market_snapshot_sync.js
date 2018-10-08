/**
* second (0-59)
* minute (0-59)
* hour (0-23)
* date (1-31)
* month (0-11)
* year
* dayOfWeek (0-6) Starting with Sunday
* ex: rule.dayOfWeek = [0, new schedule.Range(4, 6)]
*
*/

var config = require('config')
var dbConfig = config.get('DBConfig')
var mysql = require('mysql2/promise')

var schedule = require('node-schedule')
var request = require('async-request')

console.log('Crypto Market Snapshot Sync Task Started! ')

/**
 * Main Task Function A: Crypto market data snapshot task pipeline
 */
var snapshotSyncTask = async () => {
  console.log('Main Task Function: Crypto market data snapshot task pipeline start ...')
  console.log('time: ', new Date().toTimeString())
  // A-0: Check DB Connection
  console.log(dbConfig)
  const dbConn = await mysql.createConnection(dbConfig)
  console.log('DB Connected √')

  // A-1 get Snapshot data
  console.log('retriving latest snapshot data . . .')
  var snapshotData = await getLatestSnapshot()
  console.log('√ snapshot data received')
//   console.log(snapshotData)
  // A-2
  console.log('Inserting snapshot into DB . . . ')
  await snapshotDBSave(snapshotData, dbConn)
  console.log('√ DB Insert completed!')
  dbConn.end()
  console.log('√ DB connection ended!')
}

/**
 * function A-1: Sync Cryto market snapshot
 */
var getLatestSnapshot = async () => {
  console.log('start query market data from coinmarketcap')
  try {
    let url = 'https://api.coinmarketcap.com/v1/ticker/?convert=CNY&limit=200'
    let response = await request(url)
    // success
    if (response.statusCode === 200) {
    //   console.log(response.body)
      return JSON.parse(response.body)
    }
  } catch (e) {
    console.log(e)
  }
}

/**
 * Function A-2: save snapshot to database
 */

/**
 * sample data: {
        "id": "streamr-datacoin",
        "name": "Streamr DATAcoin",
        "symbol": "DATA",
        "rank": "200",
        "price_usd": "0.0502058",
        "price_btc": "0.00000724",
        "24h_volume_usd": "119434.0",
        "market_cap_usd": "33997084.0",
        "available_supply": "677154514.0",
        "total_supply": "987154514.0",
        "max_supply": null,
        "percent_change_1h": "-1.43",
        "percent_change_24h": "-2.14",
        "percent_change_7d": "-24.28",
        "last_updated": "1522654156",
        "price_cny": "0.3159953052",
        "24h_volume_cny": "751717.596",
        "market_cap_cny": "213977647.0"
    }
 */
var snapshotDBSave = async (snapshotData, dbConn) => {
    // start insert snapshot into MySQL
  if (snapshotData.length < 1) { return console.log('empty data') }
  let formedData = []
  // prepare key arrays - add createdAt
  let keysArray = Object.keys(snapshotData[0])
  keysArray.push('createdAt')
//   console.log(keysArray.join(', '))

//   prepare value arrays
  for (var i = 0; i < snapshotData.length; i++) {
//   for (var i = 0; i < 1; i++) {
    let row = Object.values(snapshotData[i])
    row.push(new Date().toLocaleString())
    formedData.push(row)
  }
//   console.log(formedData)

  // Insert into DB
  try {
    let output = await dbConn.query('INSERT INTO `crypto_market_snapshot` (' + keysArray.join(',') + ') VALUES ?', [formedData])
    // console.log(output)
  } catch (e) {
    console.log(e)
  }
}

/**
 * SnapshotSync Job
 * rule: every 2 hours
 */

var rule = new schedule.RecurrenceRule()
rule = '0 */20 * * * *' // production: 20 mins
// rule = '*/10 * * * * *' // development: 20s
var snapshotSyncJob = schedule.scheduleJob(rule, snapshotSyncTask)
