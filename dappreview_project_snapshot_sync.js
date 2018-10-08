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
var SqlString = require('sqlstring')

console.log('DappReview Listed Projects Snapshot Sync Task Started! ')

/**
 * Main Task Function A: Crypto market data snapshot task pipeline
 */
var snapshotSyncTask = async () => {
  console.log('Main Task Function: DappReview listed projects task pipeline start ...')
  console.log('time: ', new Date().toTimeString())
  // A-0: Check DB Connection
  console.log(dbConfig)
  const dbConn = await mysql.createConnection(dbConfig)
  console.log('DB Connected √')

  // A-1 get Snapshot data
  console.log('retriving latest snapshot data . . .')
  var snapshotData = await getLatestSnapshot() // TODO: update DappReview listed datd
  console.log('√ snapshot data received')
  console.log(snapshotData)
  // A-2
  console.log('Inserting snapshot into DB . . . ')
  await snapshotDBSave(snapshotData, dbConn) // TODO: change DappReview projects insert schema
  console.log('√ DB Insert completed!')
  // dbConn.end()
  // console.log('√ DB connection ended!')
}

/**
 * function A-1: Sync DappReview List Project Data
 */
var getLatestSnapshot = async () => {
  console.log('start query DappReview list project Data')
  try {
    let url = 'https://dapp.review/api/dapp/dapps/?search=&page=1&page_size=60&is_support_chinese=false&ordering=-dau_last_day&new=false&block_chain=1&lang=en-US'
    let result = await getLatestSnapshotRecursive(url)
    // console.log(result)
    return result
  } catch (e) {
    console.log(e)
  }
}

/**
 * function A-1-recursive: Sync DappReview List Project Data by Page
 */
var getLatestSnapshotRecursive = async (url) => {
  console.log('start query DappReview list - Recursive type')
  try {
    let response = await request(url, {
      headers: {
        'authority': 'dapp.review',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        'referer': 'https://dapp.review/explore',
        // 'X-CSRFTOKEN': 'XSuFfqwUyLwaorejLEvIXt2DlLW5x9jZUwlxLe4t7JWrkLuHSyD2U99O8pT0DOhc',
        'cookie': '__cfduid=db447d755afd3ede62b0047f6187d46e41533287114; _ga=GA1.2.1560861112.1533287134; _gid=GA1.2.118850291.1536896320; sessionid=iwei3krklsp8zlbprvpbjs7r72didzpe; csrftoken=2lv0EnLVbmenkGX37UOmQdzgdZ7hGGeAVlJVcg35AvzVjyeutJbMFNfSmYr2I9Nm; lang=en-us',
        'x-csrftoken': '2lv0EnLVbmenkGX37UOmQdzgdZ7hGGeAVlJVcg35AvzVjyeutJbMFNfSmYr2I9Nm'
      }
    })

    let result = []

    // success
    if (response.statusCode === 200) {
      let output = JSON.parse(response.body)
      console.log('Total Project ', output.count, 'next: ', output.next)
      // push current result after
      console.log(response)
      // console.log(output.results)
      result = output.results

      // if next exist, resursive in
      if (output.next != null) {
        let nextData = await getLatestSnapshotRecursive(output.next)
        result = result.concat(nextData)
      }
      return result
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
    row.id
    row.title
    row.logo
    row.logo_url
    row.dau_last_day
    row.volume_last_day
    row.tx_last_day
    row.volume_last_week
    row.tx_last_week
    row.categories
    row.star
    row.new
    row.just_description
    row.description
    row.block_chains
    row.createdAt
 */
var snapshotDBSave = async (snapshotData, dbConn) => {
    // start insert snapshot into MySQL
  if (snapshotData.length < 1) { return console.log('empty data') }
  let formedData = []
  let keysArray

//   prepare value arrays
  for (var i = 0; i < snapshotData.length; i++) {
  // for (var i = 0; i < 3; i++) { // TEST_CODE
    // Mapping values
    let originRow = snapshotData[i]

    let row = {}
    row.id = originRow.id
    row.title = originRow.title
    row.logo = originRow.logo
    row.logo_url = originRow.logo_url
    row.dau_last_day = originRow.dau_last_day
    row.volume_last_day = originRow.volume_last_day
    row.tx_last_day = originRow.tx_last_day
    row.volume_last_week = originRow.volume_last_week
    row.tx_last_week = originRow.tx_last_week
    row.categories = null
    if (originRow.categories.length > 0) {
      let categories = originRow.categories[0]
      row.categories = categories.category
    }
    row.star = null
    if (originRow.star) {
      row.star = (
        (originRow.star.star_one || 0) * 1 +
        (originRow.star.star_two || 0) * 2 +
        (originRow.star.star_thr || 0) * 3 +
        (originRow.star.star_fou || 0) * 4 +
        (originRow.star.star_fiv || 0) * 5) / 5
    }
    row.new = originRow.new
    row.just_description = originRow.just_description
    row.description = originRow.description

    // blockchains = 'ETH EOS'
    let blkChains = originRow.block_chains
    row.block_chains = ''
    for (let j = 0; j < blkChains.length; j++) {
      let blkChain = blkChains[j]
      row.block_chains += ' ' + blkChain.block_chain
    }
    row.block_chains = row.block_chains.trim()

    row.createdAt = new Date().toLocaleString()

    // prepare key arrays - add createdAt
    keysArray = Object.keys(row)

    // console.log(row)

    formedData.push(Object.values(row))
  }

  // console.log(keysArray) // KeysArray check

  // Insert into DB
  try {
    // console.log(keysArray.join(', '))
    let output = await dbConn.query('INSERT INTO `dappreview_project_snapshot` (' + keysArray.join(', ') + ') VALUES ?', [formedData])
    // console.log(output)
  } catch (e) {
    console.log(e)
  }

  console.log('Total rows: ', formedData.length)
}

/**
 * SnapshotSync Job
 * rule: every 2 hours
 */

var rule = new schedule.RecurrenceRule()
rule = '0 0 */5 * * *' // production: every 5 hours
// rule = '*/20 * * * * *' // development: 20s
var snapshotSyncJob = schedule.scheduleJob(rule, snapshotSyncTask)

// TEST_CODE
snapshotSyncTask() // TEST_CODE
