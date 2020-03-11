const levelup = require('levelup')
const leveldown = require('leveldown')
const subleveldown = require('subleveldown')
const encoding = require('encoding-down')

const Store = require('@live-change/db-store-level')

const levels = new Map()

module.exports = function(dbPath, name) {
  let level = levels.get(dbPath)
  if(!level) {
    level = levelup(leveldown(dbPath))
    levels.set(dbPath, level)
  }
  const store = new Store(subleveldown(level, name, { keyEncoding: 'ascii', valueEncoding: 'json' }))
  store.close = async function() {
    await new Promise((resolve, reject) => {
      level.createReadStream({ keys: true, values: true }).on('data', function ({ key, value }) {
        //console.log("key:", key.toString('ascii') )
        //console.log("value:", value.toString('ascii') )
      })
      .on('error', function (err) {
        reject(err)
      })
      .on('close', function () {
      })
      .on('end', ()=> resolve('readed'))
    })
    await level.close()
  }
  return store
}
