const rimraf = require("rimraf")

const Database = require("../../lib/Database.js")

function createDb(dbPath) {
  rimraf.sync(dbPath)
  const db = new Database({}, (name, config) => {
    let store
    if(process.env.DB=='level') {
      store = require('./createStore.level.js')(dbPath, name)
    } else if(process.env.DB=='lmdb' || !process.env.DB) {
      store = require('./createStore.lmdb.js')(dbPath, name)
    } else {
      console.error("Unknown database " + process.env.DB)
      throw new Error("Unknown database " + process.env.DB)
    }
    const oldClose = db.close
    db.close = async () => {
      if(oldClose) oldClose.call(db)
      await store.close()
    }
    return store
  })
  return db
}

module.exports = createDb
