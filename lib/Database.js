const Table = require('./Table.js')
const Index = require('./Index.js')
const Log = require('./Log.js')
const queryGet = require('./queryGet.js')
const queryObservable = require('./queryObservable.js')
const getRandomValues = require('get-random-values')

const ReactiveDao = require("@live-change/dao")

class Database {
  constructor(config, storeFactory, saveConfig, deleteStore, name) {
    this.name = name
    this.config = {
      tables: {},
      indexes: {},
      logs: {},
      ...config
    }
    this.saveConfig = saveConfig || (() => {})
    this.storeFactory = storeFactory
    this.deleteStore = deleteStore
    this.stores = new Map()
    this.tables = new Map()
    this.logs = new Map()
    this.indexes = new Map()

    this.configObservable = new ReactiveDao.ObservableValue(JSON.parse(JSON.stringify(this.config)))
    this.tablesListObservable = new ReactiveDao.ObservableList(Object.keys(this.config.tables))
    this.indexesListObservable = new ReactiveDao.ObservableList(Object.keys(this.config.indexes))
    this.logsListObservable = new ReactiveDao.ObservableList(Object.keys(this.config.logs))
  }

  async start(startConfig) {
    if(startConfig.slowStart) {
      for(let name in this.config.tables) await this.table(name)
      for(let name in this.config.logs) await this.log(name)
      for(let name in this.config.indexes) await (async (name) => {
        try {
          await this.index(name)
        } catch(error) {
          return 'ok'
        }
      })(name)
    } else {
      let promises = []
      for(let name in this.config.tables) promises.push(this.table(name))
      for(let name in this.config.logs) promises.push(this.log(name))
      for(let name in this.config.indexes) promises.push((async (name) => {
        try {
          await this.index(name)
        } catch(error) {
          return 'ok'
        }
      })(name))
      return Promise.all(promises).then(r => 'ok')
    }
  }

  generateUid() {
    const array = new Uint8Array(16)
    getRandomValues(array)
    return array.map (b => b.toString (16).padStart (2, "0")).join ("")
  }

  store(name, config) {
    let store = this.stores.get(name)
    if(!store) {
      store = this.storeFactory(name, config)
      this.stores.set(name, store)
    }
    return store
  }

  createTable(name, config = {}) {
    if(this.config.tables[name]) throw new Error(`Table ${name} already exists`)
    const uid = config.uid || this.generateUid()
    this.config.tables[name] = { ...config, uid }
    this.saveConfig(this.config)
    this.configObservable.set(JSON.parse(JSON.stringify(this.config)))
    this.tablesListObservable.push(name)
    return this.table(name)
  }

  async deleteTable(name) {
    const config = this.config.tables[name]
    if(!config) throw new Error(`Table ${name} not found`)
    const table = this.table(name)
    await table.deleteTable()
    delete this.config.tables[name]
    this.saveConfig(this.config)
    this.tablesListObservable.remove(name)
    this.tables.delete(name)
  }
  
  renameTable(name, newName) {
    if(this.config.tables[newName]) throw new Error(`Table ${newName} already exists`)
    const table = this.table(name)
    table.name = newName
    this.config.tables[newName] = this.config.tables[name]
    delete this.config.tables[name]
    this.tablesListObservable.push(newName)
    this.tablesListObservable.remove(name)
    this.tables.set(newName, table)
    this.tables.delete(name)
  }

  table(name) {
    let table = this.tables.get(name)
    if(!table) {
      const config = this.config.tables[name]
      if(!config) throw new Error(`Table ${name} not found`)
      table = new Table(this, name, config)
      this.tables.set(name, table)
    }
    return table
  }

  createLog(name, config = {}) {
    if(this.config.logs[name]) throw new Error(`Log ${name} already exists`)
    const uid = config.uid || this.generateUid()
    this.config.logs[name] = { ...config, uid }
    this.saveConfig(this.config)
    this.configObservable.set(JSON.parse(JSON.stringify(this.config)))
    this.logsListObservable.push(name)
    return this.log(name)
  }

  async deleteLog(name) {
    const config = this.config.logs[name]
    if(!config) throw new Error(`Log ${name} not found`)
    const log = this.log(name)
    await log.deleteLog()
    delete this.config.logs[name]
    this.saveConfig(this.config)
    this.logsListObservable.remove(name)
    this.logs.delete(name)
  }

  renameLog(name, newName) {
    if(this.config.logs[newName]) throw new Error(`Log ${newName} already exists`)
    const log = this.log(name)
    log.name = newName
    this.config.logs[newName] = this.config.logs[name]
    delete this.config.logs[name]
    this.logsListObservable.push(newName)
    this.logsListObservable.remove(name)
    this.logs.set(newName, log)
    this.logs.delete(name)
  }

  log(name) {
    let log = this.logs.get(name)
    if(!log) {
      const config = this.config.logs[name]
      if(!config) throw new Error(`Log ${name} not found`)
      log = new Log(this, name, config)
      this.logs.set(name, log)
    }
    return log
  }

  async createIndex(name, code, params, config = {}) {
    if(this.config.indexes[name]) throw new Error(`Index ${name} already exists`)
    config.code = typeof code == 'string' ? code : `(${code})`
    config.parameters = params
    const uid = config.uid || this.generateUid()
    this.config.indexes[name] = { ...config, uid }
    this.saveConfig(this.config)
    this.configObservable.set(JSON.parse(JSON.stringify(this.config)))
    this.indexesListObservable.push(name)
    return await this.index(name)
  }

  async deleteIndex(name) {
    const config = this.config.indexes[name]
    if(!config) throw new Error(`Index ${name} not found`)
    const index = await this.index(name)
    await index.deleteIndex()
    delete this.config.indexes[name]
    this.saveConfig(this.config)
    this.indexesListObservable.remove(name)
    this.indexes.delete(name)
  }

  renameIndex(name, newName) {
    if(this.config.indexes[newName]) throw new Error(`Index ${newName} already exists`)
    const index = this.index(name)
    index.name = newName
    this.config.indexes[newName] = this.config.indexes[name]
    delete this.config.indexes[name]
    this.indexesListObservable.push(newName)
    this.indexesListObservable.remove(name)
    this.indexes.set(newName, index)
    this.indexes.delete(name)
  }

  async clearOpLogs(lastTimestamp, limit) {
    let promises = []
    for(let name in this.config.tables) promises.push((async (name) => {
      const result = await this.table(name).clearOpLog(lastTimestamp, limit)
      return { ...result, type: 'table', name }
    })(name))
    for(let name in this.config.indexes) promises.push((async (name) => {
        try {
          const index = await this.index(name)
          const result = await index.clearOpLog(lastTimestamp, limit)
          return { ...result, type: 'index', name }
        } catch(error) {
          return { type: 'index', name, count: 0, last: "\xFF\xFF\xFF\xFF" }
        }
      })(name))
    const results = await Promise.all(promises)
    const summary = results.reduce(
        (a, b) => ({ count: a.count + b.count, last: a.last < b.last ? a.last : b.last }),
        { count: 0, last: "\xFF\xFF\xFF\xFF" })
    summary.results = results
    return summary
  }

  async index(name) {
    let index = this.indexes.get(name)
    if(!index) {
      const config = this.config.indexes[name]
      if(!config) throw new Error(`Index ${name} not found`)
      const code = config.code
      const params = config.parameters
      index = new Index(this, name, code, params, config)
      try {
        await index.startIndex()
      } catch(error) {
        console.error("INDEX", name, "ERROR", error, "CODE:\n", index.code)
        console.error("DELETING INDEX", name)
        delete this.config.indexes[name]
        this.indexesListObservable.remove(name)
        if(this.onAutoRemoveIndex) this.onAutoRemoveIndex(name, config.uid)
        await this.saveConfig(this.config)
        throw error
      }
      this.indexes.set(name, index)
    }
    return index
  }

  queryGet(code) {
    return queryGet(this, code)
  }

  queryUpdate(code) {
    return queryGet(this, code, true)
  }

  queryObservable(code) {
    return queryObservable(this, code)
  }

  queryObjectGet(code) {
    return queryGet.single(this, code)
  }

  queryObjectObservable(code) {
    return queryObservable.single(this, code)
  }

  handleUnhandledRejectionInIndex(name, reason, promise) {
    console.error("INDEX", name, "unhandledRejection", reason, "CODE:\n", config.code)
    console.error("DELETING INDEX", name)
    process.nextTick(() => {
      const config = this.config.indexes[name]
      if(!config) {
        console.error("INDEX", name, "IS ALREADY DELETED")
        console.trace("ALREADY DELETED")
        return;
      }
      delete this.config.indexes[name]
      this.indexesListObservable.remove(name)
      if(this.onAutoRemoveIndex) this.onAutoRemoveIndex(name, config.uid)
      this.saveConfig(this.config)
    })

  }
  handleConfigUpdated() {
    this.saveConfig(this.config)
    this.configObservable.set(JSON.parse(JSON.stringify(this.config)))
  }
}

module.exports = Database
