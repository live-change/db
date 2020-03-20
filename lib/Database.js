const Table = require('./Table.js')
const Index = require('./Index.js')
const Log = require('./Log.js')
const queryGet = require('./queryGet.js')
const queryObservable = require('./queryObservable.js')
const crypto = require('crypto')

const ReactiveDao = require("@live-change/dao")

class Database {
  constructor(config, storeFactory, saveConfig) {
    this.config = {
      tables: {},
      indexes: {},
      logs: {},
      ...config
    }
    this.saveConfig = saveConfig || (() => {})
    this.storeFactory = storeFactory
    this.stores = new Map()
    this.tables = new Map()
    this.logs = new Map()
    this.indexes = new Map()

    this.configObservable = new ReactiveDao.ObservableValue(this.config)
    this.tablesListObservable = new ReactiveDao.ObservableList(Object.keys(this.config.tables))
    this.indexesListObservable = new ReactiveDao.ObservableList(Object.keys(this.config.indexes))
    this.logsListObservable = new ReactiveDao.ObservableList(Object.keys(this.config.logs))
  }

  generateUid() {
    return crypto.randomBytes(16).toString("hex")
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
    const uid = this.generateUid()
    this.config.tables[name] = { ...config, uid }
    this.saveConfig(this.config)
    this.configObservable.set(this.config)
    this.tablesListObservable.push(name)
    return this.table(name)
  }

  deleteTable(name) {
    const config = this.config.tables[name]
    if(!config) throw new Error(`Table ${name} not found`)
    delete this.config.tables[name]
    this.saveConfig(this.config)
    this.tablesListObservable.remove(name)
    this.tables.delete(name)
  }
  
  renameTable(name, newName) {
    const table = this.table(name)
    table.name = newName
    this.config.tables[newName] = this.config.tables[name]
    delete this.config.tables[name]
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
    const uid = this.generateUid()
    this.config.logs[name] = { ...config, uid }
    this.saveConfig(this.config)
    this.configObservable.set(this.config)
    this.logsListObservable.push(name)
    return this.log(name)
  }

  deleteLog(name) {
    const config = this.config.logs[name]
    if(!config) throw new Error(`Log ${name} not found`)
    delete this.config.logs[name]
    this.saveConfig(this.config)
    this.logsListObservable.remove(name)
    this.logs.delete(name)
  }

  renameLog(name, newName) {
    const log = this.log(name)
    log.name = newName
    this.config.logs[newName] = this.config.logs[name]
    delete this.config.logs[name]
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

  async createIndex(name, code, config = {}) {
    if(this.config.indexes[name]) throw new Error(`Index ${name} already exists`)
    config.code = code
    const uid = this.generateUid()
    this.config.indexes[name] = { ...config, uid }
    this.saveConfig(this.config)
    this.configObservable.set(this.config)
    this.indexesListObservable.push(name)
    return await this.index(name)
  }

  deleteIndex(name) {
    const config = this.config.indexes[name]
    if(!config) throw new Error(`Index ${name} not found`)
    delete this.config.indexes[name]
    this.saveConfig(this.config)
    this.indexesListObservable.remove(name)
    this.indexes.delete(name)
  }

  renameIndex(name, newName) {
    const index = this.index(name)
    index.name = newName
    this.config.indexes[newName] = this.config.indexes[name]
    delete this.config.indexes[name]
    this.indexes.set(newName, index)
    this.indexes.delete(name)
  }

  async index(name) {
    let index = this.indexes.get(name)
    if(!index) {
      const config = this.config.indexes[name]
      if(!config) throw new Error(`Index ${name} not found`)
      const code = config.code
      index = new Index(this, name, code, config)
      await index.startIndex()
      this.indexes.set(name, index)
    }
    return index
  }

  queryGet(code) {
    return queryGet(this, code)
  }

  queryObservable(code) {
    return queryObservable(this, code)
  }

}

module.exports = Database
