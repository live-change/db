const Table = require('./Table.js')
const Index = require('./Index.js')
const queryGet = require('./queryGet.js')
const queryObservable = require('./queryObservable.js')

class Database {
  constructor(config, storeFactory, saveConfig) {
    this.config = {
      tables: {},
      indexes: {},
      ...config
    }
    this.saveConfig = saveConfig || (() => {})
    this.storeFactory = storeFactory
    this.stores = new Map()
    this.tables = new Map()
    this.indexes = new Map()
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
    this.config.tables[name] = config
    this.saveConfig(this.config)
    return this.table(name)
  }

  deleteTable(name) {
    const config = this.config.tables[name]
    if(!config) throw new Error(`Table ${name} not found`)
    delete this.config.tables[name]
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

  async createIndex(name, code, config = {}) {
    if(this.config.indexes[name]) throw new Error(`Index ${name} already exists`)
    config.code = code
    this.config.indexes[name] = config
    this.saveConfig(this.config)
    return await this.index(name)
  }

  deleteIndex(name) {
    const config = this.config.indexes[name]
    if(!config) throw new Error(`Index ${name} not found`)
    delete this.config.indexes[name]
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
