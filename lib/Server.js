const fs = require('fs')
const path = require('path')
const rimraf = require("rimraf")

const ReactiveDao = require("@live-change/dao")

const Database = require('./Database.js')

class DatabaseStore {
  constructor(path, backend, options) {
    this.path = path
    this.backend = backend
    this.stores = new Map()

    this.db = backend.createDb(path, options)
  }
  close() {
    return this.backend.closeDb(this.db)
  }
  delete() {
    return this.backend.deleteDb(this.db)
  }
  getStore(name, options = {}) {
    let store = this.stores.get(name)
    if(store) return store
    store = this.backend.createStore(this.db, name, options)
    this.stores.set(name, store)
    return store
  }
  closeStore(name) {
    let store = this.stores.get(name)
    if(!store) return;
    return this.backend.closeStore(store)
  }
  deleteStore(name) {
    let store = this.getStore(name)
    return this.backend.deleteStore(store)
  }
}

class Server {
  constructor(config) {
    this.config = config
    this.databases = new Map()
    this.metadata = null
    this.databaseStores = new Map()

    this.databasesListObservable = new ReactiveDao.ObservableList([])

    if(config.backend == 'leveldb') {
      this.backend = {
        levelup: require('levelup'),
        leveldown: require('leveldown'),
        subleveldown: require('subleveldown'),
        encoding: require('encoding-down'),
        Store: require('@live-change/db-store-level'),
        createDb(path, options) {
          const db = this.levelup(this.leveldown(path, options), options)
          db.path = path
          return db
        },
        closeDb(db) {
          db.close()
        },
        async deleteDb(db) {
          db.close()
          await rimraf(db.path)
        },
        createStore(db, name, options) {
          return new this.Store(this.subleveldown(db, name,
              { ...options, keyEncoding: 'ascii', valueEncoding: 'json' }))
        },
        closeStore(store) {},
        async deleteStore(store) {
          await store.clear()
        }
      }
    } else if(config.backend == 'rocksdb') {
      this.backend = {
        levelup: require('levelup'),
        rocksdb: require('level-rocksdb'),
        subleveldown: require('subleveldown'),
        encoding: require('encoding-down'),
        Store: require('@live-change/db-store-level'),
        createDb(path, options) {
          const db = this.levelup(this.rocksdb(path, options), options)
          db.path = path
          return db
        },
        closeDb(db) {
          db.close()
        },
        async deleteDb(db) {
          db.close()
          await rimraf(db.path)
        },
        createStore(db, name, options) {
          return new this.Store(this.subleveldown(db, name,
              { ...options, keyEncoding: 'ascii', valueEncoding: 'json' }))
        },
        closeStore(store) {},
        async deleteStore(store) {
          await store.clear()
        }
      }
    } else if(config.backend == 'mem') {
      this.backend = {
        levelup: require('levelup'),
        memdown: require('memdown'),
        subleveldown: require('subleveldown'),
        encoding: require('encoding-down'),
        Store: require('@live-change/db-store-level'),
        createDb(path, options) {
          const db = this.levelup(this.memdown(path, options), options)
          db.path = path
          return db
        },
        closeDb(db) {
          db.close()
        },
        async deleteDb(db) {
          db.close()
          await rimraf(db.path)
        },
        createStore(db, name, options) {
          return new this.Store(this.subleveldown(db, name,
              { ...options, keyEncoding: 'ascii', valueEncoding: 'json' }))
        },
        closeStore(store) {},
        async deleteStore(store) {
          await store.clear()
        }
      }
    } else if(config.backend == 'lmdb') {
      this.backend = {
        lmdb: require('node-lmdb'),
        Store: require('@live-change/db-store-lmdb'),
        createDb(path, options) {
          const env = new this.lmdb.Env();
          env.open({
            path: path,
            maxDbs: 1000,
            ...options
          })
          env.path = path
          return env
        },
        closeDb(db) {
          db.close()
        },
        async deleteDb(db) {
          db.close()
          await rimraf(db.path)
        },
        createStore(db, name, options) {
          return db.openDbi({
            name,
            create: true
          })
        },
        closeStore(store) {
          store.lmdb.close()
        },
        async deleteStore(store) {
          store.lmdb.drop()
        }
      }
    }
  }
  createDao(session) {
    const scriptContext = new ScriptContext({
      /// TODO: script available routines
    })
    return {
      database: {
        type: 'local',
            source: ReactiveDao.SimpleDao({
          methods: {
            createDatabase: async (dbName, options = {}) => {
              if(this.metadata.databases[dbName]) throw new Error("databaseAlreadyExists")
              this.metadata.databases[dbName] = options
              this.databases.set(dbName, initDatabase(dbName, options))
              this.databasesListObservable.push(dbName)
              await this.saveMetadata()
              return 'ok'
            },
            deleteDatabase: async (dbName) => {
              if(!this.metadata.databases[dbName]) throw new Error("databaseNotFound")
              delete this.metadata.databases[dbName]
              this.databases.get(dbName).delete()
              this.databaseStores.get(dbName).delete()
              this.databasesListObservable.remove(dbName)
              return 'ok'
            },
            createTable: async (dbName, tableName, options = {}) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              return db.createTable(tableName, options)
            },
            deleteTable: async (dbName, tableName, options) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              return db.deleteTable(tableName)
            },
            createIndex: async (dbName, indexName, code, options = {}) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const queryFunction = scriptContext.run(code, 'query')
              return db.createIndex(indexName, queryFunction, options)
            },
            deleteIndex: async (dbName, indexName, options) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              return db.deleteIndex(indexName)
            },
            createLog: async (dbName, logName, options = {}) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              return db.createLog(logName, options)
            },
            deleteLog: async (dbName, logName, options) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              return db.deleteLog(logName)
            },
            put: (dbName, tableName, object) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return table.put(object)              
            },
            delete: (dbName, tableName, id) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return table.delete(id)
            }, 
            update: (dbName, tableName, id, operations) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return table.update(id, operations)
            },
            putLog: (dbName, logName, object) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const log = db.log(logName)
              if(!log) throw new Error("logNotFound")
              return log.put(object)
            },
            flagLog: (dbName, logName, id, flags) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const log = db.log(logName)
              if(!log) throw new Error("logNotFound")
              return log.flag(id, flags)
            }
          },
          values: {
            databasesList: {
              observable: () => this.databasesListObservable,
              get: async () => this.databasesListObservable.list
            },
            databaseConfig: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.configObservable
              },
              get: async (dbName, tableName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.configObservable.value
              }
            },
            tablesList: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.tablesListObservable
              },
              get: async (dbName, tableName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.tablesListObservable.list
              }
            },
            indexesList: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.indexesListObservable
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.indexesListObservable.list
              }
            },
            logsList: {
              observable: (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.logsListObservable
              },
              get: async (dbName, logName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.logsListObservable.list
              }
            },
            tableConfig: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.configObservable
              },
              get: async (dbName, tableName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.configObservable.value
              }
            },
            indexConfig: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.configObservable
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.configObservable.value
              }
            },
            indexCode: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.codeObservable
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.codeObservable.value
              }
            },
            logConfig: {
              observable: (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const log = db.log(logName)
                if(!log) return new ReactiveDao.ObservableError('logNotFound')
                return log.configObservable
              },
              get: async (dbName, logName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const log = db.log(logName)
                if(!log) throw new Error("logNotFound")
                return log.configObservable.value
              }
            },
            
            tableObject: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.objectObservable(id)
              },
              get: async (dbName, tableName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.objectGet(id)
              }
            },
            tableRange: {
              observable: (dbName, tableName, range) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.rangeObservable(range)
              },
              get: async (dbName, tableName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.rangeGet(range)
              }
            },
            indexObject: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.objectObservable(id)
              },
              get: async (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.objectGet(id)
              }
            },
            indexRange: {
              observable: (dbName, indexName, range) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.rangeObservable(range)
              },
              get: async (dbName, indexName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.rangeGet(range)
              }
            },
            logObject: {
              observable(dbName, logName, id) {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const log = db.log(logName)
                if(!log) return new ReactiveDao.ObservableError('logNotFound')
                return log.objectObservable(id)
              },
              get: (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const log = db.log(logName)
                if(!log) throw new Error("logNotFound")
                return log.objectGet(id)
              }
            },
            logRange: {
              observable(dbName, logName, range) {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const log = db.log(logName)
                if(!log) return new ReactiveDao.ObservableError('logNotFound')
                return log.rangeObservable(range)
              },
              get: async (dbName, logName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const log = db.log(logName)
                if(!log) throw new Error("logNotFound")
                return log.rangeGet(range)
              }
            },
            query: {
              observable(dbName, code) {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const queryFunction = scriptContext.run(code, 'query')
                return db.queryObservable(queryFunction)
              },
              get: async (dbName, tableName, code) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const queryFunction = scriptContext.run(code, 'query')
                return db.queryGet(queryFunction)
              }
            }
          }
        })
      },
      store: {
        type: 'local',
            source: ReactiveDao.SimpleDao({
          methods: {},
          values: {}
        })
      },
    }
  }
  async initialize(initOptions = {}) {
    const normalMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json')
    const backupMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json.bak')
    const normalMetadataExists = await fs.promises.access(normalMetadataPath)
    const backupMetadataExists = await fs.promises.access(backupMetadataPath)
    if(initOptions.forceNew && (normalMetadataExists || backupMetadataExists)) 
      throw new Error("database already exists")
    const normalMetadata = await fs.promises.readFile("metadata.json", "utf8")
        .then(json => JSON.parse(json)).catch(err => null)
    const backupMetadata = await fs.promises.readFile("metadata.json.bak", "utf8")
        .then(json => JSON.parse(json)).catch(err => null)
    if((normalMetadataExists || backupMetadataExists) && !normalMetadata && !backupMetadata)
      throw new Error("database is broken")
    this.metadata = normalMetadata
    if(!normalMetadata) this.metadata = backupMetadata
    if(this.metadata.timestamp < backupMetadata) this.metadata = backupMetadata
    if(!this.metadata) {
      this.metadata = {
        databases: {}
      }
    }
    await this.saveMetadata()
    for(const dbName in this.metadata.databases) {
      const dbConfig = this.metadata.databases[dbName]
      this.databases.set(dbName, await this.initDatabase(dbName, dbConfig))
      this.databasesListObservable.push(dbName)
    }
  }
  async initDatabase(dbName, dbConfig) {
    const dbPath = path.resolve(this.config.dbRoot, dbName+'.db')
    let dbStore = this.databaseStores.get(dbName)
    if(!dbStore) {
      dbStore = new DatabaseStore(dbPath, this.backend, dbConfig.storage)
      this.databaseStores.set(dbName, dbStore)
    }
    return new Database(
        dbConfig,
        (name, config) => dbStore.getStore(name, config),
        (configToSave) => {
          this.metadata.databases[dbName] = configToSave
          this.saveMetadata()
        })
  }
  async saveMetadata() {
    const normalMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json')
    const backupMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json.bak')
    this.metadata.timestamp = Date.now()
    await fs.promises.writeFile(normalMetadataPath, JSON.stringify(this.metadata))
    await fs.promises.writeFile(backupMetadataPath, JSON.stringify(this.metadata))
  }

  listen(host, port) {

  }

  async close() {
    for(const db of this.databaseStores.values()) db.close()
  }
}