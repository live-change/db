const IntervalTree = require('node-interval-tree').default
const ReactiveDao = require("@live-change/dao")
const Table = require('./Table.js')
const queryGet = require('./queryGet.js')
const profileLog = require('./profileLog.js')
const queryObservable = require('./queryObservable.js')
const { ChangeStream } = require('./ChangeStream.js')

const opLogBatchSize = 128 /// TODO: incrase after testing

class ObjectReader extends ChangeStream {
  constructor(tableReader, id) {
    super()
    this.tableReader = tableReader
    this.id = id
    this.callbacks = []
  }
  onChange(cb) {
    this.callbacks.push(cb)
  }
  async change(obj, oldObj, id, timestamp) {
    for(const callback of this.callbacks) await callback(obj, oldObj, id, timestamp)
  }
  async get() {
    return await (await this.tableReader.table).objectGet(this.id)
  }
  dispose() {}
}

class RangeReader extends ChangeStream {
  constructor(tableReader, range) {
    super()
    this.tableReader = tableReader
    this.range = range
    this.rangeDescr =[ this.range.gt || this.range.gte || '', this.range.lt || this.range.lte || '\xFF\xFF\xFF\xFF' ]
    this.tableReader.rangeTree.insert(...this.rangeDescr, this )
    this.callbacks = []
  }
  onChange(cb) {
    this.callbacks.push(cb)
  }
  async change(obj, oldObj, id, timestamp) {
    for(const callback of this.callbacks) await callback(obj, oldObj, id, timestamp)
  }
  async get() {
    return await (await this.tableReader.table).rangeGet(this.range)
  }
  dispose() {}
}

class TableReader extends ChangeStream {

 /* set opLogPromise(promise) {
    console.trace("SET PROMISE", promise)
    this.oplP = promise
  }
  get opLogPromise() {
    return this.oplP
  }*/

  constructor(opLogReader, prefix, table, isLog) {
    super()
    this.opLogReader = opLogReader
    this.prefix = prefix
    this.table = table
    this.isLog = isLog
    this.objectReaders = new Map()
    this.rangeReaders = new Map()
    this.rangeTree = new IntervalTree()
    this.disposed = false
    this.callbacks = []

    this.readOpLog(this.opLogReader.currentKey)

    let firstFull = 0
    /*setInterval(() => {
      if(this.opLogObservable && this.opLogObservable.list && this.opLogObservable.list.length == opLogBatchSize) {
        if(firstFull > 0) {
          console.error("TABLE READER", prefix, "OPLOG FULL TOO LONG!")
          process.exit(1)
        } else {
          firstFull = Date.now()
        }
      } else {
        firstFull = 0
      }
    }, 1000)*/
/*    //if(this.prefix == 'table_triggers'){
      let triggersDataJson = ""
      setInterval(() => {
        if(JSON.stringify(this.opLogObservable && this.opLogObservable.list) != triggersDataJson) {
          console.log("TABLE READER RANGE", this.opLogObservableRange)
          console.log("TABLE READER STATE", this.opLogObservable.list)
          console.log("TABLE READER REQUEST ID", this.opLogObservable.requestId)
          triggersDataJson = JSON.stringify(this.opLogObservable && this.opLogObservable.list)
        }
      },500)
    }*/
  }
  async onChange(cb) {
    this.callbacks.push(cb)
  }
  async change(obj, oldObj, id, timestamp) {
    if(!(obj || oldObj)) return
    if(typeof id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    const profileOp = profileLog.started
        ? await profileLog.begin({
          operation: 'processIndexChange', index: this.opLogReader.indexName, source: this.prefix + this.table
        })
        : null
    const objectReader = this.objectReaders.get(id)
    if(objectReader) objectReader.change(obj, oldObj, id, timestamp)
    const rangeReaders = this.rangeTree.search(id, id)
    for(const rangeReader of rangeReaders) {
      rangeReader.change(obj, oldObj, id, timestamp)
    }
    //console.log("TR change", this.callbacks[0])
    for(const callback of this.callbacks) await callback(obj, oldObj, id, timestamp)
    if(profileOp) await profileLog.end(profileOp)
  }
  range(range) {
    const key = JSON.stringify(range)
    let reader = this.rangeReaders.get(key)
    if(!reader) {
      if(range.offset || range.limit) throw new Error("offset and limit in range indexes not supported")
      reader = new RangeReader(this, range)
      this.rangeReaders.set(key, reader)
    }
    return reader
    return new RangeReader(this, range)
  }
  object(id) {
    let reader = this.objectReaders.get(id)
    if(!reader) {
      reader = new ObjectReader(this, id)
      this.objectReaders.set(id, reader)
    }
    return reader
    return new ObjectReader(this, id)
  }
  dispose() {
    this.disposed = true
    for(let objectReader of this.objectReaders) objectReader.dispose()
    for(let rangeReader of this.objectReaders) rangeReader.dispose()
  }

  async readOpLog(key) {
    //console.log("READ OP LOG")
    if(this.opLogPromise) return this.opLogPromise
    if(this.opLogObservable && this.opLogObservable.list && this.opLogObservable.list.length < opLogBatchSize) {
      console.error("SHOULD NOT READ NOT FINISHED OPLOG", this.opLogObservable.list)
      console.trace("READ OP LOG TOO EARLY!!!")
      process.exit(1)
    }
    //console.log("DO READ OPLOG", key)
    if(this.opLogObservable) {
      this.opLogObservable.unobserve(this)
      this.opLogObservable = null
    }
    this.opLogPromise = new Promise(async (resolve,reject) => {
      this.opLogResolve = resolve
      if(!this.opLog) this.opLog = this.isLog ? (await this.table).data : (await this.table).opLog
      //console.log("READ OP LOG", this.prefix, key, opLogBatchSize)
      this.opLogObservableRange = { gt: key, limit: opLogBatchSize }
      this.opLogObservable = this.opLog.rangeObservable(this.opLogObservableRange)
      /// NEXT TICK BECAUSE IT CAN FINISH BEFORE EVENT START xD
      process.nextTick(() => this.opLogObservable.observe(this))
    })
    return this.opLogPromise
  }
  set(value) {
    //if(this.prefix == 'table_triggers') console.log("TABLE", this.prefix, "READER SET", value)
    if(!value) return
    this.opLogBuffer = value.slice()
    //console.log("PROMISE", this.opLogPromise)
    if(this.opLogResolve) {
      const resolve = this.opLogResolve
      this.opLogResolve = null
      this.opLogPromise = null
      //console.log("RESOLVE OPLOG PROMISE", resolve)
      resolve(value)
    }
    this.opLogReader.handleSignal()
  }
  putByField(field, id, object) {
    //if(this.prefix == 'table_triggers') console.log("TABLE", this.prefix, " READER PUT", object, this.disposed)
    if(this.disposed) return
    if(field != 'id') throw new Error("incompatible range protocol")
    this.opLogBuffer.push(object)
    this.opLogReader.handleSignal()
  }
  push(object) {
    //if(this.prefix == 'table_triggers') console.log("TABLE", this.prefix, " READER PUSH", object)
    if(this.disposed) return
    this.opLogBuffer.push(object)
    this.opLogReader.handleSignal()
  }
  async get(range = {}) {
    return await (await this.table).rangeGet(range)
  }
  async count(range = {}) {
    return await (await this.table).countGet(range)
  }
  async nextKey() {
    while(true) {
      //console.log("LOOKING FOR NEXT KEY IN", this.prefix)
      await this.opLogPromise
      if(this.opLogPromise != null) {
        console.trace("IMPOSIBBLE!")
        process.exit(1)
      }
      //console.log("FB", this.opLogBuffer && this.opLogBuffer.length)
      if (this.opLogBuffer && this.opLogBuffer.length) return this.opLogBuffer[0].id
      //console.log("NK", this.opLogObservable && this.opLogObservable.list, " < ", opLogBatchSize)
      if (this.opLogObservable && this.opLogObservable.list && this.opLogObservable.list.length < opLogBatchSize)
        return null // waiting for more
      //console.log("READING NEXT KEY IN", this.prefix)
      const lastKey = this.opLogObservable.list[this.opLogObservable.list.length - 1].id
      await this.readOpLog(lastKey)
      //console.log("READED OPLOG", this.prefix)
    }
  }
  async readTo(endKey) {
    //if(this.prefix == 'table_triggers') console.log("RT", endKey, "IN", this.opLogBuffer)
    let lastKey = null
    while(this.opLogBuffer[0] && this.opLogBuffer[0].id <= endKey) {
      const next = this.opLogBuffer.shift()
      lastKey = next.id
      if(this.isLog) {
        await this.change(next, null, next, next.id)
      } else {
        const op = next.operation
        //if(this.prefix == 'table_triggers') console.log("HANDLE OP LOG OPERATION", next)
        if(op) {
          if(op.type == 'put') {
            await this.change(op.object, op.oldObject, op.object.id, next.id)
          }
          if(op.type == 'delete') {
            //console.log("DELETE CHANGE", next)
            await this.change(null, op.object, op.object.id, next.id)
          }
        } else {
          console.error("NULL OPERATION", next)
        }
      }
      if(this.opLogBuffer.length == 0 && this.opLogObservable.list.length >= opLogBatchSize) {
        //console.log("ENTER OPLOG READ!")
        await this.readOpLog(this.opLogObservable.list[this.opLogObservable.list.length - 1].id)
        //console.log("READ TO RESULT, OP LOG PROMISE:", this.opLogPromise)
      }
      //console.log("RT", endKey, "IN", this.opLogBuffer)
    }
    return lastKey
  }
}

class OpLogReader {
  constructor(database, startingKey, onNewSource, indexName) {
    this.database = database
    this.currentKey = startingKey
    this.onNewSource = onNewSource || (() => {})
    this.indexName = indexName
    this.tableReaders = []
    this.readingMore = false
    this.gotSignals = false
    this.disposed = false
  }
  table(name) {
    const prefix = 'table_'+name
    let reader = this.tableReaders.find(tr => tr.prefix == prefix)
    if(!reader) {
      reader = new TableReader(this, prefix, this.database.table(name))
      this.tableReaders.push(reader)
      this.onNewSource('table', name)
    }
    return reader
  }
  index(name) {
    const prefix = 'index_'+name
    let reader = this.tableReaders.find(tr => tr.prefix == prefix)
    if(!reader) {
      reader = new TableReader(this, prefix, this.database.index(name))
      this.tableReaders.push(reader)
      this.onNewSource('index', name)
    }
    return reader
  }
  log(name) {
    const prefix = 'log_'+name
    let reader = this.tableReaders.find(tr => tr.prefix == prefix)
    if(!reader) {
      reader = new TableReader(this, prefix, this.database.log(name), true)
      this.tableReaders.push(reader)
      this.onNewSource('log', name)
    }
    return reader
  }

  handleSignal() {
    if(this.readingMore) {
      //if(this.indexName == 'triggers_new') console.log("STORE SIGNAL")
      this.gotSignals = true
    } else {
      //if(this.indexName == 'triggers_new') console.log("READ MORE ON SIGNAL")
      this.readMore()
    }
  }
  async readMore() {
    this.readingMore = true
    //if(this.indexName == 'triggers_new') console.log("READING TRIGGERS STARTED!")
    do {
      while(true) {
        this.gotSignals = false
        if(this.disposed) return
        const now = Date.now()
        //console.log("LOOKING FOR NEXT KEYS")
        let possibleNextKeys = await Promise.all(
            this.tableReaders.map(async tr => ({ reader: tr, key: await tr.nextKey() }))
        )
        //if(this.indexName == 'triggers_new') console.log("GOT NEXT KEYS")
        if(this.disposed) return
        //if(this.indexName == 'triggers_new') console.log("POSSIBLE NEXT KEYS", possibleNextKeys.map(({key, reader}) => [reader.prefix, key]))
        if(possibleNextKeys.length == 0) { /// It could happen when oplog is cleared
          return
        }
        let next = null
        for (const possibleKey of possibleNextKeys) {
          if (possibleKey.key && (!next || possibleKey.key < next.key)) {
            next = possibleKey
          }
        }
        //if(this.indexName == 'triggers_new') console.log("NEXT KEY", next && next.reader && next.reader.prefix, next && next.key)
        const lastKey = '\xFF\xFF\xFF\xFF'
        //if(this.indexName == 'triggers_new') console.log("NEXT", !!next, "KEY", next && next.key, lastKey)
        if(!next || next.key == lastKey) break // nothing to read
        let otherReaderNext = null
        for(const possibleKey of possibleNextKeys) {
          if(possibleKey.reader != next.reader && possibleKey.key
              && (!otherReaderNext || possibleKey.key < otherReaderNext.key))
            otherReaderNext = possibleKey
        }
        //if(this.indexName == 'triggers_new')
        //  console.log("OTHER READ NEXT", otherReaderNext && otherReaderNext.reader && otherReaderNext.reader.prefix,
        //    otherReaderNext && otherReaderNext.key)
        let readEnd = (otherReaderNext && otherReaderNext.key) // Read to next other reader key
            || (((''+(now - 1))).padStart(16, '0'))+':' // or to current timestamp
        if(readEnd < next) {
          readEnd = next+'\xff'
        }

        if((next.key||'') < this.currentKey) {
          //debugger
          console.error("time travel", next.key, this.currentKey)
          //process.exit(1) /// TODO: do something about it!
        }
        //if(this.indexName == 'triggers_new') console.log("CKN", this.currentKey, '=>', next.key)
        this.currentKey = next.key
        //if(this.indexName == 'triggers_new') console.log("READ TO", readEnd)
        const readKey = await next.reader.readTo(readEnd)
        //if(this.indexName == 'triggers_new') console.log("READED")
        if(readKey) {
          if((readKey||'') < this.currentKey) {
            //debugger
            console.error("time travel", readKey, this.currentKey)
            //process.exit(1) /// TODO: do something about it!
          }
          //if(this.indexName == 'triggers_new') console.log("CKR", this.currentKey, '=>', readKey)
          this.currentKey = readKey
        }
      }
    } while(this.gotSignals)
    this.readingMore = false
    //if(this.indexName == 'triggers_new') console.log("READING TRIGGERS FINISHED!")
  }
  dispose() {
    this.disposed = true
    for(const reader of this.tableReaders) {
      reader.dispose()
    }
  }
}

class IndexWriter {
  constructor(index) {
    this.index = index
  }
  put(object) {
    this.index.put(object)
  }
  delete(object) {
    this.index.delete(object.id)
  }
  update(id, ops) {
    if(typeof id != 'string') {
      console.error("Index update id is corrupted", JSON.stringify(id))
      console.error("INDEX", this.index.name)
      console.error("INDEX CODE", this.index.codeObservable.value)
      console.error("INDEX PARAMS", this.index.params)
    }
    this.index.update(id, ops)
  }
  change(obj, oldObj) {
    //if(this.index.name == 'triggers_new') console.log("INDEX WRITE", obj, oldObj)
    if(obj) {
      if(oldObj && oldObj.id != obj.id) {
        this.index.delete(oldObj.id)
        this.index.put(obj)
      } else {
        this.index.put(obj)
      }
    } else {
      if(oldObj) this.index.delete(oldObj.id)
    }
  }
  get(id) {
    return this.index.get(id)
  }
  synchronized(key, code) {
    return this.index.synchronized(key, code)
  }
  debug(...args) {
    console.log('INDEX', this.index.name, 'DEBUG', ...args)
  }
}

const INDEX_CREATING = 0
const INDEX_UPDATING = 1
const INDEX_READY = 2

class Index extends Table {
  constructor(database, name, code, params, config) {
    super(database, name, config)
    this.database = database
    this.codeObservable = new ReactiveDao.ObservableValue(code)
    this.params = params
    this.code = code
  }
  async startIndex() {
    console.log("EXECUTING INDEX CODE", this.name)
    this.scriptContext = this.database.createScriptContext({
      /// TODO: script available routines
    })
    const queryFunction = this.scriptContext.run(this.code,`userCode:${this.database.name}/indexes/${this.name}`)
    this.codeFunction = (input, output) => queryFunction(input, output, this.params)
    this.writer = new IndexWriter(this)
    this.reader = null
    console.log("STARTING INDEX", this.name)
    const lastIndexOperations = await this.opLog.rangeGet({ reverse: true, limit: 1 })
    const lastIndexOperation = lastIndexOperations[0]
    let lastUpdateTimestamp = 0
    if(!lastIndexOperation) { // Create Index from scratch
      //console.log("RECREATING INDEX", this.name)
      let indexCreateTimestamp = Date.now()
      this.state = INDEX_CREATING
      let timeCounter = 0
      const startReader = new queryGet.QueryReader(this.database, () => (''+(++timeCounter)).padStart(16, '0'),
          (sourceType, sourceName) => this.addSource(sourceType, sourceName))
      await this.codeFunction(startReader, this.writer)
      lastUpdateTimestamp = indexCreateTimestamp - 1000 // one second overlay
      this.opLogWritter({
        type: 'indexed'
      })
    } else {
      lastUpdateTimestamp = lastIndexOperation.timestamp - 1000 // one second overlap
      //console.log("UPDATING INDEX", this.name, "FROM", lastUpdateTimestamp)
      this.state = INDEX_UPDATING
    }
    const lastUpdateKey = ((''+lastUpdateTimestamp).padStart(16, '0'))+':'
    //console.log("INDEX SYNC FROM", lastUpdateKey)
    this.reader = new OpLogReader(this.database, lastUpdateKey,
        (sourceType, sourceName) => this.addSource(sourceType, sourceName),
        this.name)
    let codePromise
    codePromise = this.codeFunction(this.reader, this.writer)
    //console.log("READING!")
    await this.reader.readMore()
    console.log("WAITING FOR CODE!", this.name)
    await codePromise
    this.state = INDEX_READY
    const startTime = Date.now()
    console.log("INDEX STARTED!", this.name)
    await this.opLog.put({
      id: ((''+startTime).padStart(16, '0'))+':000000',
      timestamp: startTime,
      operation: {
        type: 'indexed'
      }
    })
  }
  async deleteIndex() {
    this.reader.dispose()
    await this.deleteTable()
  }
  addSource(sourceType, sourceName) {
    const config = JSON.parse(JSON.stringify(this.configObservable.value))
    if(!config.sources) config.sources = []
    const existingSourceInfo = config.sources.find(({type, name}) => type === sourceType && name === sourceName )
    if(existingSourceInfo) return
    const newSourceInfo = { type: sourceType, name: sourceName }
    config.sources.push(newSourceInfo)
    console.log("NEW INDEX", this.name, "SOURCE DETECTED", sourceType, sourceName)
    this.configObservable.set(config)
    this.database.config.indexes[this.name] = config
    this.database.handleConfigUpdated()
  }
}

module.exports = Index