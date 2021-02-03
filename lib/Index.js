const IntervalTree = require('node-interval-tree').default
const Table = require('./Table.js')
const ScriptContext = require('./ScriptContext.js')
const queryGet = require('./queryGet.js')
const queryObservable = require('./queryObservable.js')
const ReactiveDao = require("@live-change/dao")

const opLogBatchSize = 3 /// TODO: incrase after testing

class ObjectReader {
  constructor(tableReader, id) {
    this.tableReader = tableReader
    this.id = id
    this.callbacks = []
  }
  onChange(cb) {
    this.callbacks.push(cb)
  }
  change(obj, oldObj, id, timestamp) {
    for(const callback of this.callbacks) callback(obj, oldObj, id, timestamp)
  }
  async get() {
    return await (await this.tableReader.table).objectGet(this.id)
  }
  dispose() {}
}

class RangeReader {
  constructor(tableReader, range) {
    this.tableReader = tableReader
    this.range = range
    this.rangeDescr =[ this.range.gt || this.range.gte || '', this.range.lt || this.range.lte || '\xFF\xFF\xFF\xFF' ]
    this.tableReader.rangeTree.insert(...this.rangeDescr, this )
    this.callbacks = []
  }
  onChange(cb) {
    this.callbacks.push(cb)
  }
  change(obj, oldObj, id, timestamp) {
    for(const callback of this.callbacks) callback(obj, oldObj, id, timestamp)
  }
  async get() {
    return await (await this.tableReader.table).rangeGet(this.range)
  }
  dispose() {}
}

class TableReader {

 /* set opLogPromise(promise) {
    console.trace("SET PROMISE", promise)
    this.oplP = promise
  }
  get opLogPromise() {
    return this.oplP
  }*/

  constructor(opLogReader, prefix, table, isLog) {
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
  }
  async onChange(cb) {
    this.callbacks.push(cb)
  }
  change(obj, oldObj, id, timestamp) {
    if(!(obj || oldObj)) return
    if(typeof id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    const objectReader = this.objectReaders.get(id)
    if(objectReader) objectReader.change(obj, oldObj, id, timestamp)
    const rangeReaders = this.rangeTree.search(id, id)
    for(const rangeReader of rangeReaders) {
      rangeReader.change(obj, oldObj, id, timestamp)
    }
    //console.log("TR change", this.callbacks[0])
    for(const callback of this.callbacks) callback(obj, oldObj, id, timestamp)
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
    //console.log("DO READ OPLOG", key)
    if(this.opLogObservable) {
      this.opLogObservable.unobserve(this)
      this.opLogObservable = null
    }
    this.opLogPromise = new Promise(async (resolve,reject) => {
      this.opLogResolve = resolve
      if(!this.opLog) this.opLog = this.isLog ? (await this.table).data : (await this.table).opLog
      //console.log("READ OP LOG", this.prefix, key, opLogBatchSize)
      this.opLogObservable = this.opLog.rangeObservable({ gt: key, limit: opLogBatchSize })
      /// NEXT TICK BECAUSE IT CAN FINISH BEFORE EVENT START xD
      process.nextTick(() => this.opLogObservable.observe(this))
    })
    return this.opLogPromise
  }
  set(value) {
    //console.log("TABLE", this.prefix, "READER SET", value)
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
    //console.log("TABLE READER PUT", object, this.disposed)
    if(this.disposed) return
    if(field != 'id') throw new Error("incompatible range protocol")
    this.opLogBuffer.push(object)
    this.opLogReader.handleSignal()
  }
  async get() {
    return await (await this.table).rangeGet({})
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
    //console.log("RT", endKey, "IN", this.opLogBuffer)
    let lastKey = null
    while(this.opLogBuffer[0] && this.opLogBuffer[0].id <= endKey) {
      const next = this.opLogBuffer.shift()
      lastKey = next.id
      if(this.isLog) {
        this.change(next, null, next, next.id)
      } else {
        const op = next.operation
        //console.log("HANDLE OP LOG OPERATION", next)
        if(op) {
          if(op.type == 'put') {
            this.change(op.object, op.oldObject, op.object.id, next.id)
          }
          if(op.type == 'delete') {
            //console.log("DELETE CHANGE", next)
            this.change(null, op.object, op.object.id, next.id)
          }
        } else {
          console.error("NULL OPERATION", next)
        }
      }
      if(this.opLogBuffer.length == 0) {
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
  constructor(database, startingKey, onNewSource) {
    this.database = database
    this.currentKey = startingKey
    this.onNewSource = onNewSource || (() => {})
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
      //console.log("STORE SIGNAL")
      this.gotSignals = true
    } else {
      //console.log("READ MORE ON SIGNAL")
      this.readMore()
    }
  }
  async readMore() {
    this.readingMore = true
    do {
      while(true) {
        this.gotSignals = false
        if(this.disposed) return
        const now = Date.now()
        //console.log("LOOKING FOR NEXT KEYS")
        let possibleNextKeys = await Promise.all(
            this.tableReaders.map(async tr => ({ reader: tr, key: await tr.nextKey() }))
        )
        //console.log("GOT NEXT KEYS")
        if(this.disposed) return
        //console.log("POSSIBLE NEXT KEYS", possibleNextKeys.map(({key, reader}) => [reader.prefix, key]))
        if(possibleNextKeys.length == 0) { /// It could happen when oplog is cleared
          return
        }
        const lastKey = '\xFF\xFF\xFF\xFF'
        let next = null
        for (const possibleKey of possibleNextKeys) {
          if (possibleKey.key && (!next || possibleKey.key < next.key)) {
            next = possibleKey
          }
        }
        //console.log("NEXT KEY", next && next.reader && next.reader.prefix, next && next.key)
        if(!next || next.key == lastKey) break // nothing to read
        let otherReaderNext = null
        for(const possibleKey of possibleNextKeys) {
          if(possibleKey.reader != next.reader && possibleKey.key
              && (!otherReaderNext || possibleKey.key < otherReaderNext.key))
            otherReaderNext = possibleKey
        }
        //console.log("OTHER READ NEXT", otherReaderNext && otherReaderNext.reader && otherReaderNext.reader.prefix,
        //   otherReaderNext && otherReaderNext.key)
        const readEnd = (otherReaderNext && otherReaderNext.key) // Read to next other reader key
            || (((''+(now - 1))).padStart(16, '0'))+':' // or to current timestamp

        if((next.key||'') < this.currentKey) {
          debugger
          throw new Error("time travel")
        }
        //console.log("CKN", this.currentKey, '=>', next.key)
        this.currentKey = next.key
        const readKey = await next.reader.readTo(readEnd)
        if(readKey) {
          if((readKey||'') < this.currentKey) {
            debugger
            throw new Error("time travel")
          }
          //console.log("CKR", this.currentKey, '=>', readKey)
          this.currentKey = readKey
        }
      }
    } while(this.gotSignals)
    this.readingMore = false
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
    //console.log("INDEX WRITE", obj, oldObj)
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
    this.scriptContext = new ScriptContext({
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
        (sourceType, sourceName) => this.addSource(sourceType, sourceName))
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