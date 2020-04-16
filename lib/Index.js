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
  change(obj, oldObj) {
    for(const callback of this.callbacks) callback(obj, oldObj)
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
  change(obj, oldObj) {
    for(const callback of this.callbacks) callback(obj, oldObj)
  }
  async get() {
    return await (await this.tableReader.table).rangeGet(this.range)
  }
  dispose() {}
}

class TableReader {
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
  change(obj, oldObj) {
    if(!(obj || oldObj)) return
    const id = obj ? obj.id : oldObj.id
    const objectReader = this.objectReaders.get(id)
    if(objectReader) objectReader.change(obj, oldObj)
    const rangeReaders = this.rangeTree.search(id, id)
    for(const rangeReader of rangeReaders) {
      rangeReader.change(obj, oldObj)
    }
    //console.log("TR change", this.callbacks[0])
    for(const callback of this.callbacks) callback(obj, oldObj)
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
    if(this.opLogPromise) return this.opLogPromise
    if(this.opLogObservable) {
      this.opLogObservable.unobserve(this)
      this.opLogObservable = null
    }
    this.opLogPromise = new Promise(async (resolve,reject) => {
      this.opLogResolve = resolve
      if(!this.opLog) this.opLog = this.isLog ? (await this.table).data : (await this.table).opLog
      //console.log("READ OP LOG", this.prefix, key, opLogBatchSize)
      this.opLogObservable = this.opLog.rangeObservable({ gt: key, limit: opLogBatchSize })
      this.opLogObservable.observe(this)
    })
    return this.opLogPromise
  }
  set(value) {
    //console.log("TABLE", this.prefix, "READER SET", value)
    this.opLogBuffer = value.slice()
    //console.log("PROMISE", this.opLogPromise)
    if(this.opLogResolve) {
      this.opLogPromise = null
      const resolve = this.opLogResolve
      this.opLogResolve = null
      //console.log("RESOLVE", resolve)
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
      if (this.opLogBuffer && this.opLogBuffer.length) return this.opLogBuffer[0].id
      //console.log("NK", this.opLogObservable && this.opLogObservable.list, " < ", opLogBatchSize)
      if (this.opLogObservable && this.opLogObservable.list && this.opLogObservable.list.length < opLogBatchSize)
        return null // waiting for more
      //console.log("READING NEXT KEY IN", this.prefix)
      await this.readOpLog(this.opLogObservable.list[this.opLogObservable.list.length - 1].id)
      //console.log("READED OPLOG", this.prefix)
    }
  }
  async readTo(endKey) {
    while(this.opLogBuffer[0] && this.opLogBuffer[0].id <= endKey) {
      const next = this.opLogBuffer.shift()
      if(this.isLog) {
        this.change(next, null)
      } else {
        const op = next.operation
        //console.log("HANDLE OP LOG OPERATION", next.id, op)
        if(op.type == 'put') {
          this.change(op.object, op.oldObject)
        }
        if(op.type == 'delete') {
          this.change(null, op.object)
        }
      }
      if(this.opLogBuffer.length == 0)
        await this.readOpLog(this.opLogObservable.list[this.opLogObservable.list.length - 1].id)
    }
  }
}

class OpLogReader {
  constructor(database, startingKey) {
    this.database = database
    this.currentKey = startingKey
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
    }
    return reader
  }
  index(name) {
    const prefix = 'index_'+name
    let reader = this.tableReaders.find(tr => tr.prefix == prefix)
    if(!reader) {
      reader = new TableReader(this, prefix, this.database.index(name))
      this.tableReaders.push(reader)
    }
    return reader
  }
  log(name) {
    const prefix = 'log_'+name
    let reader = this.tableReaders.find(tr => tr.prefix == prefix)
    if(!reader) {
      reader = new TableReader(this, prefix, this.database.log(name), true)
      this.tableReaders.push(reader)
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
        if(possibleNextKeys.length == 0) throw new Error("No source to read!")
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
        this.currentKey = next.key
        this.currentKey = await next.reader.readTo(readEnd)
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
  synchronized(key, code) {
    return this.index.synchronized(key, code)
  }
}

const INDEX_CREATING = 0
const INDEX_UPDATING = 1
const INDEX_READY = 2

class Index extends Table {
  constructor(database, name, code, params, config) {
    super(database, name, config)
    this.codeObservable = new ReactiveDao.ObservableValue(code)
    this.configObservable = new ReactiveDao.ObservableValue(config)
    this.scriptContext = new ScriptContext({
      /// TODO: script available routines
    })
    const queryFunction = this.scriptContext.run(code, 'query')
    this.code = (input, output) => queryFunction(input, output, params)
    this.writer = new IndexWriter(this)
    this.reader = null
  }
  async startIndex() {
    const lastIndexOperations = await this.opLog.rangeGet({ reverse: true, limit: 1 })
    const lastIndexOperation = lastIndexOperations[0]
    let lastUpdateTimestamp = 0
    if(!lastIndexOperation) { // Create Index from scratch
      let indexCreateTimestamp = Date.now()
      this.state = INDEX_CREATING
      const startReader = new queryGet.QueryReader(this.database)
      await this.code(startReader, this.writer)
      lastUpdateTimestamp = indexCreateTimestamp - 1000 // one second overlay
    } else {
      lastUpdateTimestamp = lastIndexOperation.timestamp - 1000 // one second overlap
      this.state = INDEX_UPDATING
    }
    //console.log("INDEX SYNC!")
    const lastUpdateKey = ((''+lastUpdateTimestamp).padStart(16, '0'))+':'
    this.reader = new OpLogReader(this.database, lastUpdateKey)
    let codePromise = this.code(this.reader, this.writer)
    //console.log("READING!")
    await this.reader.readMore()
    //console.log("WAITING FOR CODE!")
    await codePromise
    this.state = INDEX_READY
    //console.log("INDEX STARTED!")
  }
}

module.exports = Index