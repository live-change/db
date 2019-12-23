const ReactiveDao = require("@live-change/dao")

class ObjectObserver {
  constructor(queryReader, callback) {
    this.queryReader = queryReader
    this.callback = callback
    this.resolved = false
    this.valuePromise = new Promise((resolve, reject) => {
      this.valuePromiseResolve = resolve
      this.valuePromiseReject = reject
    })
  }
  set(value) {
    if(this.queryReader.state == READER_DISPOSED) return
    for(let obj of value) {
      this.callback(obj, null)
    }
    if(!this.resolved) this.valuePromiseResolve(value)
  }
  readPromise() {
    return this.valuePromise
  }
}

class RangeObserver {
  constructor(queryReader, callback) {
    this.queryReader = queryReader
    this.callback = callback
    this.resolved = false
    this.valuePromise = new Promise((resolve, reject) => {
      this.valuePromiseResolve = resolve
      this.valuePromiseReject = reject
    })
  }
  async set(value) {
    if(this.queryReader.state == READER_DISPOSED) return
    if(this.queryReader.state == READER_READING) {
      await Promise.all(value.map(obj => this.callback(obj, null)))
    } else {
      for(let obj of value) this.callback(obj, null)
    }
    if(!this.resolved) this.valuePromiseResolve(value)
  }
  putByField(field, id, object, reverse, oldObject) {
    if(this.queryReader.state == READER_DISPOSED) return
    if(field != 'id') throw new Error("incompatible range protocol")
    this.callback(object, oldObject)
  }
  removeByField(field, id, object) {
    if(this.queryReader.state == READER_DISPOSED) return
    this.callback(null, object)
  }
  readPromise() {
    return this.valuePromise
  }
}

class Reader {
  constructor(queryReader) {
    this.queryReader = queryReader
    this.observable = null
    this.observers = []
    this.disposed = false
  }
  async startObserver(factory) {
    if(this.queryReader.state != READER_READING)
      throw new Error(`Impossible to read data in ${readerStates[this.queryReader.state]} state`)
    if(!this.observable) this.observable = this.observableFactory()
    const observer = factory(this.queryReader)
    this.observers.push(observer)
    ;(await this.observable).observe(observer)
    await observer.readPromise()
  }
  async dispose() {
    this.disposed = true
    for(let observer of this.observers) {
      ;(await this.observable).unobserve(observer)
    }
  }
}

class ObjectReader extends Reader {
  constructor(queryReader, table, id) {
    super(queryReader)
    this.table = table
    this.id = id
  }
  observableFactory() {
    return this.table.objectObservable(this.id)
  }
  onChange(cb) {
    return this.startObserver((r) => new ObjectObserver(r, cb))
  }
  get() {
    return this.table.objectGet(this.id)
  }
}

class RangeReader extends Reader {
  constructor(queryReader, table, range) {
    super(queryReader)
    this.table = table
    this.range = range
  }
  async observableFactory() {
    return await (await this.table).rangeObservable(this.range)
  }
  onChange(cb) {
    return this.startObserver((r) => new RangeObserver(r, cb))
  }
  async get() {
    return await (await this.table).rangeGet(this.range)
  }
}
class TableReader extends Reader {
  constructor(queryReader, prefix, table) {
    super(queryReader)
    this.prefix = prefix
    this.table = table
  }
  async observableFactory() {
    return await (await this.table).rangeObservable({})
  }
  onChange(cb) {
    return this.startObserver((r) => new RangeObserver(r, cb))
  }
  range(range) {
    return this.queryReader.getExistingReaderOrCreate(this.prefix+':'+JSON.stringify(range),
        () => new RangeReader(this.queryReader, this.table, range))
  }
  object(id) {
    return this.queryReader.getExistingReaderOrCreate(this.prefix+'#'+id,
        () => new ObjectReader(this.queryReader, this.table, id))
  }
  async get() {
    return await (await this.table).rangeGet({})
  }
}


const READER_READING = 0
const READER_OBSERVING = 1
const READER_DISPOSED = 2
const readerStates = ['reading', 'observing', 'disposed']

class QueryReader {
  constructor(database) {
    this.database = database
    this.readers = new Map()
    this.state = READER_READING
  }
  getExistingReaderOrCreate(key, create) {
    let reader = this.readers.get(key)
    if(!reader) {
      reader = create()
      this.readers.set(key, reader)
    }
    return reader
  }
  table(name) {
    const prefix = 'table_' + name
    return this.getExistingReaderOrCreate(prefix,
        () => new TableReader(this, prefix, this.database.table(name)))
  }
  index(name) {
    const prefix = 'index_' + name
    return this.getExistingReaderOrCreate(prefix,
        () => new TableReader(this, prefix, this.database.index(name)))
  }
  dispose() {
    this.state = READER_DISPOSED
    for(const reader of this.readers.values()) {
      reader.dispose()
    }
  }
}

class QueryWriter {
  constructor(observable) {
    this.observable = observable
    this.results = new Map()
    this.locks = new Map()
    this.observationMode = false
  }
  put(object) {
    if(this.observationMode) {
      this.observable.putObject(object)
    } else {
      this.results.set(object.id, object)
    }
  }
  delete(object) {
    if(this.observationMode) {
      this.observable.deleteObject(object)
    } else {
      this.results.delete(object.id)
    }
  }
  change(obj, oldObj) {
    if(this.observationMode) {
      if(obj) {
        if(oldObj && oldObj.id != obj.id) {
          this.observable.deleteObject(oldObj)
          this.observable.putObject(obj, null)
        } else {
          this.observable.putObject(obj, oldObj)
        }
      } else {
        if(oldObj) this.observable.deleteObject(oldObj)
      }
    } else {
      if (oldObj) this.delete(oldObj.id)
      if (obj) this.put(obj)
    }
  }
  getResultsAndStartObservation() {
    this.observationMode = true
    this.observable.set(
        Array.from(this.results.entries()).sort((a,b)=>a[0]>b[0]?1:(a[0]<b[0]?-1:0) ).map(a => a[1])
    )
  }
  async synchronized(key, code) {
    let promise = this.locks.get(key)
    while(promise) {
      await promise
      promise = this.locks.get(key)
    }
    promise = (async () => {
      let result = await code()
      this.locks.delete(key)
      return result
    })()
    this.locks.set(key, promise)
    return await promise
  }
}

class QueryObservable extends ReactiveDao.ObservableList {
  constructor(database, code) {
    super()
    this.database = database
    this.code = code

    this.disposed = false
    this.ready = false
    this.respawnId = 0

    this.forward = null

    this.readPromise = this.startReading()
  }

  async startReading() {
    this.reader = new QueryReader(this.database)
    this.writer = new QueryWriter(this)
    await this.code(this.reader, this.writer)
    this.reader.state = READER_OBSERVING
    this.writer.getResultsAndStartObservation()
  }

  async putObject(object, oldObject) {
    await this.readPromise
    console.log("PUT PUT", object)
    const id = object.id
    this.putByField('id', id, object, false, oldObject)
  }

  async deleteObject(object) {
    await this.readPromise
    const id = object.id
    this.removeByField('id', id, object)
  }

  dispose() {
    if(this.forward) {
      this.forward.unobserve(this)
      this.forward = null
      return
    }

    if(this.reader) this.reader.dispose()

    this.disposed = true
    this.respawnId++
  }

  respawn() {
    this.respawnId++
    this.ready = false
    this.disposed = false
    this.startReading()
  }
}

function queryObservable(database, code) {
  return new QueryObservable(database, code)
}

queryObservable.QueryWriter = QueryWriter
queryObservable.QueryReader = QueryReader

module.exports = queryObservable
