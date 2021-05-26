const ReactiveDao = require("@live-change/dao")
const { ChangeStream } = require('./ChangeStream.js')

class ObjectObserver {
  #callback = null
  #resolved = false
  #valuePromiseResolve = null
  #valuePromiseReject = null
  #valuePromise = null
  #oldValue = null

  constructor(queryReader, callback) {
    this._queryReader = queryReader
    this.#callback = callback
    this.#resolved = false
    this.#valuePromise = new Promise((resolve, reject) => {
      this.#valuePromiseResolve = resolve
      this.#valuePromiseReject = reject
    })
  }
  set(value) {
    if(this._queryReader.state == READER_DISPOSED) return
    const now = (''+Date.now()).padStart(16, '0')
    this.#callback(value, this.#oldValue, value ? value.id : this.#oldValue && this.#oldValue.id, now)
    this.#oldValue = value
    if(!this.#resolved) this.#valuePromiseResolve(value)
  }
  readPromise() {
    return this.#valuePromise
  }
}

class RangeObserver {
  #callback = null
  #resolved = false
  #valuePromiseResolve = null
  #valuePromiseReject = null
  #valuePromise = null

  constructor(queryReader, callback) {
    this._queryReader = queryReader
    this.#callback = callback
    this.#valuePromise = new Promise((resolve, reject) => {
      this.#valuePromiseResolve = resolve
      this.#valuePromiseReject = reject
    })
  }
  async set(value) {
    if(this._queryReader.state == READER_DISPOSED) return
    const now = (''+Date.now()).padStart(16, '0')
    if(this._queryReader.state == READER_READING) {
      await Promise.all(value.map(obj => this.#callback(obj, null, obj.id, now)))
    } else {
      for(let obj of value) this.#callback(obj, null, obj.id, now)
    }
    if(!this.#resolved) this.#valuePromiseResolve(value)
  }
  putByField(field, id, object, reverse, oldObject) {
    if(this._queryReader.state == READER_DISPOSED) return
    if(field != 'id') throw new Error("incompatible range protocol")
    const now = (''+Date.now()).padStart(16, '0')
    this.#callback(object, oldObject, id, now)
  }
  removeByField(field, id, object) {
    if(this._queryReader.state == READER_DISPOSED) return
    this.#callback(null, object)
  }
  readPromise() {
    return this.#valuePromise
  }
}

class Reader extends ChangeStream {
  #observable = null
  #observers = []
  #disposed = false

  constructor(queryReader) {
    super()
    this._queryReader = queryReader
  }
  async startObserver(factory) {
/*    if(this._queryReader.state != READER_READING)
      throw new Error(`Impossible to read data in ${readerStates[this._queryReader.state]} state`)*/
    if(!this.#observable) this.#observable = this.observableFactory()
    const observer = factory(this._queryReader)
    this.#observers.push(observer)
    ;(await this.#observable).observe(observer)
    await observer.readPromise()
    return observer
  }
  async unobserve(observer) {
    const index = this.#observers.indexOf(observer)
    if(index == -1) {
      console.error("OBSERVER NOT FOUND", observer)
      throw new Error("observer not found")
    }
    this.#observers.splice(index, 1)
    ;(await this.#observable).unobserve(observer)
  }
  async dispose() {
    this.disposed = true
    for(let observer of this.#observers) {
      ;(await this.#observable).unobserve(observer)
    }
  }
}

class ObjectReader extends Reader {
  #table = null
  #id = null

  constructor(queryReader, table, id) {
    super(queryReader)
    this.#table = table
    this.#id = id
  }
  observableFactory() {
    return this.#table.objectObservable(this.#id)
  }
  onChange(cb) {
    return this.startObserver((r) => new ObjectObserver(r, cb))
  }
  get() {
    //console.log("OBJ GET", this.#table.name, this.#id)
    return this.#table.objectGet(this.#id)
  }
}

class RangeReader extends Reader {
  #table = null
  #range = null

  constructor(queryReader, table, range) {
    super(queryReader)
    this.#table = table
    this.#range = range
  }
  async observableFactory() {
    return await (await this.#table).rangeObservable(this.#range)
  }
  onChange(cb) {
    return this.startObserver((r) => new RangeObserver(r, cb))
  }
  async get() {
    return await (await this.#table).rangeGet(this.#range)
  }
}

class TableReader extends Reader {
  #prefix = null
  #table = null

  constructor(queryReader, prefix, table) {
    super(queryReader)
    this.#prefix = prefix
    this.#table = table
  }
  async observableFactory() {
    return await (await this.#table).rangeObservable({})
  }
  onChange(cb) {
    return this.startObserver((r) => new RangeObserver(r, cb))
  }
  range(range) {
    return this._queryReader.getExistingReaderOrCreate(this.#prefix+':'+JSON.stringify(range),
        () => new RangeReader(this._queryReader, this.#table, range))
  }
  object(id) {
    return this._queryReader.getExistingReaderOrCreate(this.#prefix+'#'+id,
        () => new ObjectReader(this._queryReader, this.#table, id))
  }
  async get(range = {}) {
    return await (await this.#table).rangeGet(range)
  }
  async count(range = {}) {
    return await (await this.#table).countGet(range)
  }
}


const READER_READING = 0
const READER_OBSERVING = 1
const READER_DISPOSED = 2
const readerStates = ['reading', 'observing', 'disposed']

class QueryReader {
  #database = null
  #readers = new Map()
  #onNewSource = null

  constructor(database, onNewSource) {
    this.#database = database
    this.state = READER_READING
    this.#onNewSource = onNewSource
  }
  getExistingReaderOrCreate(key, create) {
    let reader = this.#readers.get(key)
    if(!reader) {
      reader = create()
      this.#readers.set(key, reader)
    }
    if(reader.then) return reader.then(rd => {
      this.#readers.set(key, rd)
      return rd
    })
    return reader
  }
  table(name) {
    if(this.#onNewSource) this.#onNewSource('table', name)
    const prefix = 'table_' + name
    return this.getExistingReaderOrCreate(prefix,
        () => new TableReader(this, prefix, this.#database.table(name)))
  }
  log(name) {
    if(this.#onNewSource) this.#onNewSource('log', name)
    const prefix = 'log_' + name
    return this.getExistingReaderOrCreate(prefix,
        () => new TableReader(this, prefix, this.#database.log(name)))
  }
  async index(name) {
    const prefix = 'index_' + name
    if(this.#onNewSource) this.#onNewSource('index', name)
    return await this.getExistingReaderOrCreate(prefix,
        async () => new TableReader(this, prefix, await this.#database.index(name)))
  }
  dispose() {
    this.state = READER_DISPOSED
    for(const reader of this.#readers.values()) {
      if(reader.then) {
        reader.then(rd => rd.dispose())
      } else {
        reader.dispose()
      }

    }
  }
}

class QueryWriter {
  #observable = null
  #database = null
  #results = new Map()
  #locks = new Map()
  #observationMode = false
  #reverse = false

  constructor(observable, database) {
    this.#observable = observable
    this.#database = database
  }
  setReverse(reverse) {
    this.#reverse = reverse
  }
  put(object) {
    if(this.#observationMode) {
      this.#observable.putObject(object, undefined, this.#reverse)
    } else {
      this.#results.set(object.id, object)
    }
  }
  delete(object) {
    if(this.#observationMode) {
      this.#observable.deleteObject(object)
    } else {
      this.#results.delete(object.id)
    }
  }
  change(obj, oldObj) {
    //if(!obj && !oldObj) throw new Error("empty change in observable query")
    if(this.#observationMode) {
      if(obj) {
        if(oldObj && oldObj.id != obj.id) {
          this.#observable.deleteObject(oldObj)
          this.#observable.putObject(obj, null, this.#reverse)
        } else {
          this.#observable.putObject(obj, oldObj, this.#reverse)
        }
      } else {
        if(oldObj) this.#observable.deleteObject(oldObj)
      }
    } else {
      if (oldObj) this.delete(oldObj.id)
      if (obj) this.put(obj)
    }
  }
  get(id) {
    if(this.#observationMode) {
      return this.#observable.list.find(o => o.id == id)
    } else {
      return this.#results.get(id)
    }
  }
  table(name) {
    return new TableWriter(this.#database.table(name))
  }
  log(name) {
    return new LogWriter(this.#database.log(name))
  }
  getResultsAndStartObservation() {
    this.#observationMode = true
    this.#observable.set(
        Array.from(this.#results.entries())
            .sort(this.#reverse
                ? (a,b) => a[0] > b[0] ? -1 : ( a[0] < b[0] ? 1 : 0)
                : (a,b) => a[0] > b[0] ? 1 : ( a[0] < b[0] ? -1 : 0) )
            .map(a => a[1])
    )
    this.#results = null
  }
  getSingleResultAndStartObservation() {
    this.#observationMode = true
    const value = (
        Array.from(this.#results.entries())
            .sort(this.#reverse
                ? (a,b) => a[0] > b[0] ? -1 : ( a[0] < b[0] ? 1 : 0)
                : (a,b) => a[0] > b[0] ? 1 : ( a[0] < b[0] ? -1 : 0) )
            .map(a => a[1])
    )[0] || null
    this.#observable.set(value)
    this.#results = null

  }
  async synchronized(key, code) {
    let promise = this.#locks.get(key)
    while(promise) {
      await promise
      promise = this.#locks.get(key)
    }
    promise = (async () => {
      let result = await code()
      this.#locks.delete(key)
      return result
    })()
    this.#locks.set(key, promise)
    return await promise
  }
  debug(...args) {
    console.log('QUERY DEBUG', ...args)
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
    this.writer = new QueryWriter(this, this.database)
    await this.code(this.reader, this.writer)
    this.reader.state = READER_OBSERVING
    this.writer.getResultsAndStartObservation()
  }

  async putObject(object, oldObject, reverse) {
    await this.readPromise
    const id = object.id
    this.putByField('id', id, object, reverse, oldObject)
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


class QuerySingleObservable extends ReactiveDao.ObservableValue {
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
    this.writer = new QueryWriter(this, this.database)
    await this.code(this.reader, this.writer)
    this.reader.state = READER_OBSERVING
    this.writer.getSingleResultAndStartObservation()
  }

  async putObject(object, oldObject) {
    await this.readPromise
    this.set(object, oldObject)
  }

  async deleteObject(object) {
    await this.readPromise
    this.set(null, object)
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

function querySingleObservable(database, code) {
  return new QuerySingleObservable(database, code)
}

queryObservable.single = querySingleObservable
queryObservable.QueryWriter = QueryWriter
queryObservable.QueryReader = QueryReader

module.exports = queryObservable
