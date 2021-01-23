const { TableWriter, LogWriter } = require('./queryUpdate.js')

const maxGetLimit = 256

class ObjectReader {
  #table = null
  #id = null

  constructor(table, id) {
    this.#table = table
    this.#id = id
  }
  async onChange(cb) {
    await cb(await (await this.#table).objectGet(this.#id), null)
  }
  unobserve(obs) {}
  async get() {
    return await (await this.#table).objectGet(this.#id)
  }
}

class RangeReader {
  #table = null
  #range = null

  constructor(table, range) {
    this.#table = table
    this.#range = range
  }
  async onChange(cb) {
    let objects = await (await this.#table).rangeGet(this.#range)
    await Promise.all(objects.map(object => cb(object, null)))
  }
  unobserve(obs) {}
  onDelete(cb) {}
  async get() {
    return await (await this.#table).rangeGet(this.#range)
  }
}

class TableReader {
  #table = null

  constructor(table) {
    this.#table = table
  }
  async onChange(cb) {
    let results = []
    let objects = []
    let range = { limit: maxGetLimit }
    while(true) {
      objects = await (await this.#table).rangeGet(range)
      results = results.concat(await Promise.all(objects.map(object => cb(object, null))))
      if(objects.length == maxGetLimit)  {
        range.gt = objects[objects.length - 1].id
        console.log("GET LIMIT REACHED! GETTING MORE", range)
      } else {
        break // all processed
      }
    }
    return results
  }
  unobserve(obs) {}
  range(range) {
    return new RangeReader(this.#table, range)
  }
  object(id) {
    return new ObjectReader(this.#table, id)
  }
  get() {
    return this.#table.rangeGet({})
  }
}

class QueryReader {
  #database = null

  constructor(database) {
    this.#database = database
  }
  table(name) {
    return new TableReader(this.#database.table(name))
  }
  index(name) {
    return new TableReader(this.#database.index(name))
  }
  log(name) {
    return new TableReader(this.#database.log(name))
  }
}

class QueryWriter {
  #database = null
  #results = new Map()
  #locks = new Map()
  #canUpdate = false
  #reverse = false

  constructor(database, canUpdate) {
    this.#database = database
    this.#canUpdate = canUpdate
  }
  setReverse(reverse) {
    this.#reverse = reverse
  }
  put(object) {
    this.#results.set(object.id, object)
  }
  delete(object) {
    this.#results.delete(object.id)
  }
  change(obj, oldObj) {
    if(!oldObj && !obj) throw new Error("Empty query get change")
    if(oldObj) return this.delete(oldObj)
    if(obj) return this.put(obj)
  }
  tryChange(obj, oldObj) {
    if(!oldObj && !obj) return
    return this.change(obj, oldObj)
  }
  get(id) {
    return this.#results.get(id)
  }
  table(name) {
    if(!this.#canUpdate) throw new Error("Can't update table in read query")
    return new TableWriter(this.#database.table(name))
  }
  log(name) {
    if(!this.#canUpdate) throw new Error("Can't update log in read query")
    return new LogWriter(this.#database.log(name))
  }
  getResults() {
    return Array.from(this.#results.entries())
        .sort(this.#reverse
            ? (a,b) => a[0] > b[0] ? -1 : ( a[0] < b[0] ? 1 : 0)
            : (a,b) => a[0] > b[0] ? 1 : ( a[0] < b[0] ? -1 : 0) )
        .map(a => a[1])
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

async function queryGet(database, code, canUpdate = false) {
  const reader = new QueryReader(database)
  const writer = new QueryWriter(database, canUpdate)
  await code(reader, writer)
  return writer.getResults()
}

async function querySingleGet(database, code) {
  const reader = new QueryReader(database)
  const writer = new QueryWriter(database)
  await code(reader, writer)
  return writer.getResults()[0] || null
}

queryGet.single = querySingleGet
queryGet.QueryWriter = QueryWriter
queryGet.QueryReader = QueryReader

module.exports = queryGet
