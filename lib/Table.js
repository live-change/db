const OpLogger = require('./OpLogger.js')
const AtomicWriter = require('./AtomicWriter.js')
const ReactiveDao = require('@live-change/dao')

function opLogWritter(store) {
  let lastTime = Date.now()
  let lastId = 0
  return function(operation) {
    const now = Date.now()
    if(now == lastTime) {
      lastId ++
    } else {
      lastId = 0
      lastTime = now
    }
    const id = ((''+lastTime).padStart(16, '0'))+':'+((''+lastId).padStart(6, '0'))
    store.put({ id, timestamp: lastTime, operation })
    return id
  }
}

class Table {
  constructor(database, name, config) {
    this.database = database
    this.name = name
    this.configObservable = new ReactiveDao.ObservableValue(config)

    this.data = database.store(config.uid + '.data', { ...config, ...config.data })
    this.opLog = database.store(config.uid + '.opLog', { ...config, ...config.opLog })

    this.opLogWritter = opLogWritter(this.opLog)
    this.opLogger = new OpLogger(this.data, this.opLogWritter)

    this.atomicWriter = new AtomicWriter(this.opLogger)

    this.locks = new Map()
  }

  objectGet(key) {
    return this.data.objectGet(key)
  }

  objectObservable(key) {
    return this.data.objectObservable(key)
  }

  rangeGet(range) {
    return this.data.rangeGet(range)
  }

  rangeObservable(range) {
    return this.data.rangeObservable(range)
  }

  countGet(range) {
    return this.data.countGet(range)
  }

  countObservable(range) {
    return this.data.countObservable(range)
  }

  async put(object) {
    try {
      return await this.atomicWriter.put(object)
    } catch(e) {
      console.error("ERROR WHILE PUTTING OBJECT", object.id, "TO TABLE", this.name)
      console.error(e)
      throw e
    }
  }

  delete(id) {
    return this.atomicWriter.delete(id)
  }

  update(id, operations, options) {
    if(typeof id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    return this.atomicWriter.update(id, operations, options)
  }

  async clearOpLog(lastTimestamp, limit) {
    const now = Date.now()
    const nowStr = ((''+now).padStart(16, '0'))
    if(lastTimestamp > now) throw new Error('cannot clear oplog in the future')
    const opLogStart = (await this.opLog.rangeGet({ gt: '', limit: 1 }))[0]
    if(!opLogStart) return { count: 0, last: "\xFF\xFF\xFF\xFF" }
    let logId
    try {
      logId = this.opLogWritter({
        type: 'clearOpLog',
        from: opLogStart.id,
        to: nowStr
      })
    } catch(e) { // impossible to put anything - database full - first delete something
      logId = null
    }
    const removedStats = await this.opLog.rangeDelete({
      lt: nowStr,
      limit
    })
    if(!logId) { // Panic mode
      logId = this.opLogWritter({
        type: 'clearOpLog',
        from: opLogStart.id,
        to: nowStr
      })
    }
    const opLogNewStart = (await this.opLog.rangeGet({ gt: '', limit: 1 }))[0]
    if(opLogNewStart) {
      this.opLog.put({
        id: logId,
        operation: {
          type: 'clearOpLog',
          from: opLogStart.id,
          to: opLogNewStart ? opLogNewStart.id : (('' + lastTimestamp).padStart(16, '0'))
        }
      })
    }
    return removedStats
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

  async deleteTable() {
    this.atomicWriter.cancel()
    const config = this.configObservable.value
    await this.database.deleteStore(config.uid + '.data')
    await this.database.deleteStore(config.uid + '.opLog')
  }
}

module.exports = Table
