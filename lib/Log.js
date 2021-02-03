const AtomicWriter = require('./AtomicWriter.js')
const ReactiveDao = require("@live-change/dao")

class Log {
  constructor(database, name, config) {
    this.database = database
    this.name = name
    this.configObservable = new ReactiveDao.ObservableValue(config)

    this.data = database.store(config.uid + '.log', {...config, ...config.data})

    this.lastTime = Date.now()
    this.lastId = 0
  }

  put(log) {
    const now = Date.now()
    if(now == this.lastTime) {
      this.lastId ++
    } else {
      this.lastId = 0
      this.lastTime = now
    }
    const id = ((''+this.lastTime).padStart(16, '0'))+':'+((''+this.lastId).padStart(6, '0'))
    this.data.put({ ...log, id, timestamp: this.lastTime })
    return id
  }

  putOld(log) {
    this.data.put(log)
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

  async deleteLog() {
    const config = this.configObservable.value
    await this.database.deleteStore(config.uid + '.log')
  }

}

module.exports = Log
