const AtomicWriter = require('./AtomicWriter.js')

class Log {
  constructor(database, name, config) {
    this.database = database
    this.name = name
    this.configObservable = new ReactiveDao.ObservableValue(config)

    this.data = database.store(this.name + '.log', {...config, ...config.data})

    this.atomicWriter = new AtomicWriter(this.data)

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
    this.atomicWriter.put({ ...log, id, timestamp: this.lastTime, flags: {} })
    return id
  }

  flag(id, flags = []) {
    this.atomicWriter.update(id, { op: 'merge', property: 'flags', value: { ...flags } })
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

}

module.exports = Log
