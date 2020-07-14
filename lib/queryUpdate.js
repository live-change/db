
class TableWriter {
  #table = null

  constructor(table) {
    this.#table = table
  }
  put(object) {
    return this.#table.put(object)
  }

  delete(id) {
    return this.#table.delete(id)
  }

  update(id, operations, options) {
    return this.#table.update(id, operations, options)
  }
}

class LogWriter {
  #log = null

  constructor(table) {
    this.#log = log
  }

  put(object) {
    return this.#log.put(object)
  }

  flag(id, flags) {
    return this.#log.flag(id, flags)
  }
}

module.exports = { TableWriter, LogWriter }
