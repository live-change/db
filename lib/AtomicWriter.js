function getProperty(of, propertyName) {
  const path = propertyName.split('.')
  let p = of
  for(let part of path) p = p[part]
  return p
}
function hasProperty(of, propertyName) {
  const path = propertyName.split('.')
  let p = of
  for(let part of path.slice(0,-1)) p = p[part]
  return p && p.hasOwnProperty(path[path.length-1])
}
function setProperty(of, propertyName, value) {
  const path = propertyName.split('.')
  let t = of
  for(let part of path.slice(0,-1)) {
    t[part] = t[part] || {}
    t = t[part]
  }
  const last = path[path.length-1]
  t[last] = value
}
function deleteProperty(of, propertyName) {
  const path = propertyName.split('.')
  let t = of
  for(let part of path.slice(0,-1)) {
    t[part] = t[part] || {}
    t = t[part]
  }
  const last = path[path.length-1]
  delete t[last]
}
function updateProperty(of, propertyName, mt) {
  const path = propertyName.split('.')
  let t = of
  for(let part of path.slice(0,-1)) {
    t[part] = t[part] || {}
    t = t[part]
  }
  const last = path[path.length-1]
  t[last] = mt(t[last])
}

function isObject(item) {
  return (item && typeof item === 'object' && !Array.isArray(item))
}
function mergeDeep(target, ...sources) {
  if (!sources.length) return target
  const source = sources.shift()
  if (isObject(target) && isObject(source)) {
    for (const key in source) {
      if (isObject(source[key])) {
        if (!target[key]) Object.assign(target, { [key]: {} })
        mergeDeep(target[key], source[key])
      } else {
        Object.assign(target, { [key]: source[key] })
      }
    }
  }
  return mergeDeep(target, ...sources);
}

const tests = {
  gt(obj, { property, value }) {
    return getProperty(obj, property) > value
  },
  gte(obj, { property, value }) {
    return getProperty(obj, property) >= value
  },
  lt(obj, { property, value }) {
    return getProperty(obj, property) < value
  },
  lte(obj, { property, value }) {
    return getProperty(obj, property) <= value
  },
  eq(obj, { property, value }) {
    return JSON.stringify(getProperty(obj, property)) == JSON.stringify(value)
  },
  exist(obj, { property }) {
    return hasProperty(obj, property)
  },
  notExist(obj, { property }) {
    return !hasProperty(obj, property)
  }
}

const mutators = {
  set(obj, { property, value }) {
    setProperty(obj, property, value)
    return obj
  },
  delete(obj, { property }) {
    deleteProperty(obj, property)
    return obj
  },
  addToSet(obj, { property, value }) {
    const valueStr = JSON.stringify(value)
    updateProperty(obj, property,
            v => (v ? (Array.isArray(v) ? v : [v]) : []).filter(x => JSON.stringify(x) != valueStr).concat([value]))
    return obj
  },
  deleteFromSet(obj, { property, value }) {
    const valueStr = JSON.stringify(value)
    updateProperty(obj, property,
            v => (v ? (Array.isArray(v) ? v : [v]) : []).filter(x => JSON.stringify(x) != valueStr))
    return obj
  },
  mergeSets(obj, { property, values }) {
    const valuesStrs = values.map(e=>JSON.stringify(e))
    updateProperty(obj, property, v => {
      const current = (v ? (Array.isArray(v) ? v : [v]) : [])
      const currentStrs = current.map(e=>JSON.stringify(e))
      return Array.from(new Set([...valuesStrs, ...currentStrs])).map(x => JSON.parse(x))
    })
    return obj
  },
  add(obj, { property, value }) {
    updateProperty(obj, property, v => (v||0) + value)
    return obj
  },
  max(obj, { property, value }) {
    updateProperty(obj, property, v => {
      const current = (v || -Infinity)
      return current > value ? current : value
    })
    return obj
  },
  min(obj, { property, value }) {
    updateProperty(obj, property, v => {
      const current = (v || Infinity)
      return current < value ? current : value
    })
    return obj
  },
  merge(obj, { property, value }) {
    if(property) {
      updateProperty(obj, property, v =>  mergeDeep(v, value))
      return obj
    } else return mergeDeep(obj, value)
  },
  reverseMerge(obj, { property, value }) {
    if(property) {
      updateProperty(obj, property, v =>  mergeDeep(value, v))
      return obj
    } else return mergeDeep(value, obj)
  },
  conditional(value, { conditions, operations }) {
    for(const condition of conditions) {
      const test = tests[condition.test]
      if(!test) throw new Error(`test ${condition.test} not exists`)
      if(!test(value, condition)) return value
    }
    for(let operation of operations) {
      const mutator = mutators[operation.op]
      if(!mutator) throw new Error(`mutator "${operation.op}" not exists`)
      value = mutator(value, operation)
    }
    return value
  }
}

function mutate(value, operations) {
  for(let operation of operations) {
    const mutator = mutators[operation.op]
    if(!mutator) throw new Error(`mutator "${operation.op}" not exists`)
    value = mutator(value, operation)
  }
  return value
}

class WriteQueue {
  constructor(atomicWriter, store, id) {
    this.atomicWriter = atomicWriter
    this.store = store
    this.id = id
    if(typeof id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    this.readPromise = null
    this.writePromise = null
    this.updatePromise = null
    this.writeValue = undefined
    this.operations = []
  }

  tryDeleteQueue() {
    if(this.operations.length == 0) {
      //console.log("DELETE QUEUE", this.id)
      this.atomicWriter.writes.delete(this.id)
    }
  }

  async put(object) {
    if(typeof object.id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    if(!object.id) throw new Error(`ID is empty ${JSON.stringify(object)}`)
    this.operations = []
    if(this.writePromise) await this.writePromise
    this.writePromise = this.store.put(object).then(ok => this.writePromise = null)
    this.writeValue = object
    this.writePromise.then(ok => this.tryDeleteQueue())
    return this.writePromise
  }

  async delete() {
    this.operations = []
    if(this.writePromise) await this.writePromise
    this.writePromise = this.store.delete(this.id).then(ok => this.writePromise = null)
    this.writeValue = null
    this.writePromise.then(ok => this.tryDeleteQueue())
    return this.writePromise
  }

  async update(operations, options) {
    const first = this.operations.length == 0
    this.operations.push({ operations, options })
    //console.log("QUEUE UPDATE", this.id, this.operations, "FIRST", first)
    if(first) {
      if(this.writePromise) {
        //console.log("GOT WRITE PROMISE")
        this.updatePromise = this.writePromise.then(async written => {
          if(this.operations.length == 0) return
          //console.log("VALUE WRITTEN -> DOING NEXT UPDATE", this.id, this.operations)
          let value = JSON.parse(JSON.stringify(this.writeValue))
          for(const { operations, options } of this.operations) {
            //console.log("UPDATE OPS", operations, "OPTIONS", options)
            if(options && options.ifExists && !value) continue
            if(!value) value = { id: this.id }
            value = mutate(value, operations)
          }
          this.operations = []
          if(value) await this.put(value)
          //console.log(new Date().toISOString(), "UPDATE WRITTEN", this.id, this.operations)
          return [value, written]
        })
      } else if(!this.readPromise) {
        //console.log("READING STARTED")
        this.readPromise = this.store.objectGet(this.id)
        this.updatePromise = this.readPromise.then(async readed => {
          this.readPromise = null
          //console.log("VALUE READED -> DOING UPDATE", this.id, this.operations)
          if(this.operations.length == 0) return
          let value = JSON.parse(JSON.stringify(readed))
          for(const { operations, options } of this.operations) {
            //console.log("UPDATE OPS", operations, "OPTIONS", options)
            if(options && options.ifExists && !value) continue
            if(!value) value = { id: this.id }
            value = mutate(value, operations)
          }
          this.operations = []
          //console.log("PUT UPDATED", this.id, value)
          if(value) await this.put(value)
          //console.log("UPDATE WRITTEN", this.id, this.operations)
          return [value, readed]
        })
      } else {
        //console.log("WAITING FOR READ")
      }
    }
    return this.updatePromise
  }
}

class AtomicWriter {
  constructor(store) {
    this.store = store
    this.writes = new Map()
  }

  objectGet(key) {
    return this.store.objectGet(key)
  }

  objectObservable(key) {
    return this.store.objectObservable(key)
  }

  rangeGet(range) {
    return this.store.rangeGet(range)
  }

  rangeObservable(range) {
    return this.store.rangeObservable(range)
  }

  firstId() {
    return this.store.firstId()
  }
  lastId() {
    return this.store.lastId()
  }

  async put(object) {
    const id = object.id
    let queue = this.writes.get(id)
    if(!queue) {
      queue = new WriteQueue(this, this.store, id)
      this.writes.set(id, queue)
    }
    return await queue.put(object)
  }

  delete(id) {
    let queue = this.writes.get(id)
    if(!queue) {
      queue = new WriteQueue(this, this.store, id)
      this.writes.set(id, queue)
    }
    return queue.delete()
  }

  update(id, operations, options) {
    if(typeof id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    let queue = this.writes.get(id)
    if(!queue) {
      //console.log("NEW QUEUE")
      queue = new WriteQueue(this, this.store, id)
      this.writes.set(id, queue)
    }
    return queue.update(operations, options)
  }

  cancel() {
    for(const queue of this.writes.values()) {
      queue.operations = []
    }
  }
}

module.exports = AtomicWriter
