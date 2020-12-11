
class OpLogger {
  constructor(store, ...outputs) {
    this.store = store
    this.outputs = outputs
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
    if(typeof object.id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    let res = await this.store.put(object)
    if(JSON.stringify(object) == JSON.stringify(res)) return res
    for(let output of this.outputs) output({ type: 'put', object, oldObject: res })
    return res
  }

  async delete(id) {
    let object = await this.store.delete(id)
    if(object) {
      for(let output of this.outputs) output({ type: 'delete', object })
    }
    return object
  }

}

module.exports = OpLogger