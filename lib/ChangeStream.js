class ChangeStream {
  constructor() {
  }
  onChange() {
    throw new Error("abstract method - not implemented")
  }
  to(output) {
    return this.onChange((obj, oldObj, id, timestamp) => output.change(obj, oldObj, id, timestamp))
  }
  filter(func) {
    const pipe = new ChangeStreamPipe()
    const observerPromise = this.onChange((obj, oldObj, id, timestamp) =>
        pipe.change(obj && func(obj) ? obj : null, oldObj && func(oldObj) ? oldObj : null, id, timestamp))
    pipe.master = this
    pipe.observerPromise = observerPromise
    return pipe
  }
  map(func) {
    const pipe = new ChangeStreamPipe()
    const observerPromise = this.onChange((obj, oldObj, id, timestamp) =>
        pipe.change(obj && func(obj), oldObj && func(oldObj), id, timestamp))
    pipe.master = this
    pipe.observerPromise = observerPromise
    return pipe
  }
  indexBy(func) {
    const pipe = new ChangeStreamPipe()
    const observerPromise = this.onChange((obj, oldObj, id, timestamp) => {
      const indList = obj && func(obj)
      const oldIndList = oldObj && func(oldObj)
      const ind = indList && indList.map(v => JSON.stringify(v)).join(':')+'_'+id
      const oldInd = oldIndList && oldIndList.map(v => JSON.stringify(v)).join(':')+'_'+id
      if(ind == oldInd) return // no index change, ignore
      if(ind) {
        pipe.change({ id: ind, to: id }, null, ind, timestamp)
      }
      if(oldInd) {
        pipe.change(null, { id: oldInd, to: id }, oldInd, timestamp)
      }
    })
    pipe.master = this
    pipe.observerPromise = observerPromise
    return pipe
  }
}

class ChangeStreamPipe extends ChangeStream {
  constructor() {
    super()
    this.callbacks = []
  }
  onChange(cb) {
    this.callbacks.push(cb)
    return cb
  }
  async unobserve(cb) {
    const cbIndex = this.callbacks.indexOf(cb)
    if(cbIndex == -1) throw new Error("observer not found")
    if(this.callbacks.length == 0) {
      this.master.unobservePromise = await this.observerPromise
    }
  }
  async change(obj, oldObj, id, timestamp) {
    for(const callback of this.callbacks) await callback(obj, oldObj, id, timestamp)
  }
}

module.exports = { ChangeStream, ChangeStreamPipe }