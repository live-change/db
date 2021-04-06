
class ProfileLog {
  constructor() {
    this.started = false
    this.output = null
  }

  startLog(output, performance) {
    this.output = output
    this.performance = performance
    this.started = true
  }

  async log(operation) {
    if(!this.started) return
    if(this.output) await this.output(operation)
  }

  async begin(operation) {
    if(!this.started) return
    const now = new Date()
    const op = { ...operation, start: now, time: now, type: "started", perfStart: this.performance.now() }
    await this.log(op)
    return op
  }

  async end(op) {
    if(!this.started) return
    const now = new Date()
    const perfNow = this.performance.now()
    if(!op.start) console.error("NO OP START IN", op)
    op.type = 'finished'
    op.end = now
    op.perfEnd = perfNow
    op.duration = perfNow - op.perfStart//now.getTime() - op.start.getTime()
    await this.log(op)
    return op
  }

  async endPromise(op, promise) {
    if(!this.started) return promise
    if(!op.start) throw new Error("no op start")
    await promise.then(res => {
      this.end({ ...op, result: 'done' })
    }).catch(error => {
      this.end({ ...op, result: 'error', error })
    })
    return promise
  }

  async profile(operation, code) {
    if(!this.started) return code()
    const op = await this.begin(operation)
    try {
      return await code()
    } finally {
      await this.end(op)
    }
  }

  profileFunctions(functions, mapper = x=>x) {
    const profiler = this
    for(const funcName in functions) {
      const target = functions[funcName]
      const paramNames = getParamNames(target)
      functions[funcName] = function(...args) {
        const params = {}
        for(let i = 0; i < paramNames.length; i++) {
          params[paramNames[i]] = args[i]
        }
        return profiler.profile(
            mapper({ operation: funcName, ...params }),
            function() {
              return target.apply(functions, args)
            })
      }
    }
    return functions
  }
}

const STRIP_COMMENTS = /((\/\/.*$)|(\/\*[\s\S]*?\*\/))/mg
const ARGUMENT_NAMES = /([^\s,]+)/g
function getParamNames(func) {
  const fnStr = func.toString().replace(STRIP_COMMENTS, '')
  const result = fnStr.slice(fnStr.indexOf('(')+1, fnStr.indexOf(')')).match(ARGUMENT_NAMES)
  if(result === null) return []
  return result
}

const profileLog = new ProfileLog()
profileLog.ProfileLog = ProfileLog

module.exports = profileLog