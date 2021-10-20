const vm = require('vm')
const crypto = require('crypto')
const { ChangeStreamPipe } = require('./ChangeStream.js')

const defaultNativeGlobals = [
  'Array', 'ArrayBuffer',
  'BigInt', 'BigInt64Array', 'BigUint64Array', 'Boolean',
  'console',
  'decodeURI', 'decodeURIComponent', 'DataView', 'Date',
  'encodeURI', 'encodeURIComponent', 'escape', 'eval', 'Error',
  'Float32Array', 'Float64Array', 'Function',
  'isFinite', 'Infinity', 'Int8Array', 'Int16Array', 'Int32Array', 'Intl',
  'JSON', 'Map', 'Math', 'NaN', 'Number', 'Object',
  'parseInt', 'Promise', 'Proxy',
  'RegExp', 'Set', 'String', 'Symbol',
  'undefined', 'unescape', 'Uint8Array', 'Uint8ClampedArray', 'Uint16Array', 'Uint32Array',
  'WeakSet', 'WeakMap', 'WebAssembly'
]

const defaultContext = {
  crypto,
  sha1(data) {
    if(typeof data != 'string') data = JSON.stringify(data)
    return crypto.createHash('sha1').update(data).digest('hex')
  },
  sha256(data) {
    if(typeof data != 'string') data = JSON.stringify(data)
    return crypto.createHash('sha256').update(data).digest('hex')
  },
  hash(data) {
    if(typeof data != 'string') data = JSON.stringify(data)
    return crypto.createHash('sha1').update(data).digest('hex')
  },
  pipe() {
    return new ChangeStreamPipe()
  },
  'performance': require('perf_hooks').performance,
  constructor: null
}

const filenameRE = /scriptFile:(\d+):(\d+)\)$/g

class ScriptContext {
  constructor(userContext, nativeGlobals = defaultNativeGlobals) {
    let context = {}
    for(const key of nativeGlobals) {
      context[key] = global[key]
    }
    this.context = vm.createContext({ ...defaultContext, ...context, ...userContext })
/*    vm.runInContext(`
    (function() {
      const allowed = ${JSON.stringify(nativeGlobals.concat(Object.keys(userContext)))}
      const keys = Object.getOwnPropertyNames(this)
      keys.forEach((key) => {
        const item = this[key]
        if(!item) return
        this[key].constructor = undefined
        if(allowed.indexOf(key)) return
        this[key] = undefined
      })
    })()
    `, this.context, { filename: 'init' })*/

  }

  run(code, filename) {
    return vm.runInContext(code, this.context, { filename })
  }

  createFunctionFromCode(code, paramsNames, filename) {
    let func
    try {
      func = vm.runInContext(`async function(${Object.keys(paramsNames).join(', ')}) {\n${code}\n}`,
          this.context, { filename: 'scriptFile' })
    } catch(e) {
      const mappedTrace = e.replace(filenameRE, (all, line, column) => `${filename}:${+line-1}:${column}`)
      console.error("SCRIPT COMPILATION ERROR:\n" + mappedTrace)
      throw new Error("SCRIPT COMPILATION ERROR:\n" + mappedTrace)
    }
    return (...args) => {
      try {
        return func(...args)
      } catch(e) {
        const mappedTrace = e.replace(filenameRE, (all, line, column) => `${filename}:${+line-1}:${column}`)
        throw new Error(mappedTrace)
      }
    }
  }
}

module.exports = ScriptContext
