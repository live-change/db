const vm = require('vm')

const defaultNativeGlobals = [
  'Array', 'ArrayBuffer', 'atob',
  'BigIng', 'BigInt64Array', 'BigUint64Array', 'Boolean', 'btoa',
  'console', 'crypto', 'Crypto', 'CryptoKey',
  'decodeURI', 'decodeURIComponent', 'DataView', 'Date',
  'encodeURI', 'encodeURIComponent', 'escape', 'eval', 'Error',
  'Float32Array', 'Float64Array', 'Function',
  'isFinite', 'isNan', 'Infinity', 'Int8Array', 'Int16Array', 'Int32Array', 'Intl',
  'JSON', 'Map', 'Math', 'NaN', 'Number', 'Object',
  'parseInt', 'performance', 'Promise', 'Proxy', 'PromiseRejectionEvent',
  'RegExp', 'Set', 'String', 'Symbol',
  'undefined', 'unescape', 'Uint8Array', 'Uint8ClampedArray', 'Uint16Array', 'Uint32Array',
  'WeakSet', 'WeakMap', 'WebAssembly'
]

const filenameRE = /scriptFile:(\d+):(\d+)\)$/g

class ScriptContext {
  constructor(userContext, nativeGlobals = defaultNativeGlobals) {
    this.context = vm.createContext({ userContext })
    vm.runInContext(`
    (function() {      
      const allowed = ${JSON.stringify(defaultNativeGlobals.concat(Object.keys(userContext)))} 
      const keys = Object.getOwnPropertyNames(this)    
      keys.forEach((key) => {
        const item = this[key]
        if(!item) return
        this[key].constructor = undefined
        if(allowed.indexOf(key)) return
        this[key] = undefined
      })
    })()
    `, this.context, { filename: 'init' })

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
