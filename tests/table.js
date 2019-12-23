const test = require('tape')
const levelup = require('levelup')
//const leveldown = require('leveldown')
const rocksdb = require('rocksdb')
const memdown = require('memdown')
const subleveldown = require('subleveldown')
const encoding = require('encoding-down')
const rimraf = require("rimraf")

const Database = require("../lib/Database.js")
const Store = require('@live-change/db-store-level')

const dbPath = `./test.t.db`
rimraf.sync(dbPath)

let users = [
  { id: '1', name: 'david' },
  { id: '2', name: 'thomas' },
  { id: '3', name: 'george' },
  { id: '4', name: 'donald' },
  { id: '5', name: 'david' }
]
let messages = [
  { id: '1', author: 1, text: "Hello!" },
  { id: '2', author: 2, text: "Hi!" },
  { id: '3', author: 1, text: "Bla bla bla" },
  { id: '4', author: 3, text: "IO XAOS" },
  { id: '5', author: 4, text: "Bye" },
]

test("store range observable", t => {
  t.plan(5)

  let level, db, usersTable, messagesTable

  t.test('open database', async t => {
    t.plan(1)
    level = levelup(rocksdb(dbPath), { infoLogLevel: 'debug' }, function (err, lvl) {
      if(err) throw new Error(err)
      db = new Database({}, (name, config) => {
        return new Store(subleveldown(level, name, { keyEncoding: 'ascii', valueEncoding: 'json' }))
      })
      t.pass('opened')
    })
  })

  t.test("create tables", async t => {
    t.plan(1)
    usersTable = db.createTable('users')
    messagesTable = db.createTable('messages')
    t.pass('tables created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let user of users) await usersTable.put(user)
    for(let message of messages) await messagesTable.put(message)
    t.pass("data inserted to database")
  })

  t.test("read everything", async t => {
    t.plan(1)
    level.createReadStream({ keys: true, values: true })
      .on('data', function ({ key, value }) {
        console.log("key:", key.toString('ascii') )
        console.log("value:", value.toString('ascii') )
      })
      .on('error', function (err) {
      })
      .on('close', function () {
      })
      .on('end', function () {
        t.pass("readed")
      })
  })

  t.test("close and remove database", async t => {
    t.plan(2)
    await level.close()
    t.pass('closed')
    rimraf(dbPath, (err) => {
      if(err) return t.fail(err)
      t.pass('removed')
    })
  })
})