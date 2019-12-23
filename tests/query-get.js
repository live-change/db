const test = require('tape')
const levelup = require('levelup')
const leveldown = require('leveldown')
const subleveldown = require('subleveldown')
const encoding = require('encoding-down')
const rimraf = require("rimraf")

const Database = require("../lib/Database.js")
const Store = require('@live-change/db-store-level')

const dbPath = `./test.qg.db`
rimraf.sync(dbPath)
const level = levelup(leveldown(dbPath))

const db = new Database({}, (name, config) => {
  return new Store(subleveldown(level, name, { keyEncoding: 'ascii', valueEncoding: 'json' }))
})

const users = [
  { id: '1', name: 'david' },
  { id: '2', name: 'thomas' },
  { id: '3', name: 'george' },
  { id: '4', name: 'donald' },
  { id: '5', name: 'david' }
]
const messages = [
  { id: '1', author: '1', text: "Hello!" },
  { id: '2', author: '2', text: "Hi!" },
  { id: '3', author: '1', text: "Bla bla bla" },
  { id: '4', author: '3', text: "IO XAOS" },
  { id: '5', author: '4', text: "Bye" }
]

test("store range observable", t => {
  t.plan(6)

  let usersTable, messagesTable, userByName, messagesByUser

  t.test("create tables and indexes", async t => {
    t.plan(1)
    usersTable = db.createTable('users')
    messagesTable = db.createTable('messages')
    const nameMapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
    userByName = await db.createIndex("userByName", async (input, output) => {
      await input.table('users').onChange((obj, oldObj) =>
          output.change(obj && nameMapper(obj), oldObj && nameMapper(oldObj)) )
    })
    const authorMapper = (obj) => ({ id: obj.author+'_'+obj.id, to: obj.id })
    messagesByUser = await db.createIndex("messagesByUser", async (input, output) => {
      await input.table('messages').onChange((obj, oldObj) =>
          output.change(obj && authorMapper(obj), oldObj && authorMapper(oldObj)) )
    })
    t.pass('tables and indexes created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let user of users) await usersTable.put(user)
    for(let message of messages) await messagesTable.put(message)
    t.pass("data inserted to database")
  })

  t.test("query users", async t => {
    t.plan(1)
    const results = await db.queryGet(async (input, output) => {
      await input.table('users').onChange((obj, oldObj) => output.change(obj, oldObj) )
    })
    t.deepEqual(results, users, 'query result')
  })

  t.test("query for users by name", async t => {
    t.plan(1)
    const results = await db.queryGet(async (input, output) => {
      await input.table('users').onChange((obj, oldObj) =>
          output.change(obj && { id: obj.name+'_'+obj.id, to: obj.id }, oldObj && { id: oldObj.name+'_'+obj.id, to: obj.id }) )
    })
    t.deepEqual(results, [{ id: 'david_1', to: '1' }, { id: 'david_5', to: '5' }, { id: 'donald_4', to: '4' },
      { id: 'george_3', to: '3' }, { id: 'thomas_2', to: '2' }], 'query result')
  })

  t.test("query messages with users", async t => {
    t.plan(1)
    const results = await db.queryGet(async (input, output) => {
      await input.table('messages').onChange((obj, oldObj) => output.synchronized(obj.id, async () => {
        const user = obj && await input.table('users').object(obj.author).get()
        output.change(obj && { user, ...obj }, oldObj)
      }))
    })
    t.deepEqual(results, messages.map(msg => ({ user: users.find( u => u.id == msg.author ), ...msg })))
  })



  t.test("close and remove database", async t => {
    t.plan(1)
    await level.close()
    rimraf(dbPath, (err) => {
      if(err) return t.fail(err)
      t.pass('removed')
    })
  })
})