const test = require('tape')
const rimraf = require("rimraf")

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
  t.plan(4)

  let db, usersTable, messagesTable

  t.test('open database', async t => {
    t.plan(1)
    db = require('./utils/createDb.js')(dbPath)
    t.pass('opened')
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

  t.test("close and remove database", async t => {
    t.plan(2)
    await db.close()
    t.pass('closed')
    rimraf(dbPath, (err) => {
      if(err) return t.fail(err)
      t.pass('removed')
    })
  })
})