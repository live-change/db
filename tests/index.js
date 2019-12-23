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


const dbPath = `./test.i.db`
rimraf.sync(dbPath)

let users = [
  { id: '1', name: 'david' },
  { id: '2', name: 'thomas' },
  { id: '3', name: 'george' },
  { id: '4', name: 'donald' },
  { id: '5', name: 'david' }
]
let messages = [
  { id: '1', author: '1', text: "Hello!" },
  { id: '2', author: '2', text: "Hi!" },
  { id: '3', author: '1', text: "Bla bla bla" },
  { id: '4', author: '3', text: "IO XAOS" },
  { id: '5', author: '4', text: "Bye" }
]

function delay(ms) {
  return new Promise((resolve, reject) => setTimeout(resolve, ms))
}

test("index", t => {
  t.plan(8)

  let level, db, usersTable, messagesTable, messagesByUser, userByName

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

  const idSort = (a,b) => a.id > b.id ? 1 : (a.id < b.id ? -1 : 0)

  t.test("create users by name index", async t => {
    t.plan(4)
    const mapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
    const index = await db.createIndex("userByName", async (input, output) => {
      await input.table('users').onChange((obj, oldObj) =>
          output.change(obj && mapper(obj), oldObj && mapper(oldObj)) )
    })
    await delay(100)
    let results = await index.rangeGet({})
    t.deepEqual(results, users.map(mapper).sort(idSort), 'query result')

    const updatedUser = users.find(u => u.id == "3")
    updatedUser.name = "jack"
    usersTable.put(updatedUser)
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, users.map(mapper).sort(idSort))

    users = users.filter(u => u.id != "4")
    usersTable.delete("4")
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, users.map(mapper).sort(idSort))

    const newUser = { id: '7', name: 'henry' }
    users.push(newUser)
    await usersTable.put(newUser)
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, users.map(mapper).sort(idSort))

    userByName = index
  })

  t.test("create messages by user index", async t => {
    t.plan(1)
    const authorMapper = (obj) => ({ id: obj.author+'_'+obj.id, to: obj.id })
    messagesByUser = await db.createIndex("messagesByUser", async (input, output) => {
      await input.table('messages').onChange((obj, oldObj) =>
          output.change(obj && authorMapper(obj), oldObj && authorMapper(oldObj)) )
    })

    console.log("INDEX CREATED")

    let results = await messagesByUser.rangeGet({})
    t.deepEqual(results, messages.map(m => ({ id: m.author+'_'+m.id, to: m.id })).sort(idSort))
  })

  t.test("create messages with users index", async t => {
    t.plan(5)
    const index = await db.createIndex("messagesWithUsers", async (input, output) => Promise.all([
      input.table('messages').onChange((obj, oldObj) => {
        return output.synchronized(obj ? obj.id : oldObj.id, async () => {
          const user = obj && await input.table('users').object(obj.author).get()
          output.change(obj && {user, ...obj}, oldObj)
        })
      }),
      input.table('users').onChange((obj, oldObj) => {
        const userId = obj ? obj.id : oldObj.id
        return output.synchronized('u_' + userId, async () => {
          const messageIds = await input.index('messagesByUser').range({gte: userId, lt: userId + '\xFF'}).get()
          return Promise.all(messageIds.map(async mid => {
            const message = await input.table('messages').object(mid.to).get()
            if (message) await output.synchronized(message.id, async () => {
              output.change({user: obj, ...message}, {user: oldObj, ...message})
            })
          }))
        })
      })
    ]))

    const jsResult = () => messages.map(msg => ({user: users.find(u => u.id == msg.author) || null, ...msg}))

    await delay(100)
    let results = await index.rangeGet({})
    t.deepEqual(results, jsResult())

    const newMessage = { id: '6', author: '1', text: "test" }
    messages.push(newMessage)
    messagesTable.put(newMessage)
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, jsResult())

    const newUser = { id: '4', name: 'james' }
    users.push(newUser)
    users.sort(idSort)
    usersTable.put(newUser)
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, jsResult())

    messages = messages.filter(m => m.id != '3')
    messagesTable.delete("3")
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, jsResult())

    users = users.filter(u => u.id != "2")
    usersTable.delete("2")
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results, jsResult())

  })

  t.test('read all', async t => {
    t.plan(1)
    level.createReadStream({ keys: true, values: true }).on('data', function ({ key, value }) {
      //console.log("key:", key.toString('ascii') )
      //console.log("value:", value.toString('ascii') )
    })
    .on('error', function (err) {
    })
    .on('close', function () {
    })
    .on('end', ()=>t.pass('all'))
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