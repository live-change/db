const test = require('tape')
const rimraf = require("rimraf")

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
let events = [
  { type: 'add', value: 1 },
  { type: 'sub', value: 2 },
  { type: 'add', value: 10 },
  { type: 'div', value: 3 },
  { type: 'add', value: 30 }
]

function delay(ms) {
  return new Promise((resolve, reject) => setTimeout(resolve, ms))
}

test("index", t => {
  t.plan(8)

  let level, db, usersTable, messagesTable, eventsLog, messagesByUser, userByName

  t.test('open database', async t => {
    t.plan(1)
    db = require('./utils/createDb.js')(dbPath)
    t.pass('opened')
  })

  t.test("create tables", async t => {
    t.plan(1)
    usersTable = db.createTable('users')
    messagesTable = db.createTable('messages')
    eventsLog = db.createLog('events')
    t.pass('tables created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let user of users) await usersTable.put(user)
    for(let message of messages) await messagesTable.put(message)
    for(let event of events) await eventsLog.put(event)
    t.pass("data inserted to database")
  })

  const idSort = (a,b) => a.id > b.id ? 1 : (a.id < b.id ? -1 : 0)

  t.test("create users by name index", async t => {
    t.plan(4)
    const mapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
    const index = await db.createIndex("userByName", async (input, output) => {
      const mapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
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
    messagesByUser = await db.createIndex("messagesByUser", async (input, output) => {
      const authorMapper = (obj) => ({ id: obj.author+'_'+obj.id, to: obj.id })
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

    console.log("INDEX CREATED")

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

  t.test("create events index by type", async t => {
    t.plan(2)
    const index = await db.createIndex("eventsByType", async (input, output) => {
      const mapper = (obj) => ({ id: obj.type+'_'+obj.id, to: obj.id })
      await input.log('events').onChange((obj, oldObj) =>
          output.change(obj && mapper(obj), oldObj && mapper(oldObj)) )
    })
    await delay(100)
    let results = await index.rangeGet({})
    t.equal(results.length, events.length, 'query result')

    const newEvent = { id: '7', name: 'henry' }
    events.push(newEvent)
    await eventsLog.put(newEvent)
    await delay(100)
    results = await index.rangeGet({})
    t.deepEqual(results.length, events.length)
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