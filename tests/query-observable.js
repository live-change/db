const test = require('tape')
const rimraf = require("rimraf")

const dbPath = `./test.qo.db`
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

test("query observable", t => {
  t.plan(7)

  let level, db, usersTable, messagesTable, messagesByUser

  t.test('open database', async t => {
    t.plan(1)
    db = require('./utils/createDb.js')(dbPath)
    t.pass('opened')
  })

  t.test("create tables and indexes", async t => {
    t.plan(1)
    usersTable = db.createTable('users')
    messagesTable = db.createTable('messages')
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

  let nextValueResolve
  let gotNextValue
  const getNextValue = () => {
    if(gotNextValue) {
      gotNextValue = false
      return queryObservable.list
    }
    return new Promise((resolve, reject) => nextValueResolve = resolve)
  }
  let queryObservable
  const queryObserver = (signal, value, ...rest) => {
    console.log("SIGNAL", signal, value, ...rest)
    gotNextValue = true
    if(nextValueResolve) nextValueResolve(queryObservable.list)
  }


  t.test("query users", async t => {
    t.plan(4)
    queryObservable = db.queryObservable(async (input, output) => {
      await input.table('users').onChange((obj, oldObj) => output.change(obj, oldObj) )
    })
    queryObservable.observe(queryObserver)
    const results = await getNextValue()
    t.deepEqual(results, users, 'query result')

    const newUser = { id: '6', name: 'arnold' }
    users.push(newUser)
    await usersTable.put(newUser)
    await delay(100)
    let updated = await getNextValue()
    t.deepEqual(updated, users)

    users = users.filter(u => u.id != "3")
    await usersTable.delete("3")
    await delay(100)
    updated = await getNextValue()
    t.deepEqual(updated, users)

    queryObservable.unobserve(queryObserver)
    t.pass('unobserved')
  })

  const idSort = (a,b) => a.id > b.id ? 1 : (a.id < b.id ? -1 : 0)

  t.test("query for users by name", async t => {
    t.plan(5)
    gotNextValue = false
    const mapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
    queryObservable = db.queryObservable(async (input, output) => {
      await input.table('users').onChange((obj, oldObj) =>
          output.change(obj && mapper(obj), oldObj && mapper(oldObj)) )
    })
    queryObservable.observe(queryObserver)
    const results = await getNextValue()
    t.deepEqual(results, users.map(mapper).sort(idSort), 'query result')

    const newUser = { id: '3', name: 'jack' }
    users.push(newUser)
    users.sort(idSort)
    await usersTable.put(newUser)
    await delay(100)
    let updated = await getNextValue()
    t.deepEqual(updated, users.map(mapper).sort(idSort))

    users = users.filter(u => u.id != "4")
    await usersTable.delete("4")
    await delay(100)
    updated = await getNextValue()
    t.deepEqual(updated, users.map(mapper).sort(idSort))

    const updatedUser = users.find(u => u.id == "6")
    updatedUser.name = "henry"
    await usersTable.put(updatedUser)
    await delay(100)
    updated = await getNextValue()
    t.deepEqual(updated, users.map(mapper).sort(idSort))

    queryObservable.unobserve(queryObserver)
    t.pass('unobserved')
  })

  t.test("query messages with users", async t => {
    t.plan(5)
    gotNextValue = false
    queryObservable = db.queryObservable(async (input, output) => {
      await input.table('messages').onChange((obj, oldObj) => {
        return output.synchronized(obj ? obj.id : oldObj.id, async () => {
          const user = obj && await input.table('users').object(obj.author).get()
          output.change(obj && { user, ...obj }, oldObj)
        })
      })
      await input.table('users').onChange((obj, oldObj) => {
        const userId = obj ? obj.id : oldObj.id
        return output.synchronized('u_'+userId, async () => {
          const messageIds = await input.index('messagesByUser').range({ gte: userId, lt: userId + '\xFF' }).get()
          return Promise.all(messageIds.map(async mid => {
            const message = await input.table('messages').object(mid.to).get()
            if(message) await output.synchronized(message.id, async () => {
              output.change({ user: obj, ...message }, { user: oldObj, ...message })
            })
          }))
        })
      })
    })

    const jsResult = () => messages.map(msg => ({ user: users.find( u => u.id == msg.author ) || null, ...msg }))

    queryObservable.observe(queryObserver)
    let results = await getNextValue()
    t.deepEqual(results, jsResult())

    const newMessage = { id: '6', author: '1', text: "test" }
    messages.push(newMessage)
    await messagesTable.put(newMessage)
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())

    const newUser = { id: '4', name: 'james' }
    users.push(newUser)
    users.sort(idSort)
    await usersTable.put(newUser)
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())

    messages = messages.filter(m => m.id != '3')
    await messagesTable.delete("3")
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())

    users = users.filter(u => u.id != "2")
    await usersTable.delete("2")
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())
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