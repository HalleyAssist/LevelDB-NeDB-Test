const levelup = require('levelup'),
      leveldown = require('leveldown'),
      encode = require('encoding-down'),
      datastore = require('nedb-promise').datastore,
      fs = require('fs'),
      fsPromises = fs.promises,
      assert = require('assert')

class leveldbDatabase {
    constructor(){
        this._db = null
    }
    get name(){
        return 'LevelDB'
    }
    async startDb(){
        if (this._db){
            this._db.close()
        }
        // Remove previous database
        await fsPromises.rm('./leveldb_db', {force: true, recursive: true}).catch()
        await fsPromises.mkdir('./leveldb_db')
    
        // Create new database
        this._db = levelup(encode(leveldown('./leveldb_db/leveldb_db'), { valueEncoding: 'json' }))
        for (let i = 0; i < 200; i++){
            await this.addItem(`startup_data${i}`, `startup_value${i}`)
        }
    }
    async addItem(key, value){
        await this._db.put(key, value)
    }
    async findItem(key){
        let item = await this._db.get(key)
        return item
    }
}

class nedbDatabase {
    constructor(){
        this._db = null
    }
    get name(){
        return 'NeDB'
    }
    async startDb(){
        if (this._db)
            await this._db.remove({ }, { multi: true })

        // Remove previous database
        await fsPromises.rm('./nedb_db', {force: true, recursive: true}).catch()
        await fsPromises.mkdir('./nedb_db')
    
        // Create new database
        this._db = datastore({
            filename: './nedb_db/db.json',
            autoload: true
        })

        for (let i = 0; i < 200; i++){
            await this.addItem(`startup_data${i}`, `startup_value${i}`)
        }
    }
    async addItem(key, value){
        const data = {
            [key]: value
        }
        await this._db.insert(data)
    }
    async findItem(key){
        const item = await this._db.findOne({ [key]: {$exists: true }})
        return item[key]
    }
}

async function runBasicTest(databaseObject){
    await databaseObject.startDb()
    const started = Date.now()

    for (let i = 0; i < 200; i++){
        await databaseObject.addItem(`key${i}`, `value${i}`)
    }
    for (let i = 0; i < 200; i++){
        const item = await databaseObject.findItem(`key${i}`)
        assert(item == `value${i}`)
    }

    const testTime = Date.now() - started
    return testTime
}

async function runPacketTest(databaseObject){
    await databaseObject.startDb()
    const started = Date.now()

    const packet = {
        cmd: 'publish',
        topic: 'hello/world',
        payload: Buffer.from('muahah'),
        qos: 0,
        retain: true
    }
    for (let i = 0; i < 200; i++){
        packet.qos = i
        await databaseObject.addItem(`key${i}`, packet)
    }
    for (let i = 0; i < 200; i++){
        const item = await databaseObject.findItem(`key${i}`)
        assert(item.qos == i)
    }

    const testTime = Date.now() - started
    return testTime
}

async function runEventTest(databaseObject){
    await databaseObject.startDb()
    const started = Date.now()

    const event = {
        name: 'name',
        _s: 0,
        rooms: {},
        lastWeather: {
            when: 55967638.040533334,
            what: {
                degrees: 23.9,
                cityName: "Melbourne, Victoria, AU",
                forecastLow: 16,
                forecastHigh: 24,
                forecastSunset: "19:37",
                forecastSunrise: "07:19",
                currentDescriptionCode: null,
                forecastDescriptionCode: "Sunny"
            }
        }
    }
    for (let i = 0; i < 200; i++){
        event._s = i
        await databaseObject.addItem(`key${i}`, event)
    }
    for (let i = 0; i < 200; i++){
        const item = await databaseObject.findItem(`key${i}`)
        assert(item._s == i)
    }

    const testTime = Date.now() - started
    return testTime
}

function averageResults(array){
    let total = 0
    for (const entry of array){
        total += entry
    }
    return total/(array.length)
}

async function main(){
    const databases = [
        new leveldbDatabase(),
        new nedbDatabase()
    ]

    for (const db of databases){
        // Basic kv test
        let basicTestTotal = []
        for (let i = 0; i < 5; i++){
            const timeTaken = await runBasicTest(db)
            basicTestTotal.push(timeTaken)
        }
        console.log(`Basic Test: ${db.name} took: ${basicTestTotal}ms (average: ${Math.round(averageResults(basicTestTotal))}ms)`)

        // MQTT Packet test
        let packetTestTotal = []
        for (let i = 0; i < 5; i++){
            const timeTaken = await runPacketTest(db)
            packetTestTotal.push(timeTaken)
        }
        console.log(`Packet Test: ${db.name} took: ${packetTestTotal}ms (average: ${Math.round(averageResults(packetTestTotal))}ms)`)

        // Event test
        let eventTestTotal = []
        for (let i = 0; i < 5; i++){
            const timeTaken = await runEventTest(db)
            eventTestTotal.push(timeTaken)
        }
        console.log(`Event Test: ${db.name} took: ${eventTestTotal}ms (average: ${Math.round(averageResults(eventTestTotal))}ms)`)
    }
}

main()