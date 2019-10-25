const express = require('express')
const bodyParser = require('body-parser')
const {Kafka, logLevel} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
    logLevel: logLevel.NOTHING = 0
})

const app = express()

app.use(bodyParser.json())

const producer = kafka.producer()
app.get('/', async(req, res)=>{
    await producer.send({
        topic: 'test-topic',
        messages: [
            {value: 'Hello Kafka!'}
        ],
    })

    return res.json({ ok: 'Enviado com sucesso' });
})

async function run() {
    await producer.connect()
}

run().catch(console.error).then(()=>{
    app.listen(3000, console.log("Server ON http://localhost:3000"))
})