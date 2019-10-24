const express = require('express')
const bodyParser = require('body-parser')
const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const app = express()

app.use(bodyParser.json())

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'group-received' })

app.post('/', async(req, res)=>{
    await producer.send({
        topic: 'test-topic',
        messages: [
            {value: 'Hello Kafka!'}
        ],
    })

    return res.json({ ok: true });
})

async function run() {
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({ topic: 'callback-topic' });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Resposta de callback: ${message.value.toString()}`);
        },
    });
}

run().catch(console.error).then(()=>{
    app.listen(3000, console.log("Server ON"))
})