const express = require('express')
const app = express()
const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'consumer-app',
    brokers: ['localhost:9092']
})

app.get('/', async(req, res)=>{
    const consumer = kafka.consumer({ groupId: 'test-group' })
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic' })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            })
        },
    })
    return res.json({msg: 'recebido com sucesso'})
})
app.listen(3002, console.log('consumer on'))