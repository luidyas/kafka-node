const express = require('express')
const bodyParser = require('body-parser')
const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const message = {
    user: {id: 1, name: 'test'},
    email: 'test@gmail.com',
    age: 99
}

const app = express()

app.use(bodyParser.json())

app.post('/', async(req, res)=>{
    const producer = kafka.producer()
    await producer.connect()
    await producer.send({
        topic: 'test-topic',
        messages: [
            { value: JSON.stringify(message) },
        ],
    })
    res.json({msg: 'Send with success'})
})

app.listen(3000, console.log('producer on '))