const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'test-group' })
async function run(){
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic' })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({value: message.value.toString()});
        },
    })
}

run().catch(console.error)