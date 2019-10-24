const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'test-group' })
const producer = kafka.producer()
async function run(){
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic' })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            setTimeout(async () => {
                await producer.send({
                    topic: 'callback-topic',
                    messages: [
                        { value: message.value.toString()}
                    ]
                })
            }, 3000)
        },
    })
}

run().catch(console.error)