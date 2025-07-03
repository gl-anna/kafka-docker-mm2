import {Kafka, logLevel} from 'kafkajs';

const kafkaA = new Kafka({
    clientId: 'admin_A',
    brokers: ['localhost:9092'],
    logLevel: logLevel.ERROR
});

// const kafkaB = new Kafka({
//     clientId: 'admin_B',
//     brokers: ['localhost:8092'],
//     logLevel: logLevel.ERROR
// });

async function createTopic(kafka: Kafka, topic: string) {
    const admin = kafka.admin();
    await admin.connect();

    // Check if topic exists before creating
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(topic)) {
        await admin.createTopics({
            topics: [{
                topic,
                numPartitions: 3,
                replicationFactor: 1
            }]
        });
        console.log(`Created topic: ${topic}`);
    }

    await admin.disconnect();
}

async function produceMessages(kafka: Kafka, topic: string, prefix: string) {
    const producer = kafka.producer();
    await producer.connect();

    console.log(`Producing messages to ${topic}...`);

    // Create unique messages with timestamp
    const timestamp = Date.now();
    for (let i = 0; i < 10; i++) {
        const msg = `${prefix}_${i}_${timestamp}`;
        await producer.send({
            topic,
            messages: [{value: msg}]
        });
        console.log(`Produced: ${msg}`);
    }

    await producer.disconnect();
}

async function setup() {
    try {
        // const runId = Date.now(); // Unique ID for this test run

        /*
        * MirrorMaker2 creates topics automatically
        * when it replicates messages, it will create topics in Cluster B
        * */

        // Cluster A setup (source cluster)
        await createTopic(kafkaA, 'topic1');
        await produceMessages(kafkaA, 'topic1', 'test_msg');

        console.log("Waiting 30 seconds for MirrorMaker2 replication..."); // TODO: check this delay
        await new Promise(resolve => setTimeout(resolve, 30000));

        console.log("Setup completed successfully!");
    } catch (err) {
        console.error("Setup failed:", err);
        process.exit(1);
    }
}

setup();