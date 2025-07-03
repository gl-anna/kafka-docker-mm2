import {Kafka, Consumer, logLevel} from 'kafkajs';

interface RecordDetails {
    topic: string;
    partition: number;
    offset: string;
    value: string;
}

async function main() {
    const kafkaA = new Kafka({
        clientId: 'consumer_A',
        brokers: ['localhost:9092'],
        logLevel: logLevel.ERROR
    });

    const kafkaB = new Kafka({
        clientId: 'consumer_B',
        brokers: ['localhost:8092'],
        logLevel: logLevel.ERROR
    });

    const topicA = 'topic1';
    // const topicB = 'clusterA.topic1'; // original version
    const topicB = 'topic1';

    console.log("Generating results table...");

    const mapA = new Map<string, RecordDetails>();
    const mapB = new Map<string, RecordDetails>();

    // Read from Cluster A
    const consumerA = kafkaA.consumer({
        groupId: `compare_group_A_${Date.now()}`,
        allowAutoTopicCreation: false,
        // fromBeginning: true
    });

    await consumerA.connect();
    await readAllMessages(consumerA, topicA, mapA);
    await consumerA.disconnect();

    // Read from Cluster B
    const consumerB = kafkaB.consumer({
        groupId: `compare_group_B_${Date.now()}`,
        allowAutoTopicCreation: false,
        // fromBeginning: true
    });

    await consumerB.connect();
    await readAllMessages(consumerB, topicB, mapB);
    await consumerB.disconnect();

    printComparisonTable(mapA, mapB);
}

async function readAllMessages(
    consumer: Consumer,
    topic: string,
    map: Map<string, RecordDetails>
) {
    await consumer.subscribe({topic, fromBeginning: true});

    let receivedMessages = 0;
    const startTime = Date.now();

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({topic, partition, message}) => {
            const value = message.value?.toString() || '';
            map.set(value, {
                topic,
                partition,
                offset: message.offset,
                value
            });
            receivedMessages++;
        }
    });

    // Wait for messages or timeout after 15 seconds
    while (receivedMessages < 10 && Date.now() - startTime < 15000) {
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    await consumer.stop();
}

function printComparisonTable(mapA: Map<string, RecordDetails>, mapB: Map<string, RecordDetails>) {
    const widths = [25, 14, 20, 16, 16, 20, 16, 5];
    const headers = [
        "Message value",
        "ClusterA topic",
        "ClusterA partition",
        "ClusterA offset",
        "ClusterB topic",
        "ClusterB partition",
        "ClusterB offset",
        "Match"
    ];

    console.log(formatRow(headers, widths));
    console.log('-'.repeat(widths.reduce((sum, width) => sum + width + 3, -3)));

    // Get messages only from Cluster A (source)
    const sourceMessages = Array.from(mapA.keys());

    for (const key of sourceMessages) {
        const recordA = mapA.get(key);
        const recordB = mapB.get(key);

        const fields = [
            key.length > 25 ? key.substring(0, 22) + '...' : key,
            recordA?.topic || '-',
            recordA?.partition?.toString() || '-',
            recordA?.offset || '-',
            recordB?.topic || '-',
            recordB?.partition?.toString() || '-',
            recordB?.offset || '-',
            shouldMatch(recordA, recordB) ? '✓' : '✗'
        ];

        console.log(formatRow(fields, widths));
    }
}

function formatRow(fields: string[], widths: number[]): string {
    return fields
        .map((field, i) => field.padEnd(widths[i]).substring(0, widths[i]))
        .join(' | ');
}

function shouldMatch(a?: RecordDetails, b?: RecordDetails): boolean {
    // Only match based on message value
    return !!a && !!b && a.value === b.value;
}

main().catch(err => {
    console.error('Error in main:', err);
    process.exit(1);
});