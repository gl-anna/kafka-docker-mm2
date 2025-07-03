import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class TestOffsetMm2 {
    static class RecordDetails {
        String topic;
        int partition;
        long offset;
        String value;

        RecordDetails(String topic, int partition, long offset, String value) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.value = value;
        }
    }

    public static void main(String[] args) {
        // Cluster A config
        Properties clusterAprops = new Properties();
        clusterAprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        clusterAprops.put(ConsumerConfig.GROUP_ID_CONFIG, "compare_group_A");
        clusterAprops.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_A");
        clusterAprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clusterAprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clusterAprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        clusterAprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Cluster B config
        Properties clusterBprops = new Properties();
        clusterBprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8092");
        clusterBprops.put(ConsumerConfig.GROUP_ID_CONFIG, "compare_group_B");
        clusterBprops.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_B");
        clusterBprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clusterBprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clusterBprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        clusterBprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topicA = "topic1";
        String topicB = "clusterA.topic1";

        Map<String, RecordDetails> mapA = new HashMap<>();
        Map<String, RecordDetails> mapB = new HashMap<>();

        // Read Cluster A
        try (Consumer<String, String> consumerA = new KafkaConsumer<>(clusterAprops)) {
            List<PartitionInfo> partitionsA = consumerA.partitionsFor(topicA);
            List<TopicPartition> topicPartitionsA = new ArrayList<>();
            for (PartitionInfo p : partitionsA) {
                topicPartitionsA.add(new TopicPartition(topicA, p.partition()));
            }

            consumerA.assign(topicPartitionsA);
            consumerA.seekToBeginning(topicPartitionsA);

            while (true) {
                ConsumerRecords<String, String> records = consumerA.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    mapA.put(record.value(), new RecordDetails(
                            record.topic(), record.partition(), record.offset(), record.value()));
                }
            }
        }

        // Read Cluster B
        try (Consumer<String, String> consumerB = new KafkaConsumer<>(clusterBprops)) {
            List<PartitionInfo> partitionsB = consumerB.partitionsFor(topicB);
            List<TopicPartition> topicPartitionsB = new ArrayList<>();
            for (PartitionInfo p : partitionsB) {
                topicPartitionsB.add(new TopicPartition(topicB, p.partition()));
            }

            consumerB.assign(topicPartitionsB);
            consumerB.seekToBeginning(topicPartitionsB);

            while (true) {
                ConsumerRecords<String, String> records = consumerB.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    mapB.put(record.value(), new RecordDetails(
                            record.topic(), record.partition(), record.offset(), record.value()));
                }
            }
        }

        // Print table header
        System.out.println(String.format(
                "%-15s | %-14s | %-20s | %-16s | %-14s | %-20s | %-14s | %-5s",
                "Message value", "ClusterA topic", "ClusterA partition", "ClusterA offset",
                "ClusterB topic", "ClusterB partition", "ClusterB offset", "Match"));
        System.out.println(new String(new char[150]).replace("\0", "-"));

        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(mapA.keySet());
        allKeys.addAll(mapB.keySet());

        for (String key : allKeys) {
            RecordDetails recordA = mapA.get(key);
            RecordDetails recordB = mapB.get(key);

            String messageValue = recordA != null ? recordA.value : recordB != null ? recordB.value : "null";

            String topicAout = recordA != null ? recordA.topic : "-";
            String partAout = recordA != null ? String.valueOf(recordA.partition) : "-";
            String offsetAout = recordA != null ? String.valueOf(recordA.offset) : "-";

            String topicBout = recordB != null ? recordB.topic : "-";
            String partBout = recordB != null ? String.valueOf(recordB.partition) : "-";
            String offsetBout = recordB != null ? String.valueOf(recordB.offset) : "-";

            String match;
            if (recordA != null && recordB != null &&
                recordA.partition == recordB.partition &&
                recordA.offset == recordB.offset) {
                match = "✓";
            } else {
                match = "✗";
            }

            System.out.println(String.format(
                    "%-15s | %-14s | %-20s | %-16s | %-14s | %-20s | %-14s | %-5s",
                    messageValue, topicAout, partAout, offsetAout,
                    topicBout, partBout, offsetBout, match));
        }
    }
}
