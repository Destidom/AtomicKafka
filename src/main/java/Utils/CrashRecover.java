package Utils;

import Serializer.JsonEncoder;
import consensus.AtomicMulticast;
import constants.Constants;
import kafka.common.OffsetMetadata;
import model.KafkaMessage;
import model.Type;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Over-engineered class...!
 */
public class CrashRecover implements Runnable {

    private Consumer<Long, String> consumer = null;
    private List<String> topic = new ArrayList<>();
    private boolean running = true;
    private String groupID = "";
    public static int CLIENT_ID = -1;

    private Duration pollDuriation = Duration.ofSeconds(10);

    public CrashRecover() {
        this.topic.add("default");
        createConsumer();
    }

    public CrashRecover(String topic, String groupID, Integer clientID) {
        this.topic.add(topic);
        this.groupID = groupID;
        this.CLIENT_ID = clientID;
        createConsumer();
    }


    /**
     * Dependency Injection for testing later on.
     *
     * @param consumer
     */
    public void setConsumer(Consumer<Long, String> consumer) {
        this.consumer = consumer;
    }

    public void createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Constants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET_RESET_EARLIER);
        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(this.topic);
        this.consumer = consumer;
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        JsonEncoder json = new JsonEncoder();
        TopicPartition partition = new TopicPartition(this.topic.get(0), 0);
        Map<Integer, List<Long>> messageIdMap = new HashMap<>();

        while (running) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));

            if (consumerRecords.count() == 0) {
                continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println(record);
                if (record.value() == null) {
                    System.out.println("Decoded msg is null ");
                    return;
                }
                KafkaMessage msg = json.decode(record.value());
                if (msg == null || msg.getMessageType() == null) {
                    System.out.println("No message in value");
                    return;
                }

                switch (msg.getMessageType()) {
                    case ClientMessage:
                        msg.setOffset(record.offset());
                        List<Long> offsetList = messageIdMap.computeIfAbsent(msg.getMessageID(), k -> new ArrayList<>());
                        offsetList.add(record.offset());
                        break;

                    case Delivery:
                        List<Long> msgOffsets = messageIdMap.get(msg.getMessageID());
                        if(msgOffsets != null) {
                            Long latestOffset =  Collections.max(msgOffsets);
                            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(latestOffset)));
                        }
                        break;
                    default:
                        System.out.println("Unknown message type!");
                }
            });
        }
        consumer.close();
    }
}
