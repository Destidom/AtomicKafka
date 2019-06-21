package Runnables;

import Serializer.JsonEncoder;
import com.codahale.metrics.Meter;
import constants.Constants;
import model.KafkaMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Over-engineered class...!
 */
public class ConsumerThread implements Runnable {

    private Consumer<Long, String> consumer = null;
    private List<String> topic = new ArrayList<>();
    private boolean running = true;
    private ProducerContainer producer = null;
    private String groupID = "";
    public static int CLIENT_ID = -1;
    public static int MESSAGES_DeLIVERED = 0;
    public int numberOfMessages = 0;
    private Duration pollDuriation = Duration.ofSeconds(10);
    private long endTime = 0L;

    public long getEndTime() {
        return this.endTime;
    }

    private Meter incoming = null;

    public ConsumerThread() {
        this.topic.add("default");
        createConsumer();
    }

    public ConsumerThread(List<String> topic) {
        this.topic = topic;
        createConsumer();
    }

    public ConsumerThread(ProducerContainer producer, List<String> topic, String groupID) {
        this.producer = producer;
        this.topic = topic;
        createConsumer();
    }

    public ConsumerThread(ProducerContainer producer, String topic, String groupID, Integer clientID, int numberOfMessages, Meter incoming) {
        this.producer = producer;
        this.topic.add(topic);
        this.groupID = groupID;
        this.CLIENT_ID = clientID;
        this.numberOfMessages = numberOfMessages;
        createConsumer();
        this.incoming = incoming;
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
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
        int noMessageFound = 0;
        JsonEncoder json = new JsonEncoder();
        while (running) {

            ConsumerRecords<Long, String> consumerRecords = consumer.poll(pollDuriation);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                continue;
            }

            //print each record.
            consumerRecords.forEach(record -> {
                if (record.value() != null) {
                    KafkaMessage msg = json.decode(record.value());
                    if (msg != null) { // set offset and timestamp for first time message.
                        KafkaMessage toSend = null;
                        switch (msg.getMessageType()) {
                            case Delivery: // Phase 4, Deliver msg to the topics.
                                this.incoming.mark();
                                break;
                            default:
                                break;
                        }
                    }

                }

            });


        }

        System.out.println("Successfully delivered # " + this.MESSAGES_DeLIVERED + " time " + this.endTime);

        // Shutting down the atomic Node.
        consumer.close();
    }
}

 /*System.out.println("Read Record Key " + record.key());
                System.out.println("Read Record value " + record.value());
                System.out.println("Read Record partition " + record.partition());
                System.out.println("Read Record offset " + record.offset());
                System.out.println("Read Record headers " + record.headers());
                System.out.println("Read Record TimeStamp " + record.timestamp());
                System.out.println("Read Record TimeStampType " + record.timestampType());*/