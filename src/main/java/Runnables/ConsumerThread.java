package Runnables;

import Serializer.JsonEncoder;
import consensus.AtomicConsensus;
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

    private Duration pollDuriation = Duration.ofSeconds(3);

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

    public ConsumerThread(ProducerContainer producer, String topic, String groupID) {
        this.producer = producer;
        this.topic.add(topic);
        this.groupID = groupID;
        createConsumer();
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
                /*System.out.println("Read Record Key " + record.key());
                System.out.println("Read Record value " + record.value());
                System.out.println("Read Record partition " + record.partition());
                System.out.println("Read Record offset " + record.offset());
                System.out.println("Read Record headers " + record.headers());
                System.out.println("Read Record TimeStamp " + record.timestamp());
                System.out.println("Read Record TimeStampType " + record.timestampType());*/
                if (record.value() != null) {
                    KafkaMessage msg = json.decode(record.value());
                    System.out.println("We got message " + msg);
                    switch (msg.getMessageType()) {
                        case ClientMessage: // Phase one, send out and receive notifications of msgs
                            KafkaMessage toSend = AtomicConsensus.getInstance().phaseOne(msg);
                            ProducerContainer.getInstance().sendMessage(toSend, toSend.getTopic());
                            break;
                        case NotifyMessage: //Phase one, receive Notify messages.
                            AtomicConsensus.getInstance().phaseOne(msg);
                            break;
                        case AckMessage: // Phase 2, received all Notification msgs send out Accept.
                            AtomicConsensus.getInstance().phaseTwo(msg);
                            break;
                        case Decided: // Phase 3, Received all ACK messages needed, start delivery.
                            AtomicConsensus.getInstance().phaseThree(msg);
                            break;
                        case UniqueAckMessage: // Accept we are the only receiver. Go direct to Delivery
                            AtomicConsensus.getInstance().phaseFour(msg);
                            break;
                        case Delivery: // Phase 4, Deliver msg to the topics.
                            // (maybe not needed, need to read commit the delivered message though)
                            AtomicConsensus.getInstance().phaseFour(msg);
                            break;
                        case NackMessage: // Node disagree with decision, restart.
                            // (not needed for now)
                            break;
                        case DupMessage: // Already got this message (used if a Node tries to take responsibility over another topic)
                            // (not needed for now)
                            break;
                        default:
                            System.out.println("Something wrong happend in ConsumerThread Switch");

                    }

                } else {
                    System.out.println("No message in value");
                }

            });


            // commits the offset of record to broker.
            // We should only do this after a delivery. Commit "eats" up the messages.
            // It is possible to get the messages back, but is there any good reason to commit the message
            //  - Only reason I can think of is if we handle multiple messages at once.
            consumer.commitAsync();


            // Consensus algorithm here.

            // Receive message from consensus here.
            /*String[] topics = this.topic.stream().toArray(String[]::new);
            KafkaMessage msg = new KafkaMessage(1, this.producer.getID(), Type.AckMessage, "Responding",
                    topics);*/

            // Send Messages from consensus algorithm to the respective topics.
            /*if (this.topic.contains("T1")) {
                this.producer.sendMessage(msg, "T2");
            } else {
                this.producer.sendMessage(msg, "T1");
            }*/

        }

        System.out.println("Polls without messages: " + noMessageFound);

        // Shutting down the atomic Node.
        this.producer.stop();
        consumer.close();
    }
}
