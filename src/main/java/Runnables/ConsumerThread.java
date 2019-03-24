package Runnables;

import Serializer.JsonEncoder;
import consensus.AtomicMulticast;
import constants.Constants;
import model.KafkaMessage;
import model.Type;
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

    private Duration pollDuriation = Duration.ofSeconds(10);

    public ConsumerThread() {
        this.topic.add("default");
        createConsumer();
    }

    public ConsumerThread(ProducerContainer producer, String topic, String groupID, Integer clientID) {
        this.producer = producer;
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
        ProducerContainer prod = ProducerContainer.getInstance();
        AtomicMulticast consensus = AtomicMulticast.getInstance();
        while (running) {

            ConsumerRecords<Long, String> consumerRecords = consumer.poll(pollDuriation);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                continue;
            }

            //print each record.
            consumerRecords.forEach(record -> {
                if (record.value() != null) {
                    KafkaMessage msg = json.decode(record.value());
                    if (msg != null) { // set offset and timestamp for first time message.
                        KafkaMessage toSend = null;
                        List<KafkaMessage> delivery = null;
                        switch (msg.getMessageType()) {

                            case ClientMessage: // Phase one, send out and receive notifications of msgs
                                msg.setOffset(record.offset()); // Set offset for clientMessage since Phase I
                                System.out.println("Got msg");
                                toSend = consensus.phaseOne(msg);
                                // TODO: fix this hack
                                toSend.setSentFromTopic(this.topic.get(0));
                                // Sending ack to ourselves.
                                if (toSend != null)
                                    prod.sendMessage(toSend, toSend.getTopic());
                                break;

                            case NotifyMessage: //Phase one, receive Notify messages.
                                msg.setOffset(record.offset());
                                toSend = consensus.phaseOne(msg);
                                System.out.println("Got notify");
                                if (toSend != null) {
                                    String toTopic = toSend.getSentFromTopic();
                                    prod.sendMessage(toSend, toTopic );
                                }
                                break;
                            case AckMessage: // Phase 2, received all ACKS msgs decide on a message.
                                System.out.println("Received ack");
                                KafkaMessage sendingMsg = consensus.phaseTwo(msg); // no return, ignore it.
                                if(sendingMsg != null) {
                                    sendingMsg.setMessageType(Type.Decided);
                                   System.out.println("aa " + sendingMsg);
                                    prod.sendMessage(sendingMsg, sendingMsg.getTopic());
                                }
                                break;
                            case Decided:
                                System.out.println("Got Delivery");
                                consensus.getDeliveryHeap().remove(msg);
                                msg.setMessageType(Type.Delivery);
                                consensus.getDeliveryHeap().add(msg);
                                delivery = consensus.checkDelivery();

                                // TODO: Send Decided MSG to every topic.
                                for(int i =0; i < delivery.size(); i++) {
                                    //System.out.println("There are deliverable messages!");
                                    prod.sendMessage(delivery.get(i), this.topic);
                                }
                                break;
                            case Delivery: // NOT IN USE!
                                break;
                            case UniqueAckMessage: // NOT IN USE
                                break;
                            case NackMessage: // NOT IN USE!
                                // (not needed for now)
                                break;
                            case DupMessage: // Already got this message (used if a Node tries to take responsibility over another topic)
                                // (not needed for now)
                                break;
                            default:
                                System.out.println("Something wrong happened in ConsumerThread Switch");

                        }
                    } else {
                        System.out.println("Decoded msg is null ");
                    }
                } else {
                    System.out.println("No message in value");
                }
            });


            // commits the offset of record to broker.
            consumer.commitAsync();

        }
        this.producer.stop();
        consumer.close();
    }
}