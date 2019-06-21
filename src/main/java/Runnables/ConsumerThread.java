package Runnables;

import Serializer.JsonEncoder;
import com.codahale.metrics.Meter;
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
import java.util.Random;

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

    public AtomicMulticast consensus = null;
    private ProducerContainer prod = null;


    private Duration pollDuriation = Duration.ofSeconds(1);

    public ConsumerThread() {
        this.topic.add("default");
        createConsumer("");
    }
    private Meter requests = null;
    private Meter decided = null;

    public ConsumerThread(ProducerContainer producer, String topic, String groupID, Integer clientID, Meter meter, Meter decided, String brokers) {
        this.producer = producer;
        this.topic.add(topic);
        this.groupID = groupID;
        this.CLIENT_ID = clientID;
        this.requests = meter;
        this.decided = decided;
        createConsumer(brokers);
    }


    /**
     * Dependency Injection for testing later on.
     *
     * @param consumer
     */
    public void setConsumer(Consumer<Long, String> consumer) {
        this.consumer = consumer;
    }

    public void createConsumer(String broker) {
        Properties props = new Properties();
        String tmpBroker = Constants.KAFKA_BROKERS;

        if(!broker.isEmpty()){
            tmpBroker = broker;
        }

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tmpBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Constants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET_RESET_LATEST);
        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(this.topic);
        this.consumer = consumer;
    }

    public void stop() {
        this.running = false;
    }




    @Override
    public void run() {




        Random rand = new Random();
        JsonEncoder json = new JsonEncoder();
        prod = ProducerContainer.getInstance();
        consensus = AtomicMulticast.getInstance();
        consensus.setTopic(this.topic.get(0));
        consensus.setSenderID(this.CLIENT_ID);
        while (running) {

            ConsumerRecords<Long, String> consumerRecords = consumer.poll(pollDuriation);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                continue;
            }

            //print each record.
            consumerRecords.forEach(record -> {
                if (record.value() != null) {
                    requests.mark();
                    long count = requests.getCount();
                    KafkaMessage msg = json.decode(record.value());
                    if (msg != null) { // set offset and timestamp for first time message.
                        KafkaMessage toSend = null;

                        switch (msg.getMessageType()) {

                            case ClientMessage: // Phase one, send out and receive notifications of msgs
                                msg.setOffset(record.offset()); // Set offset for clientMessage since Phase I
                                toSend = consensus.ReceivedClientMessage(msg);
                                //System.out.println("GOT CLIENTMSG");
                                if (toSend != null) {
                                    // Sending out Notifies
                                    toSend.setSentFromTopic(this.topic.get(0));
                                    for(String topic : toSend.getTopic()) {
                                        if(!topic.equalsIgnoreCase(this.topic.get(0))) {
                                            //System.out.println("Sending Notify " + topic);
                                            prod.sendMessage(toSend, topic);
                                        }
                                    }
                                }
                                break;

                            case NotifyMessage: //Phase one, receive Notify messages.
                                msg.setOffset(record.offset());
                                //System.out.println("GOT NOTIFY");
                                toSend = consensus.ReceivedNotify(msg);
                                if (toSend != null) {
                                    // Sending out ACK
                                    prod.sendMessage(toSend, toSend.getSentFromTopic());
                                }

                                break;

                            case AckMessage: // Phase 2, received all ACKS msgs decide on a message.
                                //System.out.println("Got ACK");
                                toSend = consensus.ReceivedAcknowledge(msg); // no return, ignore it.
                                if (toSend != null) {

                                    for(String topic : toSend.getTopic()) {
                                        if(!topic.equalsIgnoreCase(this.topic.get(0))) {
                                            //System.out.println("Sending Decided " + topic + " " + toSend.toString());
                                            prod.sendMessage(toSend, topic);
                                        }
                                    }
                                }
                                break;
                            case Decided:
                                // Not sending anything back
                                consensus.Decided(msg);
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

                // commits the offset of record to broker.

            });
            consumer.commitAsync();
            checkDelivery();



        }
        this.producer.stop();
        consumer.close();
    }


    public void checkDelivery() {
        List<KafkaMessage> delivery  = consensus.checkDelivery();
        if( delivery.size() > 0){
            for(int i =0; i < delivery.size(); i++) {
                //System.out.println("There are deliverable messages!");
                prod.sendMessage(delivery.get(i), this.topic.get(0));
                this.decided.mark();
            }
        }
    }
}