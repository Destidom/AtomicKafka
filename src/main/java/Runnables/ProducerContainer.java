package Runnables;

import Serializer.JsonEncoder;
import constants.Constants;
import model.KafkaMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;


/**
 * This class is a "singleton" container for a producer. should be possible to use by multiple threads as it is
 * Synchronized. Currently just managed by one thread.
 * <p>
 * "Note that transactions requires a cluster of at least three brokers by default what is the recommended setting
 * for production; for development you can change this, by adjusting broker setting
 * `transaction.state.log.replication.factor`.";
 */
public class ProducerContainer {

    private org.apache.kafka.clients.producer.Producer<Long, String> producer = null;
    private int ID = -1;
    private String transactionalID = "default" + Integer.toString(new Random().nextInt());

    public static ProducerContainer instance;

    public static ProducerContainer getInstance() {
        if (instance == null) {
            synchronized (ProducerContainer.class) {
                if (instance == null) {
                    instance = new ProducerContainer();
                }
            }
        }
        return instance;
    }

    public ProducerContainer() {
        createProducer();
        this.producer.initTransactions(); //initiate transactions
        instance = this;
    }

    public ProducerContainer(int clientID, String transactionID) {
        this.ID = clientID;
        this.transactionalID = transactionID;
        createProducer();
        this.producer.initTransactions(); //initiate transactions
        instance = this;
    }


    private void createProducer() {
        Properties props = new Properties();
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Integer.toString(this.ID));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // enable idempotence
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.transactionalID); // set transaction id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        this.producer = new KafkaProducer<>(props);
    }


    public synchronized void sendMessage(KafkaMessage msg, String topic) {
        if (producer != null) {
            // Encode message


            JsonEncoder json = new JsonEncoder();
            producer.beginTransaction(); //begin transactions

            String encodedMsg = json.encode(msg);
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic,
                    encodedMsg);

            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Sending: Record sent with key " + 1 + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
                producer.commitTransaction();
            } catch (KafkaException e) {
                producer.abortTransaction();
                e.printStackTrace();
            } catch (ExecutionException e) {
                producer.abortTransaction();
                System.out.println("ExecutionException: Error in sending record");
                e.printStackTrace();
            } catch (InterruptedException e) {
                producer.abortTransaction();
                System.out.println("InterruptedException: Error in sending record");
                e.printStackTrace();
            }
        } else {
            System.out.println("ProducerContainer is not initialized for producer " + this.ID);
        }
    }


    public synchronized void sendMessage(KafkaMessage msg, List<String> topics) {

        String[] localTmp = topics.toArray(new String[topics.size()]);
        sendMessage(msg, localTmp);
    }

    /**
     * Send single KafkaMessage to multiple topics with transaction.
     *
     * @param msg   Message to send to different topics.
     * @param topic Topics to receive message.
     */
    public synchronized void sendMessage(KafkaMessage msg, String[] topic) {
        if (producer != null) {
            // Encode message
            JsonEncoder json = new JsonEncoder();


            producer.beginTransaction(); //begin transactions
            try {
                for (int index = 0; index < topic.length; index++) {
                    String encodedMsg = json.encode(msg);
                    ProducerRecord<Long, String> record = new ProducerRecord<>(topic[index],
                            encodedMsg);

                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println("Sending: Record sent with key " + index + " to partition " + metadata.partition()
                            + " with offset " + metadata.offset());

                }
                producer.commitTransaction();
            } catch (KafkaException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                producer.abortTransaction();
                System.out.println("ExecutionException: Error in sending record");
                e.printStackTrace();
            } catch (InterruptedException e) {
                producer.abortTransaction();
                System.out.println("InterruptedException: Error in sending record");
                e.printStackTrace();
            }
        } else {
            System.out.println("ProducerContainer is not initialized for producer " + this.ID);
        }
    }


    /*
     * Multiple messages to single Topic
     *
     * @param msg
     * @param topic
     */
    /*public synchronized void sendMessage(KafkaMessage[] msg, String topic) {
        if (producer != null) {
            // Encode message
            JsonEncoder json = new JsonEncoder();


            producer.beginTransaction(); //begin transactions
            try {
                for (int index = 0; index < msg.length; index++) {
                    String encodedMsg = json.encode(msg[index]);
                    ProducerRecord<Long, String> record = new ProducerRecord<>(topic,
                            encodedMsg);

                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println("Sending: Record sent with key " + index + " to partition " + metadata.partition()
                            + " with offset " + metadata.offset());

                }
                producer.commitTransaction();
            } catch (KafkaException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                producer.abortTransaction();
                System.out.println("ExecutionException: Error in sending record");
                e.printStackTrace();
            } catch (InterruptedException e) {
                producer.abortTransaction();
                System.out.println("InterruptedException: Error in sending record");
                e.printStackTrace();
            }
        } else {
            System.out.println("ProducerContainer is not initialized for producer " + this.ID);
        }
    }*/
    public void stop() {
        this.producer.close();
    }

    public int getID() {
        return ID;
    }

}
