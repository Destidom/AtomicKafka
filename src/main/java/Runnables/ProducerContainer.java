package Runnables;

import Serializer.JsonEncoder;
import com.codahale.metrics.Meter;
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
public class ProducerContainer implements IProducer {

    private org.apache.kafka.clients.producer.Producer<Long, String> producer = null;
    private int ID = -1;
    private String transactionalID = "default" + this.ID + Integer.toString(new Random().nextInt()); // TODO: fix

    private Meter outgoing = null;

    public static ProducerContainer instance;
    private JsonEncoder json = new JsonEncoder();

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
        createProducer(null, "");
       // this.producer.initTransactions(); //initiate transactions
        instance = this;
    }

    public ProducerContainer(int clientID, String transactionID, Meter outgoing, String brokers) {
        this.ID = clientID;
        this.transactionalID = transactionID;
        createProducer(null, brokers);
       // this.producer.initTransactions(); //initiate transactions
        instance = this;
        this.outgoing = outgoing;
    }

    public ProducerContainer(int clientID, String transactionID, Properties props, Meter outgoing, String brokers ) {
        this.ID = clientID;
        this.transactionalID = transactionID;
        createProducer(props, brokers);
       // this.producer.initTransactions(); //initiate transactions
        instance = this;
        this.outgoing = outgoing;
    }


    private void createProducer(Properties props, String brokers) {
        if (props == null) {
            props = new Properties();
        }
        String tmpBroker = Constants.KAFKA_BROKERS;
        if(!brokers.isEmpty()){
            tmpBroker = brokers;
        }

        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, tmpBroker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Integer.toString(this.ID));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // enable idempotence
       // props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.transactionalID); // set transaction id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        this.producer = new KafkaProducer<>(props);
    }


    @Override
    public void sendMessage(KafkaMessage msg, String topic) {

        if (producer != null) {
            // Encode message

            // producer.beginTransaction(); //begin transactions
          //  producer.beginTransaction();
            String encodedMsg = json.encode(msg);
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic,
                    encodedMsg);
            try {

                producer.send(record).get();
                this.outgoing.mark();
                // producer.commitTransaction();
            } catch (KafkaException e) {
                //producer.abortTransaction();
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("ProducerContainer is not initialized for producer " + this.ID);
        }



    }




    @Override
    public void stop() {
        this.producer.close();
    }

    public int getID() {
        return ID;
    }

}
