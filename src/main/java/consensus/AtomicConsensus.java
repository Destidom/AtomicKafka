package consensus;

import model.KafkaMessage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will decided which message type to send to the other topics.
 * The consensus will be based on latest TS of a ClientMessage or NodeNotify Message.
 * Will keep the state of a specific message strain.
 */
public class AtomicConsensus {

    private ConcurrentHashMap<Integer, KafkaMessage[]> state = new ConcurrentHashMap<>();

    public static AtomicConsensus instance;

    public static AtomicConsensus getInstance() {
        if (instance == null) {
            synchronized (AtomicConsensus.class) {
                if (instance == null) {
                    instance = new AtomicConsensus();
                }
            }
        }
        return instance;
    }

    private AtomicConsensus() {

    }

    /*
    If a message is designated to a topic that does not exist, what to do then, we would never get a Ack message
    from that topic. Do we timeout after a while and remove it?
        - Need to notify user somehow? Process this as a message in deliver queue as failed message?
        - What if messageID is not unique ?
     */

    public KafkaMessage phaseOne(KafkaMessage msg) {
        // Check message is designated to only one topic or more, if more notify others.
        // Send Unique Ack if only to us to log delivery.
        return null;
    }

    public KafkaMessage phaseTwo(KafkaMessage msg) {
        // Check if message has achieved phase2, meaning every node has received the msg.
        // If we enter phase 2, submit a new message with our timestamp and offset
        return null;
    }

    public KafkaMessage phaseThree(KafkaMessage msg) {
        // Check if we have achieved phase3, meaning every node has recieved the TS and offset.
        // Send a decision message to every node.
        return null;
    }

    public KafkaMessage phaseFour(KafkaMessage msg) {
        // If decision node is received from everybody. Deliver it to the deliver topic(?)
        // Delete it from hashmap.
        return null;
    }

    public void removeMessage(KafkaMessage msg) {
        this.state.remove(msg.getMessageID());
    }

}
