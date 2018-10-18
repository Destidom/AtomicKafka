package consensus;

import model.KafkaMessage;
import model.Type;
import org.apache.commons.lang3.SerializationUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will decided which message type to send to the other topics.
 * The consensus will be based on latest TS of a ClientMessage or NodeNotify Message.
 * Will keep the state of a specific message strain.
 */
public class AtomicConsensus {

    // Map is as follows
    // (Integer, HashMap)      MessageID -> MAP.
    // (Integer, KafkaMessage) ClientID -> KafkaMessage.
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, KafkaMessage>> state = new ConcurrentHashMap<>();

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

        // Two possible message types, ClientMessage (as the first message we received, or notify message
        // Both messages means the same, but comes from different origins.
        // And if a Node has not received a clientmessage, it will receive a notify message and treat it the same way.

        // Dont care about numbers of messages here, just build the array and store messages.

        System.out.println("In phase one with " + msg.toString());

        // If only one topic, skip phases and storing message and send it directly to kafka.
        if (msg.getTopic().length == 1) {
            // Send message directly to phase 4.
            msg.setMessageType(Type.Decided);
            return msg;
        }
        ConcurrentHashMap<Integer, KafkaMessage> list = state.get(msg.getMessageID());
        if (list == null) {
            System.out.println("Init list");
            list = new ConcurrentHashMap<>();
            list.put(msg.getClientID(), msg);
            state.put(msg.getMessageID(), list);
        } else {
            System.out.println("adding to list");
            state.get(msg.getMessageID()).put(msg.getClientID(), msg);
        }


        // Create response message.
        KafkaMessage cloned = SerializationUtils.clone(msg);
        cloned.setMessageType(Type.AckMessage);

        // Test
        /*synchronized (state) {
            state.get(cloned.getMessageID()).forEach(e -> System.out.println("test " + e.toString()));
            // Add message to state array
        }*/

        return cloned;
    }

    public KafkaMessage phaseTwo(KafkaMessage msg) {
        // Check if message has achieved phase2, meaning every node has received the msg.
        // If we enter phase 2, submit a new message with our timestamp and offset
        System.out.println("In phase two with " + msg.toString());
        ConcurrentHashMap<Integer, KafkaMessage> messageMap = this.state.get(msg.getMessageID());
        KafkaMessage storedMessage = messageMap.get(msg.getClientID());

        // Does not exists, send out NACK.
        if (storedMessage == null) {
            // Create NACK to restart process?
            // Should not be possible to get here, but still important to test for it
        }

        // Check if we have the Client/Notify message, if so update stored msg to ACK state.
        if (storedMessage.getClientID() == msg.getClientID() && storedMessage.getMessageID() == msg.getMessageID()) {
            storedMessage.setMessageType(Type.AckMessage);
        }

        // Check if we have enough ACKS to step into next phase.
        boolean allAcks = true;
        int counter = 0;
        for (Map.Entry<Integer, KafkaMessage> entry : this.state.get(msg.getMessageID()).entrySet()) {
            if (entry.getValue().getMessageType() != Type.AckMessage) {
                allAcks = false;
                ++counter;
            }
        }

        // TODO: Improve this section to handle the specific topics and number of acks.
        if (allAcks && counter == messageMap.mappingCount() && counter == storedMessage.getTopic().length) {
            // Check if we have enough acks.
            System.out.println("We got all acks now!");
        } else {
            // wait for retrieving all acks.
            System.out.println("We await for    all acks now!");
        }


        return null;
    }

    public KafkaMessage phaseThree(KafkaMessage msg) {
        // Check if we have achieved phase3, meaning every node has recieved the TS and offset.
        // Send a decision message to every node.
        System.out.println("In phase three with " + msg.toString());
        return null;
    }

    public KafkaMessage phaseFour(KafkaMessage msg) {
        // If decision node is received from everybody. Deliver it to the deliver topic(?)
        // Delete it from hashmap.
        System.out.println("In phase four with " + msg.toString());
        return null;
    }

    public void removeMessage(KafkaMessage msg) {
        this.state.remove(msg.getMessageID());
    }

}
