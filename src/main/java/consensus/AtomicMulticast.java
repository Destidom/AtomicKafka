package consensus;

import model.KafkaMessage;
import model.Type;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will decided which message type to send to the other topics.
 * The consensus will be based on latest TS of a ClientMessage or NodeNotify Message.
 * Will keep the state of a specific message strain.
 */
public class AtomicMulticast implements Atomic {

    // Map is as follows
    // (Integer, HashMap)      MessageID -> MAP.
    // (Integer, KafkaMessage) ClientID -> KafkaMessage.
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, KafkaMessage>> state = new ConcurrentHashMap<>();

    public static AtomicMulticast instance;

    public static AtomicMulticast getInstance() {
        if (instance == null) {
            synchronized (AtomicMulticast.class) {
                if (instance == null) {
                    instance = new AtomicMulticast();
                }
            }
        }
        return instance;
    }

    private AtomicMulticast() {

    }

    /*
    If a message is designated to a topic that does not exist, what to do then, we would never get a Ack message
    from that topic. Do we timeout after a while and remove it?
        - Need to notify user somehow? Process this as a message in deliver queue as failed message?
        - What if messageID is not unique ?
     */

    /**
     * Phase One is the content based multicast phase.
     * Ensuring all intended Topic receive the Message.
     *
     * @param msg
     * @return
     */
    @Override
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
            msg.setMessageType(Type.UniqueAckMessage);
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


        // Create response message. (Notify to ourselves, TODO: Remove self sending to notify)
        KafkaMessage cloned = SerializationUtils.clone(msg);

        if (msg.getMessageType() == Type.NotifyMessage) {
            // Respond to other nodes that we have gotten the message from client.
            cloned.setMessageType(Type.AckMessage);
        } else {
            // Notify others that there are a new message TODO: Redo logic to be reactive instead of proactive.
            cloned.setMessageType(Type.NotifyMessage);
        }

        return cloned;
    }

    // Wait for a last ack, if no ack received send notify.
    // If no received ack from that notify assume responsibility of the topic (?)
    @Override
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

        Set<Integer> haveTopicResponses = new HashSet<>();
        for (Map.Entry<Integer, KafkaMessage> entry : this.state.get(msg.getMessageID()).entrySet()) {
            if (entry.getValue().getMessageType() != Type.AckMessage) {
                allAcks = false;
            } else {
                // Store unique senders.
                // TODO: Create correlation between SenderID and Topic somehow.
                haveTopicResponses.add(entry.getValue().getSenderID());
            }
        }

        // TODO: Improve this section to handle the specific topics and number of acks.
        // TODO: Find out which offset/timestamp is the latest and send it to others.
        if (allAcks && haveTopicResponses.size() == storedMessage.getTopic().length) {
            // Check if we have enough Acks.
            System.out.println("We got all acks now!");
            // Create response message. (Notify to ourselves, TODO: Remove self sending to notify)
            KafkaMessage cloned = SerializationUtils.clone(msg);
            cloned.setMessageType(Type.Decided);
            return cloned;
        } else {
            // wait for retrieving all Acks.
            System.out.println("We await for all acks now!");
        }


        return null;
    }

    @Override
    public KafkaMessage phaseThree(KafkaMessage msg) {
        // Check if we have achieved phase3, meaning every node has recieved the TS and offset.
        // Send a decision message to every node.
        System.out.println("In phase three with " + msg.toString());
        // TODO: Decide on a message and send decided msg out.
        // TODO: Based on Timestamp, offset, they "should" never end up being the same as another message
        return null;
    }

    @Override
    public KafkaMessage phaseFour(KafkaMessage msg) {
        // If decision node is received from everybody. Deliver it to the deliver topic(?)
        // Delete it from HashMap.
        System.out.println("In phase four with " + msg.toString());
        // TODO: Find out how to deliver, same Topic or a dedicated Topic for delivered messages?
        // TODO: Pros with dedicated: less scann of topic to find delivered messages, easier to test.
        // TODO: Con with dedicated: requires more of a client.
        return null;
    }

    public void removeMessage(KafkaMessage msg) {
        this.state.remove(msg.getMessageID());
    }

}
