package consensus;

import Runnables.ConsumerThread;
import model.KafkaMessage;
import model.Type;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import  java.util.PriorityQueue;

// TODO: Verification of delivered messages?
// TODO: Find out how to deliver, same Topic or a dedicated Topic for delivered messages?
// TODO: Pros with dedicated: less scann of topic to find delivered messages, easier to test.
// TODO: Con with dedicated: requires more of a client.
// TODO: Read up on Kafka mock library.
// TODO: implement round and vround, to decline messageIDs lower than a certain value for phase I ?
// Since ClientMessage and notify message is the entry point of the system.

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

    private final PriorityQueue<KafkaMessage> deliveryHeap = new PriorityQueue<>();


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


        // If only one topic, skip phases and storing message and send it directly to kafka.
        if (msg.getTopic().length == 1) {
            //System.out.println("Unique msg " + msg.toString());
            // Send message directly to phase 4.
            msg.setMessageType(Type.Delivery);
            return msg;
        }


        ConcurrentHashMap<Integer, KafkaMessage> list = state.get(msg.getMessageID());
        if (list == null) {
            list = new ConcurrentHashMap<>();
            msg.setSenderID(ConsumerThread.CLIENT_ID); // Set node as sender.
            list.put(msg.getSenderID(), msg);
            state.put(msg.getMessageID(), list);
        } else {
            state.get(msg.getMessageID()).put(msg.getSenderID(), msg);
        }


        // Create response message.
        KafkaMessage cloned = SerializationUtils.clone(msg);
        cloned.setSenderID(ConsumerThread.CLIENT_ID); // Set ourselves as sender.
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
        ConcurrentHashMap<Integer, KafkaMessage> messageMap = this.state.get(msg.getMessageID());
        if (messageMap == null) { // TODO: Temp fix
            messageMap = new ConcurrentHashMap<>();
            msg.setSenderID(ConsumerThread.CLIENT_ID); // Set node as sender.
            messageMap.put(msg.getSenderID(), msg);
            state.put(msg.getMessageID(), messageMap);
        }
        KafkaMessage storedMessage = null;
        if (msg.getMessageType() == Type.AckMessage) {
            messageMap.put(msg.getSenderID(), msg);
            storedMessage = msg;
        }


        // Check if we have the Client/Notify message, if so update stored msg to ACK state.
        if (storedMessage.getSenderID() == msg.getSenderID() && storedMessage.getMessageID() == msg.getMessageID()) {
            storedMessage.setMessageType(Type.AckMessage);
        }

        // Check if we have enough ACKS to step into next phase.
        boolean allAcks = true;

        Set<Integer> haveTopicResponses = new HashSet<>();
        for (Map.Entry<Integer, KafkaMessage> entry : this.state.get(msg.getMessageID()).entrySet()) {
            // A entry can be updated to decided if our queue is slow.
            if (entry.getValue().getMessageType() != Type.AckMessage) {
                allAcks = false;
            } else {
                // Store unique senders.
                // TODO: Create correlation between SenderID and Topic somehow.
                // TODO: will not work if one node takes responsability over multiple topics...
                haveTopicResponses.add(entry.getValue().getSenderID());
            }

            if (allAcks == false && entry.getValue().getMessageType() == Type.Decided) {
                allAcks = true;
                haveTopicResponses.add(entry.getValue().getSenderID());
            }
            // Store unique senders.
            // TODO: Create correlation between SenderID and Topic somehow.
            // TODO: will not work if one node takes responsability over multiple topics...


        }

        // TODO: Improve this section to handle the specific topics and number of acks.
        if (allAcks && haveTopicResponses.size() == storedMessage.getTopic().length) {
            // Check if we have enough Acks.
            // TODO: When all ACKS: Find out which offset/timestamp is the latest and send it to others.
            Iterator it = messageMap.entrySet().iterator();
            KafkaMessage decidedMessage = null;
            long lastestOffset = -999L;
            long lastestTimeStamp = -999L;
            while (it.hasNext()) {
                Map.Entry<Integer, KafkaMessage> pair = (Map.Entry) it.next();
                KafkaMessage tmpMsg = pair.getValue();
                // TODO: WE HAVE TO CHECK AGAINST OTHER MESSAGES WITH DIFFERENT MSG-ID
                if (lastestOffset < tmpMsg.getOffset() && lastestTimeStamp < tmpMsg.getTimeStamp()) { // TODO: Fix this..
                    lastestOffset = tmpMsg.getOffset();
                    lastestTimeStamp = tmpMsg.getTimeStamp();
                    decidedMessage = tmpMsg;
                }
            }

            // Create response message. (Notify to ourselves)
            KafkaMessage cloned = SerializationUtils.clone(decidedMessage);
            cloned.setSenderID(ConsumerThread.CLIENT_ID);
            cloned.setMessageType(Type.Delivery);
            return cloned;
        } /*else {
            // wait for retrieving all Acks.
            System.out.println("We await for ACKS now!");
        }*/


        return null;
    }

    @Override
    public KafkaMessage phaseThree(KafkaMessage msg) {
        // Check if we have achieved phase3, meaning every node has recieved the TS and offset.
        // Send a decision message to every node.
        if (this.state == null) {
            System.out.println("State is null");
        }

        if (msg == null) {
            System.out.println("MSG is null");
        }

        ConcurrentHashMap<Integer, KafkaMessage> messageMap = this.state.get(msg.getMessageID());
        if (msg.getMessageType() == Type.Decided) {
            messageMap.put(msg.getSenderID(), msg);
        }

        // TODO: Go through all deliverable messages and deliver by offsets. from earliest to latest.
        boolean allDecided = true;
        long matchOffset = msg.getOffset();
        long matchTimeStamp = msg.getTimeStamp();
        Set<Integer> haveTopicResponses = new HashSet<>();
        for (Map.Entry<Integer, KafkaMessage> entry : this.state.get(msg.getMessageID()).entrySet()) {
            if (entry.getValue().getMessageType() != Type.Decided
                    && entry.getValue().getTimeStamp() != matchTimeStamp
                    && entry.getValue().getOffset() != matchOffset) {
                allDecided = false;
            } else {
                // Store unique senders.
                // TODO: Create correlation between SenderID and Topic somehow.?
                // TODO: will not work if one node takes responsability over multiple topics...
                haveTopicResponses.add(entry.getValue().getSenderID());
            }
        }
        // TODO: If everyone has decided on message and all have the same offset and timestamp, do delivery
        if (allDecided && haveTopicResponses.size() == msg.getTopic().length) {
            KafkaMessage cloned = SerializationUtils.clone(msg);
            cloned.setSenderID(ConsumerThread.CLIENT_ID);
            cloned.setMessageType(Type.Delivery);
            // TODO: When delivered, clear out the delivered messages from state.
            //this.state.remove(msg.getMessageID());
            return cloned;
        } else {
            //System.out.println("Waiting for decided values!");
        }

        return null;
    }

    @Override
    public KafkaMessage phaseFour(KafkaMessage msg) {
        //System.out.println("In phase four with " + msg.toString());
        KafkaMessage cloned = SerializationUtils.clone(msg);
        cloned.setSenderID(ConsumerThread.CLIENT_ID);
        cloned.setMessageType(Type.Delivery);

        return cloned;
    }

    public void removeMessage(KafkaMessage msg) {
        this.state.remove(msg.getMessageID());
    }

}
