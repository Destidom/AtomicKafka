package consensus;

import Runnables.ConsumerThread;
import comparator.TimestampDecending;
import model.KafkaMessage;
import model.Type;
import org.apache.commons.lang3.SerializationUtils;
import comparator.TimestampAscending;

import java.util.*;
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

    private static final PriorityQueue<KafkaMessage> deliveryHeap = new PriorityQueue<>(new TimestampAscending());

    protected Long logicalClock = 0L;

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
            msg.setTimeStamp(logicalClock);
            logicalClock++;
            deliveryHeap.add(msg); // Delivery happens through deliveryHeap!
            return null;
        }

        // More than once receiver!
        // Update the timestamp!
        msg.setTimeStamp(logicalClock);
        logicalClock++;

        ConcurrentHashMap<Integer, KafkaMessage> list = state.get(msg.getMessageID());
        if (list == null) {
            list = new ConcurrentHashMap<>();
            msg.setSenderID(ConsumerThread.CLIENT_ID); // Set node as sender.
            list.put(msg.getSenderID(), msg);
            state.put(msg.getMessageID(), list);
        } else {
            state.get(msg.getMessageID()).put(msg.getSenderID(), msg);
            if(!this.deliveryHeap.contains(msg))
                this.deliveryHeap.add(msg);
        }

        // Create response message.
        KafkaMessage cloned = cloneMessage(msg);
        cloned.setSenderID(ConsumerThread.CLIENT_ID); // Set ourselves as sender.


        // TODO: does each node have to "notify" all other nodes if this is already done ?
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
        ConcurrentHashMap<Integer, KafkaMessage> messageMap = this.state.get(msg.getMessageID());

        if (messageMap == null) { // If we receive a ACK before Notify a list has to be created!

            messageMap = new ConcurrentHashMap<>();
            msg.setSenderID(ConsumerThread.CLIENT_ID); // Set node as sender.

            msg.setTimeStamp(logicalClock);
            logicalClock++;

            messageMap.put(msg.getSenderID(), msg);
            state.put(msg.getMessageID(), messageMap);
            this.deliveryHeap.add(msg);
        }

        KafkaMessage storedMessage = null;

        if (msg.getMessageType() == Type.AckMessage) {
            messageMap.put(msg.getSenderID(), msg);
            storedMessage = msg;

            // TODO: Update deliveryheap here.
            if(!this.deliveryHeap.contains(msg)) {
                this.deliveryHeap.add(msg);
            } else {
                this.deliveryHeap.remove(msg);
                this.deliveryHeap.add(msg);
            }
        }

        // Only check if we reponses from ALL topics, dont waste resources.
        if(this.state.get(msg.getMessageID()).entrySet().size() == msg.getTopic().length) {
            System.out.println("All messages received, find if all is ack");
            boolean allAcks = true;
            PriorityQueue<KafkaMessage> queue = new PriorityQueue<>( new TimestampDecending());

            for (Map.Entry<Integer, KafkaMessage> entry : this.state.get(msg.getMessageID()).entrySet()) {
                // A entry can be updated to decided if our queue is slow.
                if (entry.getValue().getMessageType() != Type.AckMessage) {
                    allAcks = false;
                    break;
                } else {
                    // Sort timestamp from biggest to smallest.
                    queue.add(entry.getValue());
                }
            }

            if (allAcks) {
                System.out.println("All messages are ACK add to delivery heap");
                KafkaMessage decidedMessage = queue.poll();
                decidedMessage.setMessageType(Type.Delivery);
                deliveryHeap.remove(decidedMessage); // Remove to update TS. (This removes on messageID).
                deliveryHeap.add(decidedMessage); // Readding to force placement update.
                System.out.println(Arrays.toString(this.deliveryHeap.toArray()));

            }
        }

        return null;
    }

    @Override
    public KafkaMessage phaseThree(KafkaMessage msg) {
        // Check if we have achieved phase3, meaning every node has recieved the TS and offset.
        // Send a decision message to every node.
        /*if (this.state == null) {
            System.out.println("State is null");
        }

        if (msg == null) {
            System.out.println("MSG is null");
        }

        ConcurrentHashMap<Integer, KafkaMessage> messageMap = this.state.get(msg.getMessageID());
        if (msg.getMessageType() == Type.Decided) {
            messageMap.put(msg.getSenderID(), msg);
        }

        // No need to check every time if we have not gotten all responses yet!
        Set entries = this.state.get(msg.getMessageID()).entrySet();
        if(entries.size() == msg.getTopic().length) {

            // TODO: Go through all deliverable messages and deliver by offsets. from earliest to latest.
            boolean allDecided = true;
            long matchOffset = msg.getOffset();
            long matchTimeStamp = msg.getTimeStamp();
            Set<Integer> haveTopicResponses = new HashSet<>();

            for (Map.Entry<Integer, KafkaMessage> entry : this.state.get(msg.getMessageID()).entrySet()) {
                // TODO: This should be done by the heap, only need to check which Timestamp is the lowest!
                if (entry.getValue().getMessageType() != Type.Decided
                        && entry.getValue().getTimeStamp() != matchTimeStamp
                        && entry.getValue().getOffset() != matchOffset) {
                    allDecided = false;
                } else {
                    // Store unique senders.
                    haveTopicResponses.add(entry.getValue().getSenderID());
                }
            }
            // TODO: REMOVE THIS, delivery will be done by heap
            if (allDecided && haveTopicResponses.size() == msg.getTopic().length) {
                KafkaMessage cloned = cloneMessage(msg);
                cloned.setSenderID(ConsumerThread.CLIENT_ID);
                cloned.setMessageType(Type.Delivery);
                return cloned;
            }
        }*/
        return null;
    }

    @Override
    public KafkaMessage phaseFour(KafkaMessage msg) {
        //System.out.println("In phase four with " + msg.toString());
        KafkaMessage cloned = cloneMessage(msg);
        cloned.setSenderID(ConsumerThread.CLIENT_ID);
        cloned.setMessageType(Type.Delivery);

        return cloned;
    }

    private KafkaMessage cloneMessage(KafkaMessage msg ) {
        KafkaMessage cloned = null;
        try {
            cloned = (KafkaMessage) msg.clone();
        } catch (CloneNotSupportedException e ) {
            System.out.println("COULD NOT CLONE MSG codeline 267");
            cloned = SerializationUtils.clone(msg); // slow
        }
        return cloned;
    }

    public void removeMessage(KafkaMessage msg) {
        this.state.remove(msg.getMessageID());
    }


    public List<KafkaMessage> checkDelivery() {
        List<KafkaMessage> list = new ArrayList<>();
        if (this.deliveryHeap.peek() != null) {

            while (this.deliveryHeap.peek() != null &&
                    this.deliveryHeap.peek().getMessageType() == Type.Delivery)
            {
                System.out.println(this.deliveryHeap.peek().toString() + " can be delivered!");
                KafkaMessage msg = this.deliveryHeap.poll();
                this.state.remove(msg.getMessageID());
                list.add(msg);
            }
        }
        return list;
    }
}//{"messageID":1,"clientID":1,"messageType":"ClientMessage","value":"Hello","topic":["T1","T2"]}
