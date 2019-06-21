package consensus;

import Runnables.ConsumerThread;
import comparator.TimestampDecending;
import model.KafkaMessage;
import model.Type;
import org.apache.commons.lang3.SerializationUtils;
import comparator.TimestampAscending;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, KafkaMessage>> state = new ConcurrentHashMap<>(25000);

    public static AtomicMulticast instance;

    private String topic = "";

    public PriorityQueue<KafkaMessage> getDeliveryHeap() {
        return deliveryHeap;
    }

    public void setTopic(String topic){
        this.topic = topic;
    }


    public ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, KafkaMessage>> getMap() {
        return state;
    }
    // Heap based queue
    private final PriorityQueue<KafkaMessage> deliveryHeap = new PriorityQueue<>(3000, new TimestampAscending());

    protected Long logicalClock = 0L;

    private int senderID = 0;

    public void setSenderID(int id){
        this.senderID = id;
    }

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

    /**
     * Phase One is the content based multicast phase.
     * Ensuring all intended Topic receive the Message.
     *
     * @param msg
     * @return
     */
    @Override
    public KafkaMessage ReceivedClientMessage(KafkaMessage msg) {
        // Check message is designated to only one topic or more, if more notify others.
        // Send Unique Ack if only to us to log delivery.

        // Two possible message types, ClientMessage (as the first message we received, or notify message
        // Both messages means the same, but comes from different origins.
        // And if a Node has not received a clientmessage, it will receive a notify message and treat it the same way.


        // If only one topic, skip phases and storing message and send it directly to kafka.
        if (msg.getTopic().length == 1 && msg.getTopic()[0].equalsIgnoreCase(this.topic)) {
            // Send message directly to phase 4.
            msg.setMessageType(Type.Delivery);
            msg.setTimeStamp(logicalClock);
            logicalClock++;
            deliveryHeap.add(msg);
            return null; // Delivery happens through deliveryHeap!
        } else if (msg.getTopic().length == 1) {
            msg.setMessageType(Type.ClientMessage);
            return msg;
        }

        // More than one destination!
        // Update the timestamp!
        msg.setTimeStamp(logicalClock);
        logicalClock++;


        // Create a state map
        msg.setMessageType(Type.AckMessage);
        ConcurrentHashMap<Integer, KafkaMessage> lst = state.get(msg.getMessageID());
        if (lst == null) {
            lst = new ConcurrentHashMap<>(7);
            msg.setSenderID(ConsumerThread.CLIENT_ID); // Set node as sender.
            lst.put(msg.getSenderID(), msg);
            state.put(msg.getMessageID(), lst);
        } else {
            System.out.println("Something impossible happened in ClientMessage");
            state.get(msg.getMessageID()).put(msg.getSenderID(), msg);

        }

        // Set message as ACK for ourselves.

        if(!this.deliveryHeap.contains(msg))
            this.deliveryHeap.add(msg);

        // Create response message.
        KafkaMessage cloned = cloneMessage(msg);
        cloned.setSenderID(ConsumerThread.CLIENT_ID); // Set ourselves as sender.


        // Notify others that there are a new message.
        cloned.setMessageType(Type.NotifyMessage);


        return cloned;
    }


    public KafkaMessage ReceivedNotify(KafkaMessage msg) {
        // Check message is designated to only one topic or more, if more notify others.
        // Send Unique Ack if only to us to log delivery.

        // Two possible message types, ClientMessage (as the first message we received, or notify message
        // Both messages means the same, but comes from different origins.
        // And if a Node has not received a clientmessage, it will receive a notify message and treat it the same way.
        if(logicalClock < msg.getTimeStamp())
            logicalClock = msg.getTimeStamp();

        // If only one topic, skip phases and storing message and send it directly to kafka.
        if (msg.getTopic().length == 1 && msg.getTopic()[0].equalsIgnoreCase(this.topic)) {
            // Send message directly to phase 4.
            msg.setMessageType(Type.Delivery);
            msg.setTimeStamp(logicalClock);
            logicalClock++;
            deliveryHeap.add(msg);
            return null; // Delivery happens through deliveryHeap!
        }

        // Create response message.
        KafkaMessage cloned = cloneMessage(msg);
        cloned.setSenderID(ConsumerThread.CLIENT_ID); // Set ourselves as sender.
        cloned.setMessageType(Type.AckMessage);

        // Set timestamp
        msg.setTimeStamp(logicalClock);
        logicalClock++;

        // Create a state map
        msg.setMessageType(Type.AckMessage);
        ConcurrentHashMap<Integer, KafkaMessage> list = state.get(msg.getMessageID());
        if (list == null) {
            list = new ConcurrentHashMap<>(100);
            list.put(msg.getSenderID(), msg); // received message
            list.put(cloned.getSenderID(), cloned); // our clone
            state.put(msg.getMessageID(), list);
        } else {
            state.get(msg.getMessageID()).put(msg.getSenderID(), msg);
            state.get(msg.getMessageID()).put(cloned.getSenderID(), cloned);
        }

        // Set message as ACK.

        if(!this.deliveryHeap.contains(msg))
            this.deliveryHeap.add(msg);

        return cloned;
    }






    // Wait for a last ack, if no ack received send notify.
    // If no received ack from that notify assume responsibility of the topic (?)
    @Override
    public KafkaMessage ReceivedAcknowledge(KafkaMessage msg) {
        // Check if message has achieved phase2, meaning every node has received the msg.
        ConcurrentHashMap<Integer, KafkaMessage> messageMap = this.state.get(msg.getMessageID());

        if (messageMap == null) {
            System.out.println("Something impossible happened in ACK");
            return null;
        }

        if(logicalClock <= msg.getTimeStamp())
            logicalClock = msg.getTimeStamp() + 1;

        // FIRST TIME this node will see a message from sender!
        messageMap.put(msg.getSenderID(), msg);

        // Only check if we reponses from ALL topics, dont waste resources.
        Set<Map.Entry<Integer, KafkaMessage>> messages = this.state.get(msg.getMessageID()).entrySet();
        if(messages.size() == msg.getTopic().length) {
            boolean allAcks = true;
            PriorityQueue<KafkaMessage> queue = new PriorityQueue<>( new TimestampDecending());

            for (Map.Entry<Integer, KafkaMessage> entry : messages) {
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
                KafkaMessage decidedMessage = queue.poll();

                KafkaMessage cloned = cloneMessage(decidedMessage);
                cloned.setSenderID(ConsumerThread.CLIENT_ID); // Set ourselves as sender.
                cloned.setMessageType(Type.Decided);

                deliveryHeap.remove(decidedMessage);
                decidedMessage.setMessageType(Type.Delivery);
                deliveryHeap.add(decidedMessage);

                return cloned;
            }
        }

        return null;
    }

    @Override
    public void Decided(KafkaMessage msg) {
        if(logicalClock < msg.getTimeStamp())
            logicalClock = msg.getTimeStamp() + 1;

        deliveryHeap.remove(msg);
        msg.setMessageType(Type.Delivery);
        deliveryHeap.add(msg);
    }

    @Override
    public KafkaMessage phaseFour(KafkaMessage msg) {
        throw new NotImplementedException();
    }

    private KafkaMessage cloneMessage(KafkaMessage msg ) {
        KafkaMessage cloned = null;
        try {
            cloned = (KafkaMessage) msg.clone();
        } catch (CloneNotSupportedException e ) {
            cloned = SerializationUtils.clone(msg); // slow
        }
        return cloned;
    }

    public void removeMessage(KafkaMessage msg) {
        this.state.remove(msg.getMessageID());
    }

    public List<KafkaMessage> checkDelivery() {

        List<KafkaMessage> list = new ArrayList<>();
        while (this.deliveryHeap.peek() != null &&
                this.deliveryHeap.peek().getMessageType() == Type.Delivery)
        {
            KafkaMessage msg = this.deliveryHeap.poll();
            msg.setSenderID(1);
            msg.setSentFromTopic(this.topic);
            this.state.remove(msg.getMessageID());
            list.add(msg);
        }

        return list;
    }
}
