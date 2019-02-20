import comparator.TimestampAscending;
import model.KafkaMessage;
import model.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;


public class ComparatorTest {

    private PriorityQueue<KafkaMessage> queue = null;
    private ConcurrentHashMap<Integer, KafkaMessage> state = null;


    private KafkaMessage m1 = null;
    private KafkaMessage m2 = null;
    private KafkaMessage m3 = null;
    private KafkaMessage m4 = null;
    private KafkaMessage m5 = null;

    @BeforeEach
    void init(){
        queue = new PriorityQueue<>(new TimestampAscending());
        state = new ConcurrentHashMap<>();

        m1 = new KafkaMessage();
        m1.setTimeStamp(101L);
        m1.setSenderID(1);
        m1.setMessageID(1);

        m2 = new KafkaMessage();
        m2.setSenderID(2);
        m2.setMessageID(2);
        m2.setTimeStamp(201L);

        m3 = new KafkaMessage();
        m3.setSenderID(3);
        m3.setMessageID(3);
        m3.setTimeStamp(301L);

        m4 = new KafkaMessage();
        m4.setTimeStamp(401L);
        m4.setSenderID(4);
        m4.setMessageID(4);


        m5 = new KafkaMessage();
        m5.setTimeStamp(501L);
        m5.setSenderID(5);
        m5.setMessageID(5);


    }

    @Test
    void TimestampPullingTest() {

        queue.add(m2);
        queue.add(m4);
        queue.add(m3);
        queue.add(m5);
        queue.add(m1);

        Long[] wantedTS = new Long[] {101L, 201L, 301L, 401L, 501L};

        int index = 0;
        while(!queue.isEmpty()) {

            if (queue.poll().getTimeStamp() == wantedTS[index]) {
                index++;
            } else {
                fail("Comparator failed simple 1 2 3 4 5 test");
            }
        }
    }

    @Test
    void ObjectreferenceEqualTest() {
        ConcurrentHashMap<Integer, KafkaMessage> state = new ConcurrentHashMap<>();
        PriorityQueue<KafkaMessage> queue = new PriorityQueue<>(new TimestampAscending());

        state.put(m1.getMessageID(), m1);
        queue.add(m1);

        queue.peek().setTimeStamp(404L);
        state.get(m1.getMessageID()).setMessageType(Type.AckMessage);

        assertEquals(state.get(m1.getMessageID()), queue.peek());
    }

    @Test
    void QueueRebalanceTest() {

        state.put(m3.getMessageID(), m3);
        queue.add(m3);

        state.put(m2.getMessageID(), m2);
        queue.add(m2);


        state.put(m1.getMessageID(), m1);
        queue.add(m1);


        // REMEMBER WHEN EDITING A QUEUE OBJECT NEED TO REINSERT THE OBJECT INTO THE QUEUE AGAIN TO GET RIGHT ORDER!
        KafkaMessage m = queue.peek();
        m.setTimeStamp(404L);
        queue.remove(m);
        queue.add(m);
        state.get(m1.getMessageID()).setMessageType(Type.AckMessage);


        Long[] wantedTS = new Long[] {201L, 301L, 404L};

        int index = 0;
        while(!queue.isEmpty()) {
            KafkaMessage msg = queue.poll();
            if (msg.getTimeStamp() == wantedTS[index]) {
                index++;
            } else {
                fail("Comparator failed QueueRebalanceTest \n" + msg.getTimeStamp() + " " + index);
            }
        }
    }

    @Test
    void EqualTimestampDifferentMessageIDReference() {

        state.put(m3.getMessageID(), m3);
        queue.add(m3);

        state.put(m2.getMessageID(), m2);
        queue.add(m2);


        state.put(m1.getMessageID(), m1);
        queue.add(m1);


        // REMEMBER WHEN EDITING A QUEUE OBJECT NEED TO REINSERT THE OBJECT INTO THE QUEUE AGAIN TO GET RIGHT ORDER!
        KafkaMessage[] wantedMsg = new KafkaMessage[] {m1, m2, m3};

        int index = 0;
        while(!queue.isEmpty()) {
            KafkaMessage msg = queue.poll();
            System.out.println(msg.getMessageID() + " wanted: " + wantedMsg[index].getMessageID() );
            if (msg == wantedMsg[index]) { // Reference Check.
                index++;
            } else {
                fail("Comparator failed EqualTimestampDifferentMessageIDReference \n" + msg.getTimeStamp() + " " + index);
            }
        }
    }

    @Test
    void EqualTimestampDifferentMessageIDEquals() {

        state.put(m3.getMessageID(), m3);
        queue.add(m3);

        state.put(m2.getMessageID(), m2);
        queue.add(m2);


        state.put(m1.getMessageID(), m1);
        queue.add(m1);

        KafkaMessage[] wantedMsg = new KafkaMessage[] {m1, m2, m3};

        int index = 0;
        while(!queue.isEmpty()) {
            KafkaMessage msg = queue.poll();
            if (msg.equals(wantedMsg[index])) { // Reference Check.
                index++;
            } else {
                fail("Comparator failed EqualTimestampDifferentMessageIDEquals \n" + msg.getTimeStamp() + " " + index);
            }
        }
    }
}


