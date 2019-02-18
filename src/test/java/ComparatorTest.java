import model.KafkaMessage;
import model.Type;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;


public class ComparatorTest {

    private PriorityQueue<KafkaMessage> queue = null;

    @BeforeEach
    void init(){
        queue = new PriorityQueue<>(new TimestampComparator());
    }

    @Test
    void TimestampPullingTest() {
        KafkaMessage m1 = new KafkaMessage();
        m1.setTimeStamp(1L);

        KafkaMessage m2 = new KafkaMessage();
        m2.setTimeStamp(2L);

        KafkaMessage m3 = new KafkaMessage();
        m3.setTimeStamp(3L);

        KafkaMessage m4 = new KafkaMessage();
        m4.setTimeStamp(4L);

        KafkaMessage m5 = new KafkaMessage();
        m5.setTimeStamp(5L);

        PriorityQueue<KafkaMessage> queue = new PriorityQueue<>(new TimestampComparator());

        queue.add(m2);
        queue.add(m4);
        queue.add(m3);
        queue.add(m5);
        queue.add(m1);

        Long[] wantedTS = new Long[] {1L, 2L, 3L, 4L, 5L};

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
        KafkaMessage m1 = new KafkaMessage();
        m1.setTimeStamp(101L);
        m1.setSenderID(1);
        m1.setMessageID(1);

        ConcurrentHashMap<Integer, KafkaMessage> state = new ConcurrentHashMap<>();
        PriorityQueue<KafkaMessage> queue = new PriorityQueue<>(new TimestampComparator());

        state.put(m1.getMessageID(), m1);
        queue.add(m1);

        queue.peek().setTimeStamp(404L);
        state.get(m1.getMessageID()).setMessageType(Type.AckMessage);

        assertEquals(state.get(m1.getMessageID()), queue.peek());
    }

    @Test
    void QueueRebalanceTest() {
        KafkaMessage m1 = new KafkaMessage();
        m1.setTimeStamp(101L);
        m1.setSenderID(1);
        m1.setMessageID(1);

        KafkaMessage m2 = new KafkaMessage();
        m2.setSenderID(2);
        m2.setMessageID(2);
        m2.setTimeStamp(201L);

        KafkaMessage m3 = new KafkaMessage();
        m3.setSenderID(3);
        m3.setMessageID(3);
        m3.setTimeStamp(301L);

        ConcurrentHashMap<Integer, KafkaMessage> state = new ConcurrentHashMap<>();
        PriorityQueue<KafkaMessage> queue = new PriorityQueue<>(new TimestampComparator());

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

}


