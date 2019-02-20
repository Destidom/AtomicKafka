package comparator;

import model.KafkaMessage;

import java.io.Serializable;
import java.util.Comparator;

public class TimestampDecending implements Comparator<KafkaMessage>, Serializable {

    /**
     * Compares two KafkaMessages for PriorityQueue.
     * Note, No two messages are the same as each message should be uniquely identifiable through messageID in
     * the Prioritybuffer.
     *
     * ONLY USE THIS COMPARATOR WITH A PRIORITYBUFFER TO SORT MESSAGES LOCALLY.
     *
     * @param o1 KafkaMessage
     * @param o2 KafkaMessage
     * @return -1 or 1
     */
    @Override
    public int compare(KafkaMessage o1, KafkaMessage o2) {

        // If equal timestamp use messageID as tiebreaker.
        if (o1.getTimeStamp() == o2.getTimeStamp()) {
            return 0;
        }

        return o1.getTimeStamp() > o2.getTimeStamp() ? -1 : 1;
    }
}
