package comparator;

import model.KafkaMessage;

import java.io.Serializable;
import java.util.Comparator;

public class TimestampAscending implements Comparator<KafkaMessage>, Serializable {

    /**
     * Compares two KafkaMessages for PriorityQueue.
     * Note, No two messages are the same as each message should be uniquely identifiable through messageID in
     * the Prioritybuffer.
     *
     * ONLY USE THIS COMPARATOR WITH A PRIORITYBUFFER WHERE MESSAGEIDS ARE TREATED AS UNIQUE MESSAGES!
     *
     * @param o1 KafkaMessage
     * @param o2 KafkaMessage
     * @return -1 or 1
     */
    @Override
    public int compare(KafkaMessage o1, KafkaMessage o2) {

        // If equal timestamp use messageID as tiebreaker.
        if (o1.getTimeStamp() == o2.getTimeStamp()) {
            return o1.getMessageID() < o2.getMessageID() ? -1 : 1;
        }

        return o1.getTimeStamp() < o2.getTimeStamp() ? -1 : 1;
    }
}
