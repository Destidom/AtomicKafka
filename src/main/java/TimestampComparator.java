import model.KafkaMessage;

import java.io.Serializable;
import java.util.Comparator;

public class TimestampComparator implements Comparator<KafkaMessage>, Serializable {
    @Override
    public int compare(KafkaMessage o1, KafkaMessage o2) {
        return o1.getTimeStamp() < o2.getTimeStamp() ? -1 : o1.getTimeStamp() == o2.getTimeStamp() ? 0 : 1;
    }
}
