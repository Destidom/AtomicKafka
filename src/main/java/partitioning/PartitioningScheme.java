package partitioning;


import java.util.Map;

import Runnables.IProducer;
import model.KafkaMessage;
import org.apache.kafka.clients.producer.Partitioner;
        import org.apache.kafka.common.Cluster;

public class PartitioningScheme implements Partitioner {

    public PartitioningScheme(IProducer prod) {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster)
    {
        int partition = 0;
        if(value instanceof KafkaMessage){
            //partition = value.getPartitionID();
        }
        return partition;
    }

    @Override
    public void close() {

    }

}