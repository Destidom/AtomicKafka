package Runnables;

import com.codahale.metrics.Meter;
import model.KafkaMessage;

import java.util.List;

public interface IProducer {

    void sendMessage(KafkaMessage msg, String topic);



    void stop();
}
