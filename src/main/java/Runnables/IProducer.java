package Runnables;

import model.KafkaMessage;

import java.util.List;

public interface IProducer {

    void sendMessage(KafkaMessage msg, String topic);

    void sendMessage(KafkaMessage msg, List<String> topics);

    void stop();
}
