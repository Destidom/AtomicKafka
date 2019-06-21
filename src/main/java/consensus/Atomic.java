package consensus;

import model.KafkaMessage;

public interface Atomic {

    KafkaMessage ReceivedClientMessage(KafkaMessage msg);

    KafkaMessage ReceivedAcknowledge(KafkaMessage msg);

    void Decided(KafkaMessage msg);

    KafkaMessage phaseFour(KafkaMessage msg);
}
