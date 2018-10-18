package consensus;

import model.KafkaMessage;

public interface Atomic {

    KafkaMessage phaseOne(KafkaMessage msg);

    KafkaMessage phaseTwo(KafkaMessage msg);

    KafkaMessage phaseThree(KafkaMessage msg);

    KafkaMessage phaseFour(KafkaMessage msg);
}
