package consensus;

import model.KafkaMessage;

public interface Consensus {

    KafkaMessage phaseOne(KafkaMessage msg);

    KafkaMessage phaseTwo(KafkaMessage msg);

    KafkaMessage phaseThree(KafkaMessage msg);

    KafkaMessage phaseFour(KafkaMessage msg);
}
