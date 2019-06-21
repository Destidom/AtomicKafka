package constants;

// TODO: Why did I make this as a interface???
public interface Constants {
    String KAFKA_BROKERS = "pitter1.ux.uis.no:2181";
    Integer MESSAGE_COUNT = 1;
    String CLIENT_ID = "1";
    String GROUP_ID_CONFIG = "1";
    Integer MAX_NO_MESSAGE_FOUND_COUNT = 10000;
    String OFFSET_RESET_LATEST = "latest";
    String OFFSET_RESET_EARLIER = "earliest";
    Integer MAX_POLL_RECORDS = 1000;
    int clientID = 0;
    int KafkaProducerID = 0;
    int KafkaConsumerID = 0;
}
