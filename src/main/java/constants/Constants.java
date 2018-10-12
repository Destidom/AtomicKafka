package constants;

public interface Constants {
    String KAFKA_BROKERS = "pitter11.ux.uis.no:9000";
    Integer MESSAGE_COUNT = 1;
    String CLIENT_ID = "1";
    String GROUP_ID_CONFIG = "1";
    Integer MAX_NO_MESSAGE_FOUND_COUNT = 10000;
    String TOPIC_NAME = "home-test";
    String OFFSET_RESET_LATEST = "latest";
    String OFFSET_RESET_EARLIER = "earliest";
    Integer MAX_POLL_RECORDS = 1;
}
