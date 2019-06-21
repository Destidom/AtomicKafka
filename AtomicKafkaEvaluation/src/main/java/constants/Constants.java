package constants;

// TODO: Why did I make this as a interface???
public interface Constants {
    String KAFKA_BROKERS = "pitter24.ux.uis.no:9000";
    Integer MESSAGE_COUNT = 1;
    String CLIENT_ID = "100";
    String GROUP_ID_CONFIG = "100";
    Integer MAX_NO_MESSAGE_FOUND_COUNT = 10000;
    String OFFSET_RESET_LATEST = "latest";
    String OFFSET_RESET_EARLIER = "earliest";
    Integer MAX_POLL_RECORDS = 100;
}
