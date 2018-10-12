package model;

import java.util.Arrays;

public class KafkaMessage {

    private int messageID;
    private int clientID;
    private Type messageType;
    private String value;
    private String[] topic;


    private long offset;
    private long timeStamp;

    public KafkaMessage() {
        this.messageID = -1;
        this.messageType = null;
        this.clientID = -1;
        this.value = "";
        this.topic = new String[]{"default"};
    }


    public KafkaMessage(int mID, int cID, Type mType, String value, String[] topic) {
        this.messageID = mID;
        this.messageType = mType;
        this.clientID = cID;
        this.value = value;
        this.topic = topic;
    }

    public int getMessageID() {
        return messageID;
    }

    public void setMessageID(int messageID) {
        this.messageID = messageID;
    }

    public int getSenderID() {
        return clientID;
    }

    public void setClientID(int clientID) {
        this.clientID = clientID;
    }

    public Type getMessageType() {
        return messageType;
    }

    public void setMessageType(Type messageType) {
        this.messageType = messageType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public int getClientID() {
        return clientID;
    }

    public String[] getTopic() {
        return topic;
    }

    public void setTopic(String[] topic) {
        this.topic = topic;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "KafkaMessage {" +
                "messageID=" + messageID +
                ", senderID=" + clientID +
                ", messageType=" + messageType +
                ", value='" + value + '\'' +
                ", topic=" + Arrays.toString(topic) +
                '}';
    }

}
