package model;

import java.io.Serializable;
import java.util.Arrays;

public class KafkaMessage implements Serializable, Cloneable {

    private static final long serialVersionUID = 6024531281280351055L;
    private int messageID;
    private int senderID;



    private String sentFromTopic;
    private Type messageType;
    private String value;
    private String[] topic;


    private long offset;
    private long timeStamp;

    public KafkaMessage() {
        this.messageID = -1;
        this.messageType = null;
        this.senderID = -1;
        this.value = "";
        this.topic = new String[]{"default"};
        this.offset = -999L;
        this.timeStamp = -999L;
    }


    public KafkaMessage(int mID, int cID, Type mType, String value, String[] topic) {
        this.messageID = mID;
        this.messageType = mType;
        this.senderID = cID;
        this.value = value;
        this.topic = topic;
        this.offset = -999L;
        this.timeStamp = -999L;
    }

    public int getMessageID() {
        return messageID;
    }

    public void setMessageID(int messageID) {
        this.messageID = messageID;
    }

    public int getSenderID() {
        return senderID;
    }

    public void setSenderID(int senderID) {
        this.senderID = senderID;
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

    public String getSentFromTopic() {
        return sentFromTopic;
    }

    public void setSentFromTopic(String sentFromTopic) {
        this.sentFromTopic = sentFromTopic;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    // TODO: Implement Equals, hashcode methods for comparison.

    public Object clone() throws CloneNotSupportedException {
        return (KafkaMessage) super.clone();
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

       if (!(o instanceof KafkaMessage)) {
            return false;
        }

        // typecast o to KafkaMessage so that we can compare data members
        KafkaMessage c = (KafkaMessage) o;

        // Compare the data members and return accordingly
        return c.getMessageID() == this.getMessageID();
    }

    @Override
    public String toString() {
        return "KafkaMessage {" +
                "messageID=" + messageID +
                ", senderID=" + senderID +
                ", messageType=" + messageType +
                ", value='" + value + '\'' +
                ", topic=" + Arrays.toString(topic) +
                ", OffSet=" + offset +
                ", TimeStamp=" + timeStamp +
                '}';
    }

}
