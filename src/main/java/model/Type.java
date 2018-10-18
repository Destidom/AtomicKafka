package model;

public enum Type {
    ClientMessage,
    NotifyMessage, // Same as client message, but to inform other AtomicNodes about a message they might not have received.
    DupMessage, // Already received this message before!
    AckMessage, // Acknowledge we have recieved the message?
    UniqueAckMessage, // We are the only receiver to send to delivery right away.
    NackMessage, // Atomic has not been achieved by this node yet.
    Decided, // This node has decided on the message with TS x.
    Delivery, // The consensus is on this message.

}
