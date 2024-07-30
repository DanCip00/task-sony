import java.io.Serializable;

public class Message implements Serializable {

    public enum MessageType {
        INITIALIZE, // Initial connection message
        PING,
        PONG,
        BROADCAST,
        CHAIN
    }

    private final MessageType type;
    private final String content;

    public Message(MessageType type, String content) {
        this.type = type;
        this.content = content;
    }

    public MessageType getType() {
        return type;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "Message{type=" + type + ", content='" + content + "'}";
    }
}
