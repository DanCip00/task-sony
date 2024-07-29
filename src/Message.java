public class Message {
    public enum MessageType {
        PING, BROADCAST, PONG, CHAIN
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

    // Serialize the Message object to a string
    public String serialize() {
        return type.name() + ":" + content;
    }

    // Deserialize a string to a Message object
    public static Message deserialize(String data) {
        String[] parts = data.split(":", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid message format");
        }
        MessageType type = MessageType.valueOf(parts[0]);
        String content = parts[1];
        return new Message(type, content);
    }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + type +
                ", content='" + content + '\'' +
                '}';
    }
}

