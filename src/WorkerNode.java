import java.io.*;
import java.net.*;
import java.util.UUID;

public class WorkerNode {
    private final String masterAddress;
    private final int masterPort;
    private final String name = UUID.randomUUID().toString();
    private volatile boolean running = true;

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java WorkerNode <master address> <master port>");
            System.exit(1);
        }

        String masterAddress = args[0];
        int masterPort = Integer.parseInt(args[1]);

        WorkerNode workerNode = new WorkerNode(masterAddress, masterPort);
        workerNode.start();
    }

    public WorkerNode(String masterAddress, int masterPort) {
        this.masterAddress = masterAddress;
        this.masterPort = masterPort;
    }

    public void start() {
        while (running) {
            try (Socket socket = new Socket(masterAddress, masterPort);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

                writer.println("Worker " + name + " connected.");

                // Start a new thread to handle incoming messages
                Thread messageHandlerThread = new Thread(() -> {
                    try {
                        String message;
                        while (running && (message = reader.readLine()) != null) {
                            Message receivedMessage = Message.deserialize(message);
                            System.out.println("Received: " + receivedMessage);
                            handleMessage(receivedMessage, writer);
                        }
                    } catch (IOException e) {
                        if (running) {
                            System.err.println("Error reading from master: " + e.getMessage());
                        }
                    } finally {
                        cleanUp();
                    }
                });

                messageHandlerThread.start();
                messageHandlerThread.join(); // Wait for the message handler to finish

            } catch (IOException e) {
                System.err.println("Connection error: " + e.getMessage());
                // Retry mechanism with exponential backoff
                retryConnection();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void handleMessage(Message message, PrintWriter writer) {
        switch (message.getType()) {
            case PING:
                writer.println(new Message(Message.MessageType.PING, "from " + name).serialize());
                break;
            case BROADCAST:
                writer.println(new Message(Message.MessageType.BROADCAST, "Broadcast received by " + name).serialize());
                break;
            case CHAIN:
                String forwardedMessage = message.getContent() + " -> " + name;
                new Thread(() -> forwardChainMessage(forwardedMessage)).start();
                break;
        }
    }

    private void forwardChainMessage(String messageContent) {
        Message message = new Message(Message.MessageType.CHAIN, messageContent);
        try (Socket socket = new Socket(masterAddress, masterPort);
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
            writer.println(message.serialize());
        } catch (IOException e) {
            System.err.println("Error forwarding chain message: " + e.getMessage());
        }
    }

    private void retryConnection() {
        int attempt = 0;
        while (attempt < 5 && running) {
            try {
                Thread.sleep(1000 * (long) Math.pow(2, attempt)); // Exponential backoff
                System.out.println("Retrying connection... attempt " + (attempt + 1));
                break; // Exit loop if successful connection
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Retry interrupted: " + e.getMessage());
            }
            attempt++;
        }
    }

    private void cleanUp() {
        running = false;
        // Additional cleanup if necessary
        System.out.println("Worker node cleanup.");
    }
}


