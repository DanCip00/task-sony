import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class MasterNode {
    private final int port = 12345;
    private final List<Socket> workerSockets = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final long delayMillis;
    private final String command;
    private final int pingPongCount; // Number of ping-pong interactions

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: java MasterNode <delay in milliseconds> <number of workers> <ping|broadcast|chain> [pingPongCount]");
            System.exit(1);
        }

        long delayMillis = Long.parseLong(args[0]);
        int numWorkers = Integer.parseInt(args[1]);
        String command = args[2];
        int pingPongCount = args.length > 3 ? Integer.parseInt(args[3]) : 1; // Default to 1 if not specified

        MasterNode masterNode = new MasterNode(delayMillis, command, pingPongCount);
        masterNode.start(numWorkers);
    }

    public MasterNode(long delayMillis, String command, int pingPongCount) {
        this.delayMillis = delayMillis;
        this.command = command;
        this.pingPongCount = pingPongCount;
    }

    public void start(int numberOfWorkers) throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Master node started on port " + port);

        while (workerSockets.size() < numberOfWorkers) {
            Socket workerSocket = serverSocket.accept();
            workerSockets.add(workerSocket);
            executor.submit(new WorkerHandler(workerSocket));
        }

        Thread.sleep(delayMillis);

        switch (this.command.toLowerCase()) {
            case "ping":
                sendPingPong();
                break;
            case "broadcast":
                sendBroadcast("Hello Workers!");
                break;
            case "chain":
                startChain("Chain Start");
                break;
            default:
                System.err.println("Invalid command. Use 'ping', 'broadcast', or 'chain'.");
                System.exit(1);
        }

        // Shutdown the executor service
        executor.shutdown();
    }

    private void sendPingPong() {
        for (int i = 0; i < pingPongCount; i++) {
            for (Socket socket : workerSockets) {
                try (PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
                    Message message = new Message(Message.MessageType.PING, "");
                    writer.println(message.serialize());
                } catch (IOException e) {
                    System.err.println("Error sending PING: " + e.getMessage());
                }
            }

            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Ping-pong interrupted: " + e.getMessage());
            }

            for (Socket socket : workerSockets) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    String response = reader.readLine();
                    if (response != null) {
                        Message message = Message.deserialize(response);
                        System.out.println("Received from worker: " + message);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading from worker: " + e.getMessage());
                }
            }
        }
    }

    private void sendBroadcast(String messageContent) {
        Message message = new Message(Message.MessageType.BROADCAST, messageContent);
        for (Socket socket : workerSockets) {
            try (PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
                writer.println(message.serialize());
            } catch (IOException e) {
                System.err.println("Error sending broadcast: " + e.getMessage());
            }
        }
    }

    public void startChain(String initialMessage) {
        if (!workerSockets.isEmpty()) {
            Message message = new Message(Message.MessageType.CHAIN, initialMessage);
            try (PrintWriter writer = new PrintWriter(workerSockets.get(0).getOutputStream(), true)) {
                writer.println(message.serialize());
            } catch (IOException e) {
                System.err.println("Error starting chain: " + e.getMessage());
            }
        }
    }

    private class WorkerHandler implements Runnable {
        private final Socket workerSocket;

        WorkerHandler(Socket socket) {
            this.workerSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()));
                 PrintWriter writer = new PrintWriter(workerSocket.getOutputStream(), true)) {

                System.out.println("Connection established with worker: " + workerSocket);

                String message;
                while ((message = reader.readLine()) != null) {
                    Message receivedMessage = Message.deserialize(message);
                    System.out.println("Received from worker: " + receivedMessage);
                    handleMessage(receivedMessage, writer);
                }
            } catch (SocketException e) {
                System.err.println("Socket closed unexpectedly: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Error reading from worker: " + e.getMessage());
            } finally {
                cleanUp();
            }
        }

        private void handleMessage(Message message, PrintWriter writer) {
            switch (message.getType()) {
                case PING:
                    writer.println(new Message(Message.MessageType.PONG, "from Master").serialize());
                    break;
                case BROADCAST:
                    System.out.println("Broadcast received response: " + message.getContent());
                    break;
                case CHAIN:
                    String forwardedMessage = message.getContent() + " -> Master";
                    forwardChainMessage(forwardedMessage);
                    break;
            }
        }

        private void forwardChainMessage(String messageContent) {
            Message message = new Message(Message.MessageType.CHAIN, messageContent);
            for (Socket socket : workerSockets) {
                try (PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
                    writer.println(message.serialize());
                } catch (IOException e) {
                    System.err.println("Error forwarding chain message: " + e.getMessage());
                }
            }
        }

        private void cleanUp() {
            try {
                System.out.println("Closing connection with worker: " + workerSocket);
                if (workerSocket != null && !workerSocket.isClosed()) workerSocket.close();
            } catch (IOException e) {
                System.err.println("Error during cleanup: " + e.getMessage());
            }
        }
    }
}
