import java.io.IOException;
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
            try (DatagramSocket socket = new DatagramSocket()) {
                InetAddress masterInetAddress = InetAddress.getByName(masterAddress);
                byte[] buf = ("Worker " + name + " connected.").getBytes();
                DatagramPacket packet = new DatagramPacket(buf, buf.length, masterInetAddress, masterPort);
                socket.send(packet);

                byte[] receiveBuf = new byte[256];
                DatagramPacket receivePacket = new DatagramPacket(receiveBuf, receiveBuf.length);

                while (running) {
                    socket.receive(receivePacket);
                    String message = new String(receivePacket.getData(), 0, receivePacket.getLength());
                    System.out.println("Received: " + message);
                    handleMessage(message, socket, receivePacket.getAddress(), receivePacket.getPort());
                }
            } catch (IOException e) {
                System.err.println("Connection error: " + e.getMessage());
                retryConnection();
            }
        }
    }

    private void handleMessage(String message, DatagramSocket socket, InetAddress address, int port) {
        if (message.startsWith("PING")) {
            sendResponse("PONG from " + name, socket, address, port);
        } else if (message.startsWith("BROADCAST")) {
            sendResponse("Broadcast received by " + name, socket, address, port);
        } else if (message.startsWith("CHAIN")) {
            String forwardedMessage = message + " -> " + name;
            forwardChainMessage(forwardedMessage);
        }
    }

    private void sendResponse(String response, DatagramSocket socket, InetAddress address, int port) {
        byte[] buf = response.getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);
        try {
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }

    private void forwardChainMessage(String message) {
        try (DatagramSocket socket = new DatagramSocket()) {
            InetAddress masterInetAddress = InetAddress.getByName(masterAddress);
            byte[] buf = message.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, masterInetAddress, masterPort);
            socket.send(packet);
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
}
