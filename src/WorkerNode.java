import org.json.JSONObject;
import java.io.IOException;
import java.net.*;
import java.util.UUID;

public class WorkerNode {
    private final InetAddress masterAddress;
    private final int masterPort;
    private final String name ;
    private volatile boolean running = true;

    /**
     * Static function for the creation of workers
     * @param args - <master address> <master port>
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java WorkerNode <master address> <master port>");
            System.exit(1);
        }

        String masterAddress = args[0];
        int masterPort = Integer.parseInt(args[1]);

        WorkerNode workerNode = new WorkerNode(masterAddress, masterPort, UUID.randomUUID().toString());
        workerNode.start();
    }

    public WorkerNode(String masterAddress, int masterPort, String name) throws UnknownHostException {
        this.masterAddress = InetAddress.getByName(masterAddress);
        this.masterPort = masterPort;
        this.name = name;
    }

    public void start() {
        while (running) {
            // Connect
            try (DatagramSocket socket = new DatagramSocket()) {

                JSONObject workerInfo = new JSONObject();
                workerInfo.put("type", "CONNECTION");
                workerInfo.put("name", name);
                byte[] buf = workerInfo.toString().getBytes();

                DatagramPacket packet = new DatagramPacket(buf, buf.length, masterAddress, masterPort);
                socket.send(packet);

                // Working

                byte[] receiveBuf = new byte[1026];
                DatagramPacket receivePacket = new DatagramPacket(receiveBuf, receiveBuf.length);

                while (running) {
                    socket.receive(receivePacket);
                    String message = new String(receivePacket.getData(), 0, receivePacket.getLength());
                    System.out.println("("+name+") Received: " + message);
                    handleMessage(message, socket, receivePacket.getAddress(), receivePacket.getPort());
                }
            } catch (IOException e) {
                System.err.println("Connection error: " + e.getMessage());
                retryConnection();
            }
        }
    }

    private void handleMessage(String message, DatagramSocket socket, InetAddress address, int port) {
        JSONObject json = new JSONObject(message);
        String type = json.getString("type");

        switch (type) {
            case "PING":
                sendResponse("PONG", socket, masterAddress, masterPort);
                break;
            case "BROADCAST":
                sendResponse("Hi master!", socket, masterAddress, masterPort);
                break;
            case "CHAIN":
                handleChainMessage(json, socket, address, port);
                break;
            case "EXIT":
                running = false;
                break;
        }
    }

    private void handleChainMessage(JSONObject json, DatagramSocket socket, InetAddress address, int port) {
        JSONObject workersList = json.getJSONObject("workers");
        int index = json.getInt("index");

        String message = json.getString("message") + " -> " + name;

        // Forward to the next worker in the chain
        int nextIndex = index + 1;
        if (workersList.has("worker" + nextIndex)) {
            JSONObject nextWorker = workersList.getJSONObject("worker" + nextIndex);
            try (DatagramSocket newSocket = new DatagramSocket()) {
                JSONObject newMessage = new JSONObject();
                newMessage.put("type", "CHAIN");
                newMessage.put("message", message);
                newMessage.put("workers", workersList);
                newMessage.put("index", nextIndex);

                byte[] buf = newMessage.toString().getBytes();
                DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(nextWorker.getString("address")), nextWorker.getInt("port"));
                newSocket.send(packet);
            } catch (IOException e) {
                System.err.println("Error forwarding chain message: " + e.getMessage());
            }
        }else{
            sendResponse("Chain completed: " + message, socket, masterAddress, masterPort);
        }
    }

    private void sendResponse(String response, DatagramSocket socket, InetAddress address, int port) {
        JSONObject json = new JSONObject();
        json.put("type", "RESPONSE");
        json.put("name", name);
        json.put("message", response);
        byte[] buf = json.toString().getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);
        try {
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }

    private void retryConnection() {
        int attempt = 0;
        while (attempt < 5 && running) {
            try {
                Thread.sleep(2000);
                System.out.println("Retrying connection... attempt " + (attempt + 1));
                break;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Retry interrupted: " + e.getMessage());
            }
            attempt++;
        }
    }
}
