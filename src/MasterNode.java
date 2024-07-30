import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class MasterNode {
    private final int port;
    private final List<DatagramPacket> workerPackets = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final long delayMillis;
    private final int pingPongTimes;
    private final String command;

    public int getPort() {
        return port;
    }

    public long getDelayMillis() {
        return delayMillis;
    }

    public int getPingPongTimes() {
        return pingPongTimes;
    }

    public String getCommand() {
        return command;
    }

    public MasterNode(long delayMillis, int pingPongTimes, String command, int port) {
        this.delayMillis = delayMillis;
        this.pingPongTimes = pingPongTimes;
        this.command = command;
        this.port = port;
    }

    public void connectWorker(int numberOfWorkers) throws SocketException, IOException {
        DatagramSocket socket = new DatagramSocket(port);
        System.out.println("Master node started on port " + port);


        while (workerPackets.size() < numberOfWorkers) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            String message = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Connection: " + message);
            workerPackets.add(packet);
        }
        executor.submit(new WorkerHandler(socket));
    }


    public void start() throws InterruptedException {
        switch (this.command.toLowerCase()) {
            case "ping":
                Thread.sleep(delayMillis);
                this.sendPing();
                break;
            case "broadcast":
                Thread.sleep(delayMillis);
                this.sendBroadcast("Hello Workers!");
                break;
            case "chain":
                Thread.sleep(delayMillis);
                this.startChain("Chain Start");
                break;
            default:
                System.err.println("Invalid command. Use 'ping', 'broadcast', or 'chain'.");
                System.exit(1);
        }
    }

    public void sendPing() throws InterruptedException {

        for (int i = 0; i < pingPongTimes; i++) {
            Thread.sleep(delayMillis);
            for (DatagramPacket packet : workerPackets) {

                JSONObject json = new JSONObject();
                json.put("type", "PING");
                json.put("message", "Ping");
                sendResponse(json.toString(), packet.getAddress(), packet.getPort());
            }
        }
    }

    public void sendBroadcast(String message) {
        for (DatagramPacket packet : workerPackets) {
            JSONObject json = new JSONObject();
            json.put("type", "BROADCAST");
            json.put("message", message);
            sendResponse(json.toString(), packet.getAddress(), packet.getPort());
        }
    }

    public void startChain(String initialMessage) {
        if (!workerPackets.isEmpty()) {
            DatagramPacket firstWorkerPacket = workerPackets.getFirst();
            JSONObject json = new JSONObject();
            json.put("type", "CHAIN");
            json.put("message", initialMessage);
            json.put("workers", getWorkersList());
            json.put("index", 0);
            sendResponse(json.toString(), firstWorkerPacket.getAddress(), firstWorkerPacket.getPort());
        }
    }

    private JSONObject getWorkersList() {
        JSONObject workersList = new JSONObject();
        for (int i = 0; i < workerPackets.size(); i++) {
            DatagramPacket packet = workerPackets.get(i);
            JSONObject workerInfo = new JSONObject();
            workerInfo.put("address", packet.getAddress().getHostAddress());
            workerInfo.put("port", packet.getPort());
            workersList.put("worker" + i, workerInfo);
        }
        return workersList;
    }

    private void sendResponse(String response, InetAddress address, int port) {
        byte[] buf = response.getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }
}
