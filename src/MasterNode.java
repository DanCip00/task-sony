import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class MasterNode {
    private final int port;
    private final TreeMap<String, InetAddress> workerAddresses = new TreeMap<>();
    private final TreeMap<String, Integer> workerPort = new TreeMap<>();

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

    /**
     * Create a MasterNode capable of handle ping, broadcast and chain communcation using connection less UDP.
     * @param delayMillis - Delay between pings and operations
     * @param pingPongTimes - Number of ping sent in case of PING operation
     * @param command - Type of operation PING|BROADCAST|CHAIN
     * @param port - Port of the listening socket
     */
    public MasterNode(long delayMillis, int pingPongTimes, String command, int port) {
        this.delayMillis = delayMillis;
        this.pingPongTimes = pingPongTimes;
        this.command = command;
        this.port = port;
    }

    /**
     * Given a number of Workers, wait until all workers have sent a message. It also create a socket that listen
     * in the given port.
     * @param numberOfWorkers
     * @throws SocketException
     * @throws IOException
     */
    public void connectWorker(int numberOfWorkers) throws SocketException, IOException {
        DatagramSocket socket = new DatagramSocket(port);
        System.out.println("Master node started on port " + port);


        while (workerAddresses.size() < numberOfWorkers) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            String message = new String(packet.getData(), 0, packet.getLength());
            JSONObject jsonObject = new JSONObject(message);
            String nameWorker = jsonObject.getString("name");
            workerAddresses.put(nameWorker, packet.getAddress());
            workerPort.put(nameWorker, packet.getPort());
            System.out.println("Worker" + nameWorker + " connected");
        }
        executor.submit(new WorkerHandler(socket, workerAddresses, workerPort));
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
                System.err.println("(Master) Invalid command");
                System.exit(1);
        }

        // Termination
        executor.close();
        for (String key : workerAddresses.keySet()) {
            JSONObject json = new JSONObject();
            json.put("type", "EXIT");
            json.put("message", "");
            sendResponse(json.toString(), workerAddresses.get(key),workerPort.get(key));
        }
    }

    public void sendPing() throws InterruptedException {

        for (int i = 0; i < pingPongTimes; i++) {
            Thread.sleep(delayMillis);
            for (String key : workerAddresses.keySet()) {

                JSONObject json = new JSONObject();
                json.put("type", "PING");
                json.put("message", "Ping");
                sendResponse(json.toString(), workerAddresses.get(key),workerPort.get(key));
            }
        }
    }

    public void sendBroadcast(String message) {
        for (String key : workerAddresses.keySet()) {
            JSONObject json = new JSONObject();
            json.put("type", "BROADCAST");
            json.put("message", message);
            sendResponse(json.toString(), workerAddresses.get(key),workerPort.get(key));
        }
    }

    public void startChain(String initialMessage) {
        if (!workerAddresses.isEmpty()) {
            String firstWorker = workerAddresses.keySet().iterator().next();
            JSONObject json = new JSONObject();
            json.put("type", "CHAIN");
            json.put("message", initialMessage);
            json.put("workers", getWorkersList());
            json.put("index", 0);
            sendResponse(json.toString(), workerAddresses.get(firstWorker), workerPort.get(firstWorker));
        }
    }

    private JSONObject getWorkersList() {
        JSONObject workersList = new JSONObject();
        int i = 0;
        for (String key: workerAddresses.keySet()) {

            JSONObject workerInfo = new JSONObject();
            workerInfo.put("address", workerAddresses.get(key).getHostAddress());
            workerInfo.put("port", workerPort.get(key));
            workersList.put("worker" + i++, workerInfo);
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
