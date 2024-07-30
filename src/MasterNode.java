import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class MasterNode {
    private final int port = 12345;
    private final List<DatagramPacket> workerPackets = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final long delayMillis;
    private final int pingPongTimes;
    private String command;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.err.println("Usage: java MasterNode <delay in milliseconds> <number of workers> <ping-pong times> <ping|broadcast|chain>");
            System.exit(1);
        }

        long delayMillis = Long.parseLong(args[0]);
        int numWorkers = Integer.parseInt(args[1]);
        int pingPongTimes = Integer.parseInt(args[2]);
        String command = args[3];

        MasterNode masterNode = new MasterNode(delayMillis, pingPongTimes, command);
        masterNode.start(numWorkers);
    }

    public MasterNode(long delayMillis, int pingPongTimes, String command) {
        this.delayMillis = delayMillis;
        this.pingPongTimes = pingPongTimes;
        this.command = command;
    }

    public void start(int numberOfWorkers) throws IOException, InterruptedException {
        DatagramSocket socket = new DatagramSocket(port);
        System.out.println("Master node started on port " + port);

        while (workerPackets.size() < numberOfWorkers) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            workerPackets.add(packet);
            executor.submit(new WorkerHandler(socket, packet));
        }

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
                System.err.println("Invalid command.");
                System.exit(1);
        }
    }

    private class WorkerHandler implements Runnable {
        private final DatagramSocket socket;
        private final DatagramPacket workerPacket;

        WorkerHandler(DatagramSocket socket, DatagramPacket packet) {
            this.socket = socket;
            this.workerPacket = packet;
        }

        @Override
        public void run() {
            try {
                byte[] buf = new byte[256];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                while (true) {
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    System.out.println("Received from worker: " + message);
                    handleMessage(message, packet.getAddress(), packet.getPort());
                }
            } catch (IOException e) {
                System.err.println("Error reading from worker: " + e.getMessage());
            }
        }

        private void handleMessage(String message, InetAddress address, int port) {
            if (message.startsWith("PING")) {
                sendResponse("PONG from Master", address, port);
            } else if (message.startsWith("BROADCAST")) {
                System.out.println("Broadcast received response: " + message);
            } else if (message.startsWith("CHAIN")) {
                forwardChainMessage(message, address, port);
            }
        }

        private void sendResponse(String response, InetAddress address, int port) {
            byte[] buf = response.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);
            try {
                socket.send(packet);
            } catch (IOException e) {
                System.err.println("Error sending response: " + e.getMessage());
            }
        }

        private void forwardChainMessage(String message, InetAddress address, int port) {
            for (DatagramPacket packet : workerPackets) {
                sendResponse(message, packet.getAddress(), packet.getPort());
            }
        }
    }

    public void sendPing() throws InterruptedException {
        for (DatagramPacket packet : workerPackets) {
            sendResponse("PING", packet.getAddress(), packet.getPort());
        }

        for (int i = 0; i < pingPongTimes; i++) {
            Thread.sleep(delayMillis);
            for (DatagramPacket packet : workerPackets) {
                sendResponse("PING", packet.getAddress(), packet.getPort());
            }

            Thread.sleep(delayMillis);
            for (DatagramPacket packet : workerPackets) {
                sendResponse("PONG from Master", packet.getAddress(), packet.getPort());
            }
        }
    }

    public void sendBroadcast(String message) {
        for (DatagramPacket packet : workerPackets) {
            sendResponse("BROADCAST: " + message, packet.getAddress(), packet.getPort());
        }
    }

    public void startChain(String initialMessage) {
        if (!workerPackets.isEmpty()) {
            DatagramPacket firstWorkerPacket = workerPackets.get(0);
            sendResponse("CHAIN: " + initialMessage, firstWorkerPacket.getAddress(), firstWorkerPacket.getPort());
        }
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
