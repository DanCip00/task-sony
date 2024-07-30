import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.TreeMap;


public class WorkerHandler implements Runnable {
    private final DatagramSocket socket;
    private final TreeMap<String, InetAddress> workerAddresses;
    private final TreeMap<String, Integer> workerPort;

    public TreeMap<String, Integer> getWorkerPort() {
        return workerPort;
    }

    public TreeMap<String, InetAddress> getWorkerAddresses() {
        return workerAddresses;
    }

    WorkerHandler(DatagramSocket socket, TreeMap<String, InetAddress> workerAddresses, TreeMap<String, Integer> workerPort) {
        this.socket = socket;
        this.workerAddresses = workerAddresses;
        this.workerPort = workerPort;
    }

    @Override
    public void run() {
        System.out.println("(Master) WorkerHandler started");
        try {

            while (true) {
                byte[] buf = new byte[512];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                JSONObject jsonResponse = new JSONObject(new String(packet.getData()));
                String message = jsonResponse.getString("message");
                String name = jsonResponse.getString("name");

                System.out.println("(Master) From "+name+" : " + message);
            }
        } catch (IOException e) {
            System.err.println("(Master) Received from worker: " + e.getMessage());
        }
        return;
    }
}
