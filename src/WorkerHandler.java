import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.TreeMap;


/**
 * Runnable class that handle input messages of the master node. Automatically terminate after 2000 ms of no messages
 */
public class WorkerHandler implements Runnable {
    private final DatagramSocket socket;
    private final TreeMap<String, InetAddress> workerAddresses;
    private final TreeMap<String, Integer> workerPort;
    private final int timeoutMillis = 2000;

    public TreeMap<String, Integer> getWorkerPort() {
        return workerPort;
    }

    public TreeMap<String, InetAddress> getWorkerAddresses() {
        return workerAddresses;
    }

    WorkerHandler(DatagramSocket socket, TreeMap<String, InetAddress> workerAddresses, TreeMap<String, Integer> workerPort) throws SocketException {
        this.socket = socket;
        this.workerAddresses = workerAddresses;
        this.workerPort = workerPort;
        this.socket.setSoTimeout(timeoutMillis);
    }

    @Override
    public void run() {
        System.out.println("(Master) WorkerHandler started");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] buf = new byte[512];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
                } catch (IOException e) {
                    if (e.getMessage().contains("Receive timed out")) {
                        System.out.println("(Master) No message received for " + timeoutMillis + " milliseconds. Terminating thread.");
                        socket.close();
                        return;
                    } else throw e;
                }

                JSONObject jsonResponse = new JSONObject(new String(packet.getData(), 0, packet.getLength()));
                String message = jsonResponse.getString("message");
                String name = jsonResponse.getString("name");

                System.out.println("(Master) From " + name + " : " + message);

            } catch (IOException e) {
                System.err.println("(Master) Error while receiving from worker: " + e.getMessage());
                break;
            }
        }
    }
}
