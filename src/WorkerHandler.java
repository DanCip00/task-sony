import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;


public class WorkerHandler implements Runnable {
    private final DatagramSocket socket;

    WorkerHandler(DatagramSocket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        System.out.println("WorkerHandler started");
        try {

            while (true) {
                byte[] buf = new byte[512];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                System.out.println("Received from worker: " + message);
            }
        } catch (IOException e) {
            System.err.println("Received from worker: " + e.getMessage());
        }
        return;
    }
}
