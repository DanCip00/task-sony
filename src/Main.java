import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        int masterPort = 12346;
        long delayMillis = 500;
        int pingPongTimes = 10;
        int numberOfWorkers = 10;
        String command = "chain";

        // Start the MasterNode
        MasterNode masterNode = new MasterNode(delayMillis, pingPongTimes, command, masterPort);
        Thread masterThread = new Thread(() -> {
            try {
                masterNode.connectWorker(numberOfWorkers);
                masterNode.start();
            } catch (IOException | InterruptedException e) {
                System.err.println("Error in MasterNode: " + e.getMessage());
            }
        });
        masterThread.start();
        Thread.sleep(500);

        // Start WorkerNodes
        for (int i = 0; i < numberOfWorkers; i++) {

            String name = "Worker n°" + String.valueOf(i);
            Thread workerThread = new Thread(() -> {
                WorkerNode workerNode = null;
                try {
                    workerNode = new WorkerNode("localhost", masterPort, name);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                workerNode.start();
            });
            workerThread.start();
        }
    }
}

