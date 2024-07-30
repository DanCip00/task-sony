import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        List<Thread> workerThreads = new ArrayList<>();
        int pingPongTimes = 0;
        if (args.length < 4) {
            System.err.println("Wrong number of arguments");
        }

        int masterPort = Integer.parseInt(args[0]);
        long delayMillis = Long.parseLong(args[1]);
        int numberOfWorkers = Integer.parseInt(args[2]);
        String command = args[3];

        if (command.equalsIgnoreCase("ping")){
            pingPongTimes = Integer.parseInt(args[4]);
        }

        System.out.println("Using values:");
        System.out.println("Master Port: " + masterPort);
        System.out.println("Delay (ms): " + delayMillis);
        System.out.println("PingPong Times: " + pingPongTimes);
        System.out.println("Number of Workers: " + numberOfWorkers);
        System.out.println("Command: " + command);

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

            String name = "Worker n°" + i;
            Thread workerThread = new Thread(() -> {
                WorkerNode workerNode = null;
                try {
                    workerNode = new WorkerNode("localhost", masterPort, name);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                workerNode.start();
            });
            workerThreads.add(workerThread);
            workerThread.start();
        }

        // join
        try {
            for (Thread workerThread : workerThreads) {
                workerThread.join();
            }
            masterThread.join();
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }
}

