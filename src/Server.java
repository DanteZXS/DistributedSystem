import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server extends Thread {
    private static int serverPort;
    private static String name;
    private static int state;
    private static AtomicBoolean isMaster;
    private static int configNum;

    private static int replicaPort;

    /**
     * The following section is for passive replication
     */
    private static int checkpoint_freq;
    private static int backup_id;
    private static int checkpoint_count = 1;
    private static int prev_count = 1;
    private final static int[] backup_ports = {700, 701};
    private static Set<Integer> alive_backups = new HashSet<>();

    private static boolean i_am_ready;
    private static int serverId;
    private static int high_watermark;

    private Thread sendCheckPointThread;
    private Thread receiveCheckPointThread;

    private AtomicBoolean changeStatus;

    /**
     * Each active server will open up two TCP connections as a client socket to the other
     * two active servers; when a server dead and recovers, it opens up the a server socket to receive
     * checkpoints from the other alive servers; after it is updated to the correct states, it re-opens two
     * client sockets.
     */
    private final static int[] recovery_ports = {601, 602, 603};

    public Server() {
        changeStatus = new AtomicBoolean(false);
    }


    public static void main(String[] args) {
        if (args[0].equalsIgnoreCase("-h")) {
            // print how to use the program
            System.out.println("If launching the primary server:");
            System.out.println("<heartbeat_port> <server_name> True <checkpoint_freq> <1 for active 2 for passive> <# of the same server kind> <RM port>");
            System.out.println("If launching the backup server:");
            System.out.println("<heartbeat_port> <server_name> False <id (either 1 or 2)> <1 for active 2 for passive> <# of the same server kind> <RM port>");
            return;
        }
        if (args.length != 7) {
            System.out.println("Wrong Input!!!");
            return;
        }
        serverPort = Integer.parseInt(args[0]);
        name = args[1];
        serverId = Integer.parseInt(name.replaceAll("[\\D]", ""));
        if (serverId > 3) {
            System.out.println("wrong server id as input");
            return;
        }
        boolean var = "True".equals(args[2]);
        isMaster = new AtomicBoolean(var);
        if (isMaster.get()) checkpoint_freq = Integer.parseInt(args[3]);
        else backup_id = Integer.parseInt(args[3]);
        configNum = Integer.parseInt(args[4]);
        i_am_ready = Integer.parseInt(args[5]) == 1 ? true : false;

        // setup a connectiong with replica manager
        replicaPort = Integer.parseInt(args[6]);

        // create a server object, changeStatus is false at beginning
        Server curServer = new Server();
        if (configNum == 2) {
            curServer.acceptReplica(replicaPort);
        }
        try (ServerSocket serverSocket = new ServerSocket(serverPort);) {
            System.out.println("Replica Manager port is " + replicaPort);
            System.out.println("Current server port is " + serverPort + ", name is " + name);
            System.out.println("Current server id is : " + serverId);
            System.out.println("The current server is Master ? " + isMaster);
            System.out.println("The server is :" + (i_am_ready ? "ready" : "not ready"));

            // primary server checkpoints the backups
            if (isMaster.get()) {
                curServer.sendCheckpoints(1, checkpoint_freq);
                curServer.sendCheckpoints(2, checkpoint_freq);
            }
            // backups receive checkpoints from primary server
            else {
                curServer.receiveCheckpoints(backup_ports[backup_id - 1]);
            }

            // if the server is ready, opens up two client sockets to other two servers
            if (i_am_ready) {
                for (int i = 0; i < recovery_ports.length; i++) {
                    if (i != serverId - 1) {
                        sendRecoveryMsg(recovery_ports[i], checkpoint_freq);
                    }
                }
            }
            // if the server just ecovered, receive checkpoints and re-update the states
            else {
                receiveRecoveryMsg(recovery_ports[serverId - 1], checkpoint_freq);
            }

            while (true) {
                // waits for client to connect
                Socket clientSocket = serverSocket.accept();
                ServerHandler handler = new ServerHandler(clientSocket);
                handler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void acceptReplica(int replicaPort) {

        new Thread(() -> {
            try (ServerSocket serverReplicaSocket = new ServerSocket(replicaPort);) {
                while (true) {
                    Socket clientServer = serverReplicaSocket.accept();
                    BufferedReader clientInput = new BufferedReader(new InputStreamReader(clientServer.getInputStream()));
                    String line;
                    while ((line = clientInput.readLine()) != null) {
                        System.out.println("get message from RM: " + line);
                        String[] tokens = line.split("\\s+");
                        if (tokens[0].equals("change")) {
                            //TO-DO if this server get "change" message means that it is becoming primary
                            isMaster.set(true);
                            break;
                        }
                    }
                    break;
                }

                changeStatus.set(true);
                if (sendCheckPointThread != null) {
                    sendCheckPointThread.interrupt();
                }
                if (receiveCheckPointThread != null) {
                    receiveCheckPointThread.interrupt();
                }
                changeStatus.set(false);
                System.out.println("change status: " + changeStatus);
                if (backup_id == 1)
                    sendCheckpoints(2, 3);
                else
                    sendCheckpoints(1, 3);


            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

    }


    // private synchronized void statusChange(boolean flag) {
    //     changeStatus = flag;
    // }

    // private synchronized boolean getStatus() {
    //     return changeStatus;
    // }

    private static void sendRecoveryMsg(int port, int frequency) {
        new Thread(() -> {
            while (true) {
                String line;
                try (Socket socket = new Socket("localhost", port);
                     BufferedReader in1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out1 = new PrintWriter(socket.getOutputStream(), true);
                ) {
                    printTimestamp();
                    System.out.printf("Sending recovery checkpoint to the newly added server :");
                    System.out.printf("my_state=%d %n", state);
                    out1.printf("my_state=%d %n", state);
                    break;
                } catch (IOException e) {
                    try {
                        Thread.sleep(frequency * 1000);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private static void receiveRecoveryMsg(int port, int frequency) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                // waits for primary to send checkpoint message
                Socket clientSocket = serverSocket.accept();
                System.out.println("Ready to accept recovery checkpoint messages...");
                while (true) {
                    try (BufferedReader clientInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                        String line;
                        while ((line = clientInput.readLine()) != null) {
                            printTimestamp();
                            System.out.println("Receiving checkpoint from alive servers...");
                            // Update my_state and checkpoint_count
                            state = Integer.parseInt(line.split(" ")[0].split("=", 2)[1]);
                            System.out.printf("Update to my_state=%d %n", state);

                            synchronized (Server.class) {
                                i_am_ready = true;
                            }
                            break;
                        }
                        if (i_am_ready) {
                            for (int i = 0; i < recovery_ports.length; i++) {
                                if (i != serverId - 1) {
                                    sendRecoveryMsg(recovery_ports[i], checkpoint_freq);
                                }
                            }
                            return;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

    }

    private void sendCheckpoints(int backup_id, int frequency) {
        sendCheckPointThread = new Thread(() -> {
            while (!changeStatus.get()) {
                // while (true) {
                String line;
                try (Socket socket = new Socket("localhost", backup_ports[backup_id - 1]);
                     BufferedReader in1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out1 = new PrintWriter(socket.getOutputStream(), true);
                ) {
                    try {
                        synchronized (Server.class) {
                            alive_backups.add(backup_id);
                        }
                        while (!changeStatus.get()) {
                            printTimestamp();
                            System.out.printf("Sending checkpoint to backup %d :", backup_id);
                            System.out.printf("my_state=%d checkpoint_count=%d%n", state, checkpoint_count);
                            out1.printf("my_state=%d checkpoint_count=%d%n", state, checkpoint_count);
                            line = in1.readLine();
                            if (line == null) {
                                synchronized (Server.class) {
                                    System.out.printf("Backup %d is dead %n", backup_id);
                                    alive_backups.remove(backup_id);
                                }
                                break;
                            }
                            Thread.sleep(frequency * 1000);
                            synchronized (Server.class) {
                                if (alive_backups.size() > 1) {
                                    if (prev_count == checkpoint_count) {
                                        checkpoint_count++;
                                    } else {
                                        prev_count = checkpoint_count;
                                    }
                                } else {
                                    checkpoint_count++;
                                    prev_count++;
                                }
                            }
                        }
                    } catch (InterruptedException | IOException e) {
                        return;
                    }
                } catch (IOException e) {
                    continue;
                }
            }
        });
        sendCheckPointThread.start();
    }

    private void receiveCheckpoints(int port) {
        receiveCheckPointThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                // waits for primary to send checkpoint message
                while (!changeStatus.get()) {
                    // while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Ready to accept primary server checkpoint messages...");
                    try (BufferedReader clientInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                         OutputStream clientOutput = clientSocket.getOutputStream();) {
                        String line;
                        while ((line = clientInput.readLine()) != null) {
                            printTimestamp();
                            System.out.println("Receiving checkpoint from primary server...");
                            // Update my_state and checkpoint_count
                            state = Integer.parseInt(line.split(" ", 2)[0].split("=", 2)[1]);
                            checkpoint_count = Integer.parseInt(line.split(" ", 2)[1].split("=", 2)[1]);
                            System.out.printf("Update to my_state=%d checkpoint_count=%d %n", state, checkpoint_count);
                            clientOutput.write("Accepted checkpoints \n".getBytes());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        receiveCheckPointThread.start();
    }

    private static class ServerHandler extends Thread {
        private final Socket clientSocket;
        private OutputStream out;
        private BufferedReader in;


        public ServerHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                acceptClient();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void acceptClient() throws IOException {
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            this.out = clientSocket.getOutputStream();
            String line;
            while ((line = in.readLine()) != null) {
                // all client messages are in format "clientname message"
                // retrieve the client name by splitting the line
                String[] tokens = line.split(" ", 3);
                if (tokens != null && tokens.length > 0) {
                    String client = tokens[0];
                    Integer requestNum = Integer.valueOf(tokens[1]);
                    if (!i_am_ready) high_watermark = requestNum;
                    String msg = tokens[2];

                    if (client.contains("LFD")) {
                        heartbeat(client);
                    } else {
                        receiveRequest(client, requestNum, msg);
                        // In passive replication, only master sends back the response and updates state
                        if (isMaster.get() || configNum == 1) {
                            if (i_am_ready) {
                                printState(msg);
                                sendReply(client, requestNum, msg);
                            } else {
                                msg += "alive \n";
                                out.write(msg.getBytes());
                            }
                        } else {
                            msg += "backup \n";
                            out.write(msg.getBytes());
                        }
                    }
                }
            }

        }

        private void heartbeat(String LFD) throws IOException {
            printTimestamp();
            System.out.printf("Acknowledge heartbeat from %s %n", LFD);
            // String reply = "heartbeat\n";
            String reply = String.format("RM:add replica S%d %s %d%n", serverId, isMaster, replicaPort);
            out.write(reply.getBytes());
        }

        private void printState(String msg) {
            String[] msgArr = msg.split(" ", 2);
            printTimestamp();
            synchronized (this) {
                if (msgArr.length == 2 && msgArr[1].contains("=")) {
                    // SET STATE=1
                    String[] equation = msgArr[1].split("=");
                    if ("STATE".equals(equation[0])) {
                        try {
                            state = Integer.parseInt(equation[1]);
                        } catch (Exception e) {
                            System.out.println("my_state_" + name + " = " + (++state));
                        }
                        System.out.println("my_state_" + name + " = " + state);
                    } else {
                        System.out.println("my_state_" + name + " = " + (++state));
                    }
                } else {
                    System.out.println("my_state_" + name + " = " + (++state));
                }
            }
        }

        private void receiveRequest(String client, Integer requestNum, String msg) {
            printTimestamp();
            if (i_am_ready) {
                System.out.printf("Receiving <%s, %s, request_num: %s, request> %s %n", client, name, requestNum, msg);
            } else {
                System.out.printf("Receiving <%s, %s, high_watermark_request_num: %s, request> %s %n", client, name, high_watermark, msg);
            }
        }

        private void sendReply(String client, Integer requestNum, String msg) throws IOException {
            printTimestamp();
            System.out.printf("Sending <%s, %s, request_num: %s, reply> %s %n", client, name, requestNum, msg);
            msg = requestNum + " " + msg;
            msg += "\n";
            out.write(msg.getBytes());
        }

    }

    private static void printTimestamp() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        System.out.printf("[ %s ] ", timeStamp);
    }
}
