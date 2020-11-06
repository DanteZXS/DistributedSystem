import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Server extends Thread {
    private static int serverPort;
    private static String name;
    private static int state;
    private static boolean isMaster;

    /** The following section is for passive replication */
    private static int checkpoint_freq;
    private static int backup_id;
    private static int checkpoint_count = 1;
    private static int prev_count = 1;
    private final static int[] backup_ports = {600, 601};
    private static Set<Integer> alive_backups = new HashSet<>();



    public static void main(String[] args) {
        if (args[0].equalsIgnoreCase("-h")) {
            // print how to use the program
            System.out.println("If launching the primary server:");
            System.out.println("<heartbeat_port> <server_name> True <checkpoint_freq>");
            System.out.println("If launching the backup server:");
            System.out.println("<heartbeat_port> <server_name> False <id (either 0 or 1)>");
            return;
        }
        if (args.length != 4) {
            System.out.println("Wrong Input!!!");
            return;
        }
        try {
            serverPort = Integer.parseInt(args[0]);
            name = args[1];
            isMaster = "True".equals(args[2]);
            if (isMaster) checkpoint_freq = Integer.parseInt(args[3]);
            else backup_id = Integer.parseInt(args[3]);
            ServerSocket serverSocket = new ServerSocket(serverPort);
            System.out.println("Current port is " + serverPort + ", name is " + name);
            System.out.println("The current server is Master ? " + isMaster);

            // primary server checkpoints the backups
            if (isMaster ) {
                sendCheckpoints(1, checkpoint_freq);
                sendCheckpoints(2, checkpoint_freq);
            }
            // backups receive checkpoints from primary server
            else {
                receiveCheckpoints(backup_ports[backup_id-1]);
            }

            while (true) {
                // waits for client to connect
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepting client connection...");
                ServerHandler handler = new ServerHandler(clientSocket);
                handler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendCheckpoints(int backup_id, int frequency) {
        new Thread(() -> {
            while (true) {
                String line;
                try (Socket socket = new Socket("localhost", backup_ports[backup_id-1]);
                     BufferedReader in1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out1 = new PrintWriter(socket.getOutputStream(), true);
                ) {
                    try {
                        synchronized (Server.class) {
                            alive_backups.add(backup_id);
                        }
                        while (true) {
                            printTimestamp();
                            System.out.printf("Sending checkpoint to backup %d :", backup_id);
                            System.out.printf("my_state=%d checkpoint_count=%d%n", state, checkpoint_count);
                            out1.printf("my_state=%d checkpoint_count=%d%n", state, checkpoint_count);
                            line = in1.readLine();
                            if (line == null){
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
                } catch(IOException e) {
                    continue;
                }
            }
        }).start();
    }

    private static void receiveCheckpoints(int port) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                // waits for primary to send checkpoint message
                while (true) {
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
        }).start();
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
                    String msg = tokens[2];

                    if (client.contains("LFD")) {
                        heartbeat(client);
                    } else {
                        receiveRequest(client, requestNum, msg);
                        // In passive replication, only master sends back the response and updates state
                        if (isMaster) {
                            printState(msg);
                            sendReply(client, requestNum, msg);
                        }
                    }
                }
            }

        }

        private void heartbeat(String LFD) throws IOException {
            printTimestamp();
            System.out.printf("Acknowledge heartbeat from %s %n", LFD);
            String reply = "heartbeat\n";
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
            System.out.printf("Receiving <%s, %s, request_num: %s, request> %s %n", client, name, requestNum, msg);
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