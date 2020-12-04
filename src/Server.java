import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.awt.event.WindowListener;
import java.awt.event.WindowAdapter;

import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

public class Server extends Thread {
    private static int serverPort;
    private static String name;
    private static int state;
    private static boolean isMaster;
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

    private final static int[] RM_ports = {666, 665, 664};
    private final static int[] server_ports = {1234, 1235, 1236};

    private static Set<Integer> alive_backups = new HashSet<>();

    private static AtomicBoolean i_am_ready;
    private static int serverId;
    private static int high_watermark;

    private Thread sendCheckPointThread;
    private Thread receiveCheckPointThread;

    private AtomicBoolean changeStatus;

    private AtomicBoolean changeStatusReceive;

    private ServerSocket receiveServerSocket;

    private static TextArea textArea;

    /**
     * Each active server will open up two TCP connections as a client socket to the other
     * two active servers; when a server dead and recovers, it opens up the a server socket to receive
     * checkpoints from the other alive servers; after it is updated to the correct states, it re-opens two
     * client sockets.
     */
    private final static int[] recovery_ports = {601, 602, 603};

    public Server(String[] args) {
        changeStatus = new AtomicBoolean(false);
        changeStatusReceive = new AtomicBoolean(false);
        i_am_ready = new AtomicBoolean(false);
        launchServer(args);
    }


    public static void main(String[] args) {
        if (args[0].equalsIgnoreCase("-h")) {
            // print how to use the program
            System.out.println("<server_name> <server config > <checkpoint_freq> <# of the same server kind>");
            System.out.println("server config - A : active ; P : passive (primary); B<id> : passive (backup 1 or 2) ");
            return;
        }
        if (args.length != 4) {
            System.out.println("Wrong Input!!!");
            return;
        }
        // create a server object, changeStatus is false at beginning
        new Server(args);
    }

    public void launchServer(String[] args) {
        name = args[0];
        serverId = Integer.parseInt(name.replaceAll("[\\D]", ""));
        if (serverId > 3) {
            System.out.println("wrong server id as input");
            return;
        }
        serverPort = server_ports[serverId - 1];

        // check config and backup id
        if (args[1].contains("A")) {
            configNum = 1;
            isMaster = true;
        } else {
            configNum = 2;
            if (args[1].contains("P")) {
                isMaster = true;
            } else {
                isMaster = false;
                backup_id = Integer.parseInt(args[1].replaceAll("[\\D]", ""));
                if (!(backup_id == 1 || backup_id == 2)) {
                    System.out.println("wrong backup id");
                    return;
                }
            }
        }
        // create gui
        createGUI();

        checkpoint_freq = Integer.parseInt(args[2]);
        boolean val = Integer.parseInt(args[3]) == 1 ? true : false;
        i_am_ready.set(val);
        // setup a connectiong with replica manager
        replicaPort = RM_ports[serverId - 1];

        if (configNum == 2) {
            this.acceptReplica(replicaPort);
        }

        try (ServerSocket serverSocket = new ServerSocket(serverPort);) {
            System.out.println("Replica Manager port is " + replicaPort);
            System.out.println("Current server port is " + serverPort + ", name is " + name);
            System.out.println("Current server id is : " + serverId);
            System.out.println("The current server is Master ? " + isMaster);
            System.out.println("The server is :" + (i_am_ready.get() ? "ready" : "not ready"));

            // primary server checkpoints the backups
            if (configNum == 2) {
                if (isMaster) {
                    this.sendCheckpoints(1, checkpoint_freq);
                    this.sendCheckpoints(2, checkpoint_freq);
                }
                // backups receive checkpoints from primary server
                else {
                    this.receiveCheckpoints(backup_ports[backup_id - 1]);
                }
            }

            // if the server is ready, opens up two client sockets to other two servers
            if (i_am_ready.get()) {
                for (int i = 0; i < recovery_ports.length; i++) {
                    if (i != serverId - 1) {
                        sendRecoveryMsg(recovery_ports[i], checkpoint_freq);
                    }
                }
            }
            // if the server just recovered, receive checkpoints and re-update the states
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
                            isMaster = true;
                            break;
                        }
                    }
                    break;
                }

                changeStatus.set(true);
                changeStatusReceive.set(true);
                if (sendCheckPointThread != null) {
                    sendCheckPointThread.interrupt();
                }
                if (receiveCheckPointThread != null) {
                    receiveCheckPointThread.interrupt();
                }


                changeStatus.set(false);
                System.out.println("change status: " + changeStatus);

                if (receiveServerSocket != null) {
                    receiveServerSocket.close();
                }


                sendCheckpoints(2, checkpoint_freq);
                sendCheckpoints(1, checkpoint_freq);



            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

    }



    private static void sendRecoveryMsg(int port, int frequency) {

        new Thread(() -> {
            while (i_am_ready.get()) {
                String line;
                try (Socket socket = new Socket("localhost", port);
                     BufferedReader in1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out1 = new PrintWriter(socket.getOutputStream(), true);
                ) {
                    // printTimestamp();
                    // System.out.printf("Sending recovery checkpoint to the newly added server :");
                    // System.out.printf("my_state=%d %n", state);
                    out1.printf("my_state=%d %n", state);

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
                while (!i_am_ready.get()){

                    try (BufferedReader clientInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                        String line;
                        while ((line = clientInput.readLine()) != null) {
                            printTimestamp();
                            System.out.println("Receiving checkpoint from alive servers...");
                            // Update my_state and checkpoint_count
                            state = Integer.parseInt(line.split(" ")[0].split("=", 2)[1]);
                            System.out.printf("Update to my_state=%d %n", state);

                            // synchronized (Server.class) {
                                i_am_ready.set(true);
                            // }
                            break;
                        }
                        System.out.println("I am ready state: " + i_am_ready);

                        for (int i = 0; i < recovery_ports.length; i++) {
                            if (i != serverId - 1) {
                                sendRecoveryMsg(recovery_ports[i], checkpoint_freq);
                            }
                        }
                        return;

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
                        while (true) {
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
                while (!changeStatusReceive.get()) {
                    receiveServerSocket = serverSocket;
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
                        // e.printStackTrace();
                        return;
                    }


                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
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
                    if (!i_am_ready.get()) high_watermark = requestNum;
                    String msg = tokens[2];

                    if (client.contains("LFD")) {
                        heartbeat(client);
                    } else {
                        receiveRequest(client, requestNum, msg);
                        // In passive replication, only master sends back the response and updates state
                        if (isMaster || configNum == 1) {
                            if (i_am_ready.get()) {
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

        private synchronized void printState(String msg) {
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

        private synchronized void receiveRequest(String client, Integer requestNum, String msg) {
            printTimestamp();
            if (i_am_ready.get()) {
                System.out.printf("Receiving <%s, %s, request_num: %s, request> %s %n", client, name, requestNum, msg);
            } else {
                System.out.printf("Receiving <%s, %s, high_watermark_request_num: %s, request> %s %n", client, name, high_watermark, msg);
            }
        }

        private synchronized void sendReply(String client, Integer requestNum, String msg) throws IOException {
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

    // overwrite the write() methods of OutputStream to append the text to the text pane instead
    private static void updateTextArea(final String text) {
        SwingUtilities.invokeLater(() -> textArea.append(text));
    }

    /** This method refers to :
     * http://unserializableone.blogspot.com/2009/01/redirecting-systemout-and-systemerr-to.html*/
    private static void redirectSystemStreams() {
        OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                updateTextArea(String.valueOf((char) b));
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                updateTextArea(new String(b, off, len));
            }

            @Override
            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }
        };

        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(out, true));
    }

    private static void createGUI() {
        //Create and set up the window.
        JFrame frame = new JFrame("Server" + serverId);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        textArea = new TextArea();
        redirectSystemStreams();
        //Add contents to the window.
        frame.add(textArea);

        //Display the window.
        frame.pack();
        frame.setVisible(true);
    }
}
