import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;
import java.text.SimpleDateFormat;


public class GFD {
    private final static int port = 8888;
    public static int member_count;
    private static Set<String> membership = new HashSet<>();
    private static int port1;
    private static int port2;
    private static int port3;
    private static int frequency;


    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Wrong input!!! Sample input: java GFD [port1] [port2] [port3] frequency");
            return;
        }

        port1 = Integer.parseInt(args[0]);
        port2 = Integer.parseInt(args[1]);
        port3 = Integer.parseInt(args[2]);
        frequency = Integer.parseInt(args[3]);


        try(ServerSocket serverSocket = new ServerSocket(port);) {
            
            System.out.println("Launching GFD ...");
            printMembers();
            startHeartBeat(port1, frequency);
            startHeartBeat(port2, frequency);
            startHeartBeat(port3, frequency);
            while (true) {
                // waits for LFD to connect
                Socket lfdSocket = serverSocket.accept();
                GFDHandler handler = new GFDHandler(lfdSocket);
                handler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void startHeartBeat(int port, int frequency) {
        new Thread(() -> {
            while (true) {
                String line = null;
                try (Socket socket = new Socket("localhost", port);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);) {
                    try {
                        int heartbeat_count = 0;
                        while (true) {
                            out.printf("LFD Heartbeating from GFD %n");
                            line = in.readLine();
                            if (line == null){ 
                                System.out.printf("LFD at port: %d is dead %n", port);
                                System.out.println("Waiting for this LFD to re-connect");
                                break;
                            }
                            else {
                                printTimestamp();
                                System.out.printf("[%s] GFD receives heartbeat from port %s %n", heartbeat_count, line);
                            }
                            Thread.sleep(frequency * 1000);
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



    private static void printTimestamp() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        System.out.printf("[ %s ] ", timeStamp);
    }

    private static void printMembers() {
        System.out.printf("GFD: %s members ", member_count);
        for (String m : membership) {
            System.out.print(m + " ");
        }
        System.out.println();
    }

    private static class GFDHandler extends Thread {
        private final Socket lfdSocket;
        private BufferedReader in;

        public GFDHandler(Socket lfdSocket) {
            this.lfdSocket = lfdSocket;
        }

        @Override
        public void run() {
            try {
                connectLFD();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void connectLFD() throws IOException {
            this.in = new BufferedReader(new InputStreamReader(lfdSocket.getInputStream()));
            String line;

            while ((line = in.readLine()) != null) {
                // actively listening to replica status notified by LFD
                String[] tokens;
                // get LFD id
                tokens = line.split(":", 2);
                String lfd = tokens[0];
                // get command "add' or "delete"
                tokens = tokens[1].split(" ", 3);
                String cmd = tokens[0];
                // get replica id
                String server = tokens[2];
                synchronized (this) {
                    if (cmd.equalsIgnoreCase("add")) {
                        if (!membership.contains(server)) {
                            // add membership
                            membership.add(server);
                            member_count++;
                            printMembers();
                        }
                    }
                }
                synchronized (this) {
                    if (cmd.equalsIgnoreCase("delete")) {
                        if (membership.contains(server)) {
                            // add membership
                            membership.remove(server);
                            member_count--;
                            printMembers();
                        }
                    }
                }
            }
        }
    }


}
