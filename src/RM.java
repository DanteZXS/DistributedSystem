import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RM {

    private static final int port = 2019;
    private static int member_count = 0;
    private static Set<String> membership = new HashSet<>();
    private static Map<String, Integer> addressMap = new HashMap<>();
    private static Map<String, Integer> backupIdMap = new HashMap<>();
    private static String primaryServer;
    private static final String HOST_NAME = "localhost";
    private static String config;
    private static boolean autoMode = false;
    private static int curEmptyBackupId;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Wrong Format!!");
            System.out.println("Use(A active P passive): Java RM auto <A or P>");
            System.out.println("Use(A active P passive): Java RM <A or P>");
        }
        if (args[0].toLowerCase().equals("auto")) { 
            autoMode = true;
            config = args[1];
        } else {
            config = args[0];
        }
        
        try(ServerSocket serverSocket = new ServerSocket(port);) {
            System.out.println("Launching RM ...");
            printMembers();
            while (true) {
                // waits for LFD to connect
                startConnecting(serverSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void fillMap(String serverId, int rmPort, boolean isMaster, int backup_id) {
        if (isMaster) {
            if (!serverId.equals(primaryServer)) {
                primaryServer = serverId;
                if (config.equals("P")) {
                    printPrimary();
                }
            }
        }
        if (!addressMap.containsKey(serverId)) {
            addressMap.put(serverId, rmPort);
        }

        if (backup_id != 0) {
            backupIdMap.put(serverId, backup_id);
        } else {
            backupIdMap.remove(serverId);
        }

    }

    private static void printPrimary() {
        System.out.println("current primary: " + primaryServer);
    }



    private static void selectNewPrimary(String server) {
        if (primaryServer.equals(server) && config.equals("P")) {
            if (addressMap.size() == 0) {
                return;
            }
            String primaryElect;
            int serverPort;
            for (Map.Entry<String, Integer> entry: addressMap.entrySet()) {
                primaryElect = entry.getKey();
                serverPort = entry.getValue();
                primaryServer = primaryElect;
                printPrimary();
                System.out.println("server port need to send change: "+ serverPort);
                sendChange(serverPort);
                curEmptyBackupId = backupIdMap.get(primaryServer);
                backupIdMap.remove(primaryServer);
                return;
            }

        }
    }


    private static void sendChange(int serverPort) {

        try(Socket socket = new Socket(HOST_NAME, serverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);) {

            out.println("change");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startConnecting(ServerSocket serverSocket) {
        try {
            Socket gfdSocket = serverSocket.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(gfdSocket.getInputStream()));
            String line;

            while ((line = in.readLine()) != null) {
                // actively listening to replica status notified by LFD
                String[] tokens;
                // get LFD id
                tokens = line.split(":", 2);
                String lfd = tokens[0];

                if (lfd.equals("RM")) {
                    // System.out.println("msg: " + line);
                    tokens = tokens[1].split("\\s+");
                    String serverId = tokens[2];
                    boolean isMaster = tokens[3].equals("true") ? true: false;
                    int rmPort = Integer.parseInt(tokens[4]);
                    int backup_id = Integer.parseInt(tokens[5]);
                    fillMap(serverId, rmPort, isMaster, backup_id);
                    continue;
                }

                // get command "add' or "delete"
                tokens = tokens[1].split(" ", 3);
                String cmd = tokens[0];
                // get replica id
                String server = tokens[2];

                if (cmd.equals("add")) {
                    if (!membership.contains(server)) {
                        // add membership
                        membership.add(server);
                        member_count++;
                        printMembers();
                    }
                }

                if (cmd.equals("delete")) {
                    if (membership.contains(server)) {
                        // add membership
                        membership.remove(server);
                        member_count--;
                        printMembers();
                        addressMap.remove(server);
                        selectNewPrimary(server);
                        // if in auto mode, recover dead replica

                        if (autoMode) {
                            // Runtime.getRuntime().exec("javac Server.java");
                            System.out.println("execute Server: " + server);
                            if (config.equals("A")) {
                                Runtime.getRuntime().exec("java Server " +  server + " " + config + " 3 2");
                            } else {
                                if (!backupIdMap.containsKey(server)) {
                                    // System.out.println(String.format("bring back %s as B%d%n", server, curEmptyBackupId));
                                    Runtime.getRuntime().exec("java Server " +  server + " " + "B" + curEmptyBackupId + " 3 1");
                                } else {
                                    int id = backupIdMap.get(server);
                                    // System.out.println(String.format("bring back %s as B%d%n", server, id));
                                    Runtime.getRuntime().exec("java Server " +  server + " " + "B" + id + " 3 1");
                                }
                            }
                            System.out.println("successfully execute");
                        }

                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private static void printMembers() {
        System.out.printf("RM: %s members ", member_count);
        for (String m : membership) {
            System.out.print(m + " ");
        }
        System.out.println();
    }
}