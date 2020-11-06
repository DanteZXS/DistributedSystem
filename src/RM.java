import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class RM {

    private static final int port = 2019;
    private static int member_count = 0;
    private static Set<String> membership = new HashSet<>();
    
    public static void main(String[] args) {
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