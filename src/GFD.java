import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;


public class GFD {
    private final static int port = 8888;
    public static int member_count;
    private static Set<String> membership = new HashSet<>();

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Launching GFD ...");
            printMembers();

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
