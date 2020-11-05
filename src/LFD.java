import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A local fault detector runs on the same machines as the
 * server to detect whether the server is dead or not.
 * It heartbeats the server at a user input frequency.
 * <p>
 * Start the LFD by inputing:
 * java LFD <port_number> <heartbeat_freq> <LFD_ID>
 */
public class LFD implements Runnable {

    private final String hostname;
    private final int port;
    private Socket socket;
    private static Socket gfdSocket;
    private PrintWriter out;
    private BufferedReader in;
    private static PrintWriter gfdOut;
    private int freq;

    private static int heartbeat_count = 1;
    private static String LFD_ID;
    private static String SERVER_ID;


    public LFD(String hostname, int port, int freq, String id) {
        this.hostname = hostname;
        this.port = port;
        this.freq = freq;
        this.LFD_ID = id;
        int id_num = Integer.parseInt(id.replaceAll("[\\D]", ""));
        this.SERVER_ID = "S" + id_num;
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            System.out.println("Wrong Input!!! Sample Input: java Server [port to server] [frequency to heartbeat server] [LFD id] [port to accept GFD heartbeat]");
            return;
        }

        // user-defined port number and heartbeat frequency
        LFD fd = new LFD("localhost", Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);

        // port of lfd that can be used by GFD, this server socket is used for accepting heatbead from GFD
        new Thread(() -> {

            try (ServerSocket serverSocket = new ServerSocket(Integer.parseInt(args[3]))) {
                // waits for client to connect
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Accepting GFD heatbeat connection...");
                    new Thread(() -> {
                        try (BufferedReader clientInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                             OutputStream clientOutput = clientSocket.getOutputStream();) {

                            String line;
                            while ((line = clientInput.readLine()) != null) {
                                String msg = args[3] + "\n";
                                clientOutput.write(msg.getBytes());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();


        // notify GFD
        gfdSocket = new Socket("localhost", 8888);
        gfdOut = new PrintWriter(gfdSocket.getOutputStream(), true);

        while (true) {
            fd.connect();
            gfdOut.printf("%s:add replica %s%n", LFD_ID, SERVER_ID);
            fd.run();
            System.out.println(SERVER_ID + " is dead");
            gfdOut.printf("%s:delete replica %s%n", LFD_ID, SERVER_ID);
            System.out.println("Waiting for server to re-connect");
        }
    }

    private void connect() {
        while (true) {
            try {
                this.socket = new Socket(this.hostname, this.port);
                this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                this.out = new PrintWriter(socket.getOutputStream(), true);
                break;
            } catch (IOException e) {
                continue;
            }
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                printTimestamp();
                System.out.printf("[%s] %s sending heartbeat to %s %n", heartbeat_count, LFD_ID, SERVER_ID);

                out.printf("LFD %s Heartbeating %n", LFD_ID);

                String line = in.readLine();
                if (line == null) break;
                else {
                    printTimestamp();
                    System.out.printf("[%s] %s receives heartbeat from %s %n", heartbeat_count, LFD_ID, SERVER_ID);
                }
                heartbeat_count++;
                Thread.sleep(this.freq * 1000);
            }
        } catch (InterruptedException | IOException e) {
            return;
        }
    }

    private void printTimestamp() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        System.out.printf("[ %s ] ", timeStamp);
    }


}
