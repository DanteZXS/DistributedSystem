import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A local fault detector runs on the same machines as the
 * server to detect whether the server is dead or not.
 * It heartbeats the server at a user input frequency.
 */
public class LFD implements Runnable {

        private final String hostname;
        private final int port;
        private Socket socket;
        private PrintWriter out;
        private BufferedReader in;
        private int freq;

        private static int heartbeat_count = 1;
        private final static String LFD_ID = "LFD1";

        public LFD (String hostname, int port, int freq) {
            this.hostname = hostname;
            this.port = port;
            this.freq = freq;
        }

        public static void main(String[] args) throws IOException {
            LFD fd = new LFD("localhost", 8818, Integer.parseInt(args[0]));
            fd.connect();
            fd.run();
            System.out.println("Server is dead");
        }

        private void connect() {
            try {
                this.socket = new Socket(this.hostname, this.port);
                this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                this.out = new PrintWriter(socket.getOutputStream(), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    printTimestamp();
                    System.out.printf("[%s] %s sending heartbeat to S1 %n", heartbeat_count, LFD_ID);

                    out.printf("%s Heartbeating %n", LFD_ID);

                    String line = in.readLine();
                    if (line == null) break;
                    else {
                        printTimestamp();
                        System.out.printf("[%s] %s receives heartbeat from S1 %n", heartbeat_count, LFD_ID);
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


