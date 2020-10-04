import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Server extends Thread {
    private static int serverPort;
    private static String name;
    public static int state;

    public static void main(String[] args) {
        try {
            serverPort = Integer.parseInt(args[0]);
            name = args[1];
            ServerSocket serverSocket = new ServerSocket(serverPort);
            System.out.println("Current port is " + serverPort + ", name is " + name);
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
                        printState();
                        sendReply(client, requestNum, msg);
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

        private void printState() {
            printTimestamp();
            System.out.println("my_state_" + name + " = " + (++state));
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

        private void printTimestamp() {
            String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
            System.out.printf("[ %s ] ", timeStamp);
        }
    }
}
