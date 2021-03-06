
import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Client {
    private final String hostname;
    private String name;
    // Connection variables with the 1st Server
    private static Socket socket1;
    private static int port1 = 1234;
    private static PrintWriter out1;
    private static BufferedReader in1;
    // Connection variables with the 2nd Server
    private static Socket socket2;
    private static int port2 = 1235;
    private static PrintWriter out2;
    private static BufferedReader in2;
    // Connection variables with the 3rd Server
    private static Socket socket3;
    private int port3 = 1236;
    private static PrintWriter out3;
    private static BufferedReader in3;

    public static int requestNum;

    private static Client client;

    private static boolean autoMode = false;
    private static int messageFreq = 0;
    private static final Random RAND = new Random();

    public Client(String hostname, String name) {
        this.hostname = hostname;
        this.name = name;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        client = new Client("localhost", args[0]);
        if (args.length > 1 && args[1].toLowerCase().equals("auto")) {
            System.out.println("Client is in auto mode");
            autoMode = true;
            messageFreq = Integer.parseInt(args[2]) * 1000;
        }
        client.connect();
        client.chat();
    }

    private void connect() {
        try {
            // Connect to the 1st server
            if (!available(port1)) {
                this.socket1 = new Socket(this.hostname, this.port1);
                this.in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                this.out1 = new PrintWriter(socket1.getOutputStream(), true);
            }
            // Connect to the 2nd server
            if (!available(port2)) {
                this.socket2 = new Socket(this.hostname, this.port2);
                this.in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                this.out2 = new PrintWriter(socket2.getOutputStream(), true);
            }
            // Connect to the 3rd server
            if (!available(port3)) {
                this.socket3 = new Socket(this.hostname, this.port3);
                this.in3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
                this.out3 = new PrintWriter(socket3.getOutputStream(), true);
            }
        } catch (IOException e) {
        }
    }

    public static boolean available(int port) {

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }

    private void chat() throws IOException, InterruptedException {
        String line = "";
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        while (autoMode || (line = stdIn.readLine()) != null) {
            client.connect();
            if (line.equals("quit")) {
                break;
            }
            if (autoMode) line = Integer.toString(RAND.nextInt(100));
            // send request to the server and then prints it in console
            sendRequest((Client.requestNum++) + " " + line, out1, out2, out3);
            // print the reply from server
            receiveReply(in1, in2, in3);
            if (autoMode) {
                Thread.sleep(messageFreq);
            }
        }
    }

    private void sendRequest(String line, PrintWriter out1, PrintWriter out2, PrintWriter out3) {
        String[] msgArr = line.split(" ", 2);
        Integer requestNum = Integer.valueOf(msgArr[0]);
        String msg = msgArr[1];
        // Send message to the 1st Server
        out1.println(this.name + " " + line);
        printTimestamp();
        System.out.printf("Sent <%s, S1, request_num = %s, request> %s %n", this.name, requestNum, msg);
        // Send message to the 2nd Server
        out2.println(this.name + " " + line);
        printTimestamp();
        System.out.printf("Sent <%s, S2, request_num = %s, request> %s %n", this.name, requestNum, msg);
        // Send message to the 3rd Server
        out3.println(this.name + " " + line);
        printTimestamp();
        System.out.printf("Sent <%s, S3, request_num = %s, request> %s %n", this.name, requestNum, msg);
    }

    private void receiveReply(BufferedReader in1, BufferedReader in2, BufferedReader in3) throws IOException {
        // Duplicate detection
        boolean flag = true;
        // Get the reply from the 1st Server
        try {
                String[] msgArr1 = in1.readLine().split(" ", 2);
                Integer requestNum1 = Integer.valueOf(msgArr1[0]);
                String msg1 = msgArr1[1];
                if (flag) {
                    printTimestamp();
                    System.out.printf("Received <%s, S1, request_num = %s, reply> %s %n", this.name, requestNum1, msg1);
                    flag = false;
                } else {
                    printTimestamp();
                    System.out.printf("[DISCARDED] Received <%s, S1, request_num = %s, reply> %s %n", this.name, requestNum1, msg1);
            }
        } catch (Exception e) {
//            System.out.println("Looks like S1 is dead");
        }


            try {

                    String[] msgArr2 = in2.readLine().split(" ", 2);
                    Integer requestNum2 = Integer.valueOf(msgArr2[0]);
                    String msg2 = msgArr2[1];
                    if (flag) {
                        printTimestamp();
                        System.out.printf("Received <%s, S2, request_num = %s, reply> %s %n", this.name, requestNum2, msg2);
                        flag = false;
                    } else {
                        printTimestamp();
                        System.out.printf("[DISCARDED] Received <%s, S2, request_num = %s, reply> %s %n", this.name, requestNum2, msg2);
                    }
            } catch (Exception e) {
//                System.out.println("Looks like S2 is dead");
            }

            // Get the reply from the 3rd Server
            try {

                    String[] msgArr3 = in3.readLine().split(" ", 2);
                    Integer requestNum3 = Integer.valueOf(msgArr3[0]);
                    String msg3 = msgArr3[1];
                    if (flag) {
                        printTimestamp();
                        System.out.printf("Received <%s, S3, request_num = %s, reply> %s %n", this.name, requestNum3, msg3);
                        flag = false;
                    } else {
                        printTimestamp();
                        System.out.printf("[DISCARDED] Received <%s, S3, request_num = %s, reply> %s %n", this.name, requestNum3, msg3);
                    }
            } catch (Exception e) {
//                System.out.println("Looks like S3 is dead");
            }
    }

    private void printTimestamp() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        System.out.printf("[ %s ] ", timeStamp);
    }
}