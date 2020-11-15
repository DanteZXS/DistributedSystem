
import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

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

    // Indicate which config the server is using
    public int configNum;

    private static Client client;
//    private boolean alive[] = {true, true, true};

    public Client(String hostname, String name, int configNum) {
        this.hostname = hostname;
        this.name = name;
        this.configNum = configNum;
    }

    public static void main(String[] args) throws IOException {
        client = new Client("localhost", args[0], Integer.parseInt(args[1]));
//        client.connect();
        client.chat();
    }

    private void connect() {
        try {
            // Connect to the 1st server
            this.socket1 = new Socket(this.hostname, this.port1);
            this.in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
            this.out1 = new PrintWriter(socket1.getOutputStream(), true);
            // Connect to the 2nd server
            this.socket2 = new Socket(this.hostname, this.port2);
            this.in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
            this.out2 = new PrintWriter(socket2.getOutputStream(), true);
            // Connect to the 3rd server
            this.socket3 = new Socket(this.hostname, this.port3);
            this.in3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
            this.out3 = new PrintWriter(socket3.getOutputStream(), true);
        } catch (IOException e) {
        }
    }

    private void chat() throws IOException {
        String line = "";
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        while ((line = stdIn.readLine()) != null) {
            client.connect();
            if (line.equals("quit")) {
                break;
            }
            // send request to the server and then prints it in console
            sendRequest((Client.requestNum++) + " " + line, out1, out2, out3);
            // print the reply from server
            receiveReply(in1, in2, in3);
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
        if (configNum == 1) {
            // Active replication
            // Duplicate detection
            boolean flag = true;
            // Get the reply from the 1st Server
            try {
                printTimestamp();
                String[] msgArr1 = in1.readLine().split(" ", 2);
                Integer requestNum1 = Integer.valueOf(msgArr1[0]);
                String msg1 = msgArr1[1];
                if (flag) {
                    System.out.printf("Received <%s, S1, request_num = %s, reply> %s %n", this.name, requestNum1, msg1);
                    flag = false;
                } else {
                    System.out.printf("[DISCARDED] Received <%s, S1, request_num = %s, reply> %s %n", this.name, requestNum1, msg1);
                }
            } catch (Exception e) {
                System.out.println("Looks like S1 is dead");
            }
            // Get the reply from the 2nd Server
            try {
                printTimestamp();
                String[] msgArr2 = in2.readLine().split(" ", 2);
                Integer requestNum2 = Integer.valueOf(msgArr2[0]);
                String msg2 = msgArr2[1];
                if (flag) {
                    System.out.printf("Received <%s, S2, request_num = %s, reply> %s %n", this.name, requestNum2, msg2);
                    flag = false;
                } else {
                    System.out.printf("[DISCARDED] Received <%s, S2, request_num = %s, reply> %s %n", this.name, requestNum2, msg2);
                }
            } catch (Exception e) {
                System.out.println("Looks like S2 is dead");
            }

            // Get the reply from the 3rd Server
            try {
                printTimestamp();
                String[] msgArr3 = in3.readLine().split(" ", 2);
                Integer requestNum3 = Integer.valueOf(msgArr3[0]);
                String msg3 = msgArr3[1];
                if (flag) {
                    System.out.printf("Received <%s, S3, request_num = %s, reply> %s %n", this.name, requestNum3, msg3);
                    flag = false;
                } else {
                    System.out.printf("[DISCARDED] Received <%s, S3, request_num = %s, reply> %s %n", this.name, requestNum3, msg3);
                }
            } catch (Exception e) {
                System.out.println("Looks like S3 is dead");
            }
        } else {
            // Passive replication
            boolean isPrimary = false;
            // How to identify primary server? The message won't end with the word "alive"

            // Check the first server
            String incomingMessage1 = in1.readLine();
            isPrimary = !incomingMessage1.endsWith("backup ");
            if (isPrimary) {
                printTimestamp();
                String[] msgArr1 = incomingMessage1.split(" ", 2);
                Integer requestNum1 = Integer.valueOf(msgArr1[0]);
                String msg1 = msgArr1[1];
                System.out.printf("Received <%s, S1, request_num = %s, reply> %s %n", this.name, requestNum1, msg1);
                return;
            }

            // Check the second server
            String incomingMessage2 = in2.readLine();
            isPrimary = !incomingMessage2.endsWith("backup ");
            if (isPrimary) {
                printTimestamp();
                String[] msgArr2 = incomingMessage2.split(" ", 2);
                Integer requestNum2 = Integer.valueOf(msgArr2[0]);
                String msg2 = msgArr2[1];
                System.out.printf("Received <%s, S2, request_num = %s, reply> %s %n", this.name, requestNum2, msg2);
                return;
            }

            // Check the third server
            String incomingMessage3 = in3.readLine();
            isPrimary = !incomingMessage3.endsWith("backup ");
            if (isPrimary) {
                printTimestamp();
                String[] msgArr3 = incomingMessage3.split(" ", 2);
                Integer requestNum3 = Integer.valueOf(msgArr3[0]);
                String msg3 = msgArr3[1];
                System.out.printf("Received <%s, S3, request_num = %s, reply> %s %n", this.name, requestNum3, msg3);
            }
        }
    }

    private void printTimestamp() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        System.out.printf("[ %s ] ", timeStamp);
    }
}