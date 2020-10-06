import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Client {
    private final String hostname;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private String name;


    public Client (String hostname, int port, String name) {
        this.hostname = hostname;
        this.port = port;
        this.name = name;
    }

    public static void main(String[] args) throws IOException {
        Client client = new Client("localhost", 8818, args[0]);
        client.connect();
        client.chat();
        client.socket.close();
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

   private void chat() throws IOException {
        String line = "";
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        while ( (line = stdIn.readLine()) != null) {
            if (line.equals("quit")) {
                break;
            }
            // send request to the server and then prints it in console
            sendRequest(line);
            // print the reply from server
            receiveReply();
        }
    }

    private void sendRequest(String line) {
        out.println(this.name + " " +line);
        printTimestamp();
        System.out.printf("Sent <%s, S1, request> %s %n", this.name, line);
    }
    private void receiveReply() throws IOException {
        printTimestamp();
        System.out.printf("Received <%s, S1, reply> %s %n", this.name, in.readLine());
    }

    private void printTimestamp() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        System.out.printf("[ %s ] ", timeStamp);
    }
}
