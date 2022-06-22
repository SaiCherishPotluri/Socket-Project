import java.io.*;
import java.net.*;
import java.util.*;

public class DHTPeerTest {
	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);

		System.out.println("Enter Server Hostname and Port Number:");
		String hostname = scan.nextLine();
		int port = Integer.parseInt(scan.nextLine());

		try {
			InetAddress address = InetAddress.getByName(hostname);
			DatagramSocket socket = new DatagramSocket();

			while (true) {
				System.out.println("Enter the command:");
				String input = scan.nextLine();
//				System.out.println(input);
//				System.out.println(input.length());
				DatagramPacket request = new DatagramPacket(input.getBytes(), input.length(), address, port);
				socket.send(request);

				byte[] buffer = new byte[10000];
				DatagramPacket response = new DatagramPacket(buffer, buffer.length);
				socket.receive(response);

				ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
				ObjectInputStream ois = new ObjectInputStream(bais);
				String responseStatus = (String) ois.readObject();
				System.out.println("Server returned: "+responseStatus);

				if(responseStatus.startsWith(Constants.FAILURE_MESSAGE)){
					if(input.startsWith("register")) {
						System.out.println("Failed Registration of User");
					}else if(input.startsWith("leave-dht")) {
						System.out.println("Failed leave-dht");
					}else if(input.startsWith("join-dht")) {
						System.out.println("Failed join-dht");
					}else if(input.startsWith("teardown-dht")) {
						System.out.println("Failed teardown-dht");
					}else if(input.startsWith("deregister")) {
						System.out.println("Failed deregister");
					}
				}else{
					if(input.startsWith("register")) {
						System.out.println("Successful Registration of User");
					}else if(input.startsWith("leave-dht")) {
						System.out.println("Successfully executed leave-dht");
					}else if(input.startsWith("join-dht")) {
						System.out.println("Successfully executed join-dht");
					}else if(input.startsWith("teardown-dht")) {
						System.out.println("Successfully executed teardown-dht");
					}else if(input.startsWith("deregister")) {
						System.out.println("De-registered the user successfully");
					}
				}
				Thread.sleep(10);
			}

		} catch (SocketTimeoutException ex) {
			System.out.println("Timeout error: " + ex.getMessage());
			ex.printStackTrace();
		} catch (IOException ex) {
			System.out.println("Client error: " + ex.getMessage());
			ex.printStackTrace();
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
