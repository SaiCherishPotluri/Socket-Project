import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DHTServer {
    private DatagramSocket socket;
    private Map<String, PeerNode> peerNodes = new HashMap<String, PeerNode>();
    private PeerNode leavingPeerNode;
    private PeerNode joiningPeerNode;

    public DHTServer(int port) throws SocketException {
        socket = new DatagramSocket(port);
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Scanner scan = new Scanner(System.in);
        System.out.println("Enter Server Port:");
        int port = scan.nextInt();
        try {
            DHTServer server = new DHTServer(port);
            server.service();
        } catch (SocketException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        scan.close();
    }

    private void service() throws IOException {
        while (true) {
            DatagramPacket request = new DatagramPacket(new byte[48], 48);
            socket.receive(request);

            InetAddress clientAddress = request.getAddress();
            int clientPort = request.getPort();
            System.out.println(clientAddress);
            System.out.println(clientPort);

            String input = new String(request.getData());
            String[] inputParams = input.split(" ");

            List<PeerNode> dhtPeerNodes = new ArrayList<>();
            boolean success = performTask(inputParams, dhtPeerNodes);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            if (success) {
                oos.writeObject(Constants.SUCCESS_MESSAGE);
            } else {
                oos.writeObject(Constants.FAILURE_MESSAGE);
            }

            if (dhtPeerNodes.size() > 0) {
                oos.writeObject(dhtPeerNodes);
            }

            byte[] dhtPeerNodesByte = bos.toByteArray();

            DatagramPacket response = new DatagramPacket(dhtPeerNodesByte, dhtPeerNodesByte.length,
                    clientAddress, clientPort);
//            System.out.println(dhtPeerNodesByte.length);
            System.out.println("Sending the response");
            socket.send(response);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean performTask(String[] inputParams, List<PeerNode> dhtPeerNodes) {
        String taskName = inputParams[0].trim();
        boolean success = false;
        switch (taskName) {
            case "register":
                success = registerUser(inputParams[1], inputParams[2],
                        Integer.parseInt(inputParams[3].trim()), Integer.parseInt(inputParams[4].trim()),
                        Integer.parseInt(inputParams[5].trim()));
                break;
            case "setup-dht":
                success = setUpDHT(Integer.parseInt(inputParams[1].trim()), inputParams[2].trim(), dhtPeerNodes);
                break;
            case "dht-complete":
                success = dhtComplete(inputParams[1].trim());
                break;
            case "query-dht":
                success = queryDHT(inputParams[1].trim(), dhtPeerNodes);
                break;
            case "leave-dht":
                success = leaveDHT(inputParams[1].trim());
                sendCommandToLeavingNode(leavingPeerNode);
                break;
            case "dht-rebuilt":
                if (inputParams[3].trim().contains("leave-dht")) {
                    success = dhtRebuiltStepsForLeavingNode(inputParams[1].trim(), inputParams[2].trim());
                } else if (inputParams[3].trim().contains("join-dht")) {
                    success = dhtRebuiltStepsForJoiningNode(inputParams[1].trim(), inputParams[2].trim());
                }
                System.out.println("dht-rebuilt complete");
                break;
            case "join-dht":
                success = joinDHT(inputParams[1].trim());
                sendCommandToJoiningAndLeaderNode();
                break;
            case "deregister":
                success = deregisterUser(inputParams[1].trim());
                break;
            case "teardown-dht":
                success = teardownDHT(inputParams[1].trim());
                break;
            case "teardown-complete":
                success = teardownComplete(inputParams[1].trim());
                break;
            case "kill-server":
                System.out.println("Terminating the Server");
                System.exit(0);
                break;
            default:
                System.out.println("Invalid Task");
                break;
        }
        return success;
    }

    private boolean dhtComplete(String leaderUsername) {
        PeerNode leaderNode = peerNodes.get(leaderUsername);
        if (leaderNode.getState() != State.LEADER) {
            return false;
        } else {
            System.out.println("DHT Setup is Complete. Start Querying");
            return true;
        }
    }

    private boolean teardownComplete(String leaderUsername) {
        PeerNode leaderNode = peerNodes.get(leaderUsername);
        if (leaderNode.getState() != State.LEADER) {
            return false;
        } else {
            System.out.println("Teardown Complete Triggered. Changing State to FREE for all users");
            for(PeerNode peerNode:peerNodes.values()){
                peerNode.setState(State.FREE);
            }
            return true;
        }
    }

    private boolean teardownDHT(String leaderUsername) {
        try {
            PeerNode leaderNode = peerNodes.get(leaderUsername);
            if (leaderNode.getState() != State.LEADER) {
                return false;
            } else {
                System.out.println("TearingDown the DHT");
                String tearDownCommand = "teardown " + leaderNode.getName() + " teardown-dht";
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(tearDownCommand);
                byte[] packet = bos.toByteArray();
                DatagramPacket request = new DatagramPacket(packet, packet.length,
                        InetAddress.getByName(leaderNode.getIpAddress()), leaderNode.getPortLeft());
                socket.send(request);
                return true;
            }
        }catch(IOException e){
            e.printStackTrace();
            return false;
        }
    }

    private boolean deregisterUser(String username){
        try {
            PeerNode peerNode = peerNodes.get(username);
            if (peerNode.getState() != State.FREE) {
                return false;
            } else {
                String terminateCommand = "terminate";
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(terminateCommand);

                byte[] packet = bos.toByteArray();
                DatagramPacket request = new DatagramPacket(packet, packet.length,
                        InetAddress.getByName(peerNode.getIpAddress()), peerNode.getPortLeft());
                socket.send(request);
                peerNodes.remove(username);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void sendCommandToJoiningAndLeaderNode() {
        try {
            PeerNode leaderNode = null;
            for (PeerNode peerNode : peerNodes.values()) {
                if (peerNode.getState() == State.LEADER) {
                    leaderNode = peerNode;
                }
            }
            String resetLeftNeighbourCommand = "reset-left";
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(resetLeftNeighbourCommand);
            oos.writeObject(leaderNode);

            byte[] packet = bos.toByteArray();
            DatagramPacket request = new DatagramPacket(packet, packet.length,
                    InetAddress.getByName(joiningPeerNode.getIpAddress()), joiningPeerNode.getPortLeft());
            socket.send(request);

            String tearDownCommand = "teardown " + leaderNode.getName() + " join-dht";
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(tearDownCommand);
            oos.writeObject(joiningPeerNode);

            packet = bos.toByteArray();
            request = new DatagramPacket(packet, packet.length,
                    InetAddress.getByName(leaderNode.getIpAddress()), leaderNode.getPortLeft());
            socket.send(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendCommandToLeavingNode(PeerNode peerNode) {
        try {
            String tearDownCommand = "teardown " + peerNode.getName() + " leave-dht";
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(tearDownCommand);
//            oos.writeObject(leavingPeerNode.getName());
            byte[] packet = bos.toByteArray();
            DatagramPacket request = new DatagramPacket(packet, packet.length,
                    InetAddress.getByName(peerNode.getIpAddress()), peerNode.getPortLeft());
            socket.send(request);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean dhtRebuiltStepsForLeavingNode(String username, String leaderName) {
        System.out.println("dht-rebuilt executing");
        if (leavingPeerNode.getName().equals(username)) {
            peerNodes.get(username).setState(State.FREE);
            for (String key : peerNodes.keySet()) {
                if (peerNodes.get(key).getState() != State.FREE) {
                    peerNodes.get(key).setState(State.InDHT);
                }
            }
            peerNodes.get(leaderName).setState(State.LEADER);
            printPeerNodes();
            return true;
        }
        return false;
    }

    private boolean dhtRebuiltStepsForJoiningNode(String username, String leaderName) {
        System.out.println("dht-rebuilt executing");
        if (joiningPeerNode.getName().equals(username)) {
            peerNodes.get(username).setState(State.InDHT);
            peerNodes.get(leaderName).setState(State.LEADER);
            printPeerNodes();
            return true;
        }
        return false;
    }

    private void printPeerNodes() {
        for (PeerNode peerNode : peerNodes.values()) {
            System.out.println(peerNode);
        }
    }

    private boolean leaveDHT(String username) {
        if (peerNodes.containsKey(username) && peerNodes.get(username).getState() != State.FREE) {
            leavingPeerNode = peerNodes.get(username);
//            dhtPeerNodes.add(leavingPeerNode);
            return true;
        }
        return false;
    }

    private boolean joinDHT(String username) {
        if (peerNodes.containsKey(username) && peerNodes.get(username).getState() == State.FREE) {
            joiningPeerNode = peerNodes.get(username);
//            dhtPeerNodes.add(leavingPeerNode);
            return true;
        }
        return false;
    }

    private boolean queryDHT(String username, List<PeerNode> dhtPeerNodes) {
        if (peerNodes.containsKey(username) && peerNodes.get(username).getState() == State.FREE) {
            for (String key : peerNodes.keySet()) {
                PeerNode peerNode = peerNodes.get(key);
                if (peerNode.getState() == State.InDHT || peerNode.getState() == State.LEADER) {
                    dhtPeerNodes.add(peerNode);
                    break;
                }
            }
            return true;
        }
        return false;
    }

    private boolean setUpDHT(int n, String leaderUsername, List<PeerNode> dhtPeerNodes) {
        if (peerNodes.containsKey(leaderUsername)) {
            PeerNode peerNode = peerNodes.get(leaderUsername);
            peerNode.setState(State.LEADER);
            dhtPeerNodes.add(peerNode);

            for (String key : peerNodes.keySet()) {
                if (!key.equals(leaderUsername) && n > 1) {
                    peerNode = peerNodes.get(key);
                    peerNode.setState(State.InDHT);
                    dhtPeerNodes.add(peerNode);
                    n--;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean registerUser(String username, String peerIP, int peerPortLeft, int peerPortRight,
                                 int peerPortQuery) {
        PeerNode peerNode = new PeerNode(username, peerIP, peerPortLeft, peerPortRight, peerPortQuery);
        if (peerNodes.containsKey(username)) {
            System.out.println("Invalid Peer");
            return false;
        } else {
            peerNodes.put(username, peerNode);
            return true;
        }
    }

}
