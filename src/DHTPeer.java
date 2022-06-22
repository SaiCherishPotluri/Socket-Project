import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DHTPeer implements Runnable {
    static String peerName = "abhi";
    static PeerNode leftPeer;
    static PeerNode rightPeer;
    static int identifier;
    static int ringSize;
    static Map<Integer, List<Record>> localRecords = new HashMap<>();
    static int resetIdRepeatCount = 0;
    int peerPort;
    String portType;

    public DHTPeer(int peerPort, String portType) {
        this.peerPort = peerPort;
        this.portType = portType;
    }

    public static void main(String[] args) {
        Thread leftPort = new Thread(new DHTPeer(2801, "left"));
        Thread rightPort = new Thread(new DHTPeer(2802, "right"));
        Thread queryPort = new Thread(new DHTPeer(2803, "query"));

        leftPort.start();
        rightPort.start();
        queryPort.start();
    }

    public void run() {
        switch (this.portType) {
            case "left":
                setupPortLeft(this.peerPort);
                break;
            case "right":
                setupPortRight(this.peerPort);
                break;
            case "query":
                setupPortQuery(this.peerPort);
                break;
            default:
                System.out.println("Invalid port type");
                break;
        }
    }

    private void setupPortQuery(int peerQueryPort) {
        try {
            System.out.println("Query 0 Started");
            DatagramSocket socket = new DatagramSocket(peerQueryPort);
            while (true) {
                DatagramPacket request = new DatagramPacket(new byte[10000], 10000);
                socket.receive(request);
                System.out.println(request.getPort());
                byte[] packet = request.getData();
                ByteArrayInputStream bais = new ByteArrayInputStream(packet);
                ObjectInputStream ois = new ObjectInputStream(bais);
                String command = String.valueOf(ois.readObject());
                String[] commandParams = command.split(" ");
                if (command.startsWith("query")) {
                    String queryString = "";
                    for (int i = 1; i < commandParams.length; i++) {
                        queryString += commandParams[i].trim() + " ";
                    }
                    queryString = queryString.trim();
                    System.out.println(queryString);
                    int position = getPosition(queryString);
                    int peerId = position % ringSize;
                    if (peerId == identifier) {
                        Record outputRecord = null;
                        List<Record> recordList = localRecords.get(position);
                        if (recordList != null) {
                            for (Record record : recordList) {
                                if (record.getLongName().equals(queryString)) {
                                    outputRecord = record;
                                    break;
                                }
                            }
                        }
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(outputRecord);
                        byte[] outputRecordByte = bos.toByteArray();
                        DatagramPacket response = new DatagramPacket(outputRecordByte, outputRecordByte.length,
                                request.getAddress(), request.getPort());
                        socket.send(response);
                    } else {
                        DatagramPacket request1 = new DatagramPacket(packet, packet.length,
                                InetAddress.getByName(leftPeer.getIpAddress()), leftPeer.getPortQuery());
                        socket.send(request1);

                        byte[] buffer = new byte[10000];
                        DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                        socket.receive(response);

                        DatagramPacket peerResponse = new DatagramPacket(buffer, buffer.length,
                                request.getAddress(), request.getPort());
                        socket.send(peerResponse);
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void setupPortLeft(int peerLeftPort) {
        System.out.println("Left 0 Started");
        try {
            DatagramSocket socket = new DatagramSocket(peerLeftPort);
            while (true) {
                DatagramPacket request = new DatagramPacket(new byte[10000], 10000);
                socket.receive(request);
                System.out.println(request.getPort());
                byte[] packet = request.getData();
                ByteArrayInputStream bais = new ByteArrayInputStream(packet);
                ObjectInputStream ois = new ObjectInputStream(bais);
                String command = (String) ois.readObject();
                String[] commandParams = command.split(" ");
                System.out.println(command);
                if (command.startsWith("set-id")) {
                    System.out.println("Setting Id command: " + command);
                    List<PeerNode> leftRightNode = (List<PeerNode>) ois.readObject();
                    System.out.println(leftRightNode.get(0).getName());
                    System.out.println(leftRightNode.get(1).getName());
                    identifier = Integer.parseInt(commandParams[1].trim());
                    ringSize = Integer.parseInt(commandParams[2].trim());
                    leftPeer = leftRightNode.get(0);
                    rightPeer = leftRightNode.get(1);
                } else if (command.startsWith("store")) {
                    System.out.println("Storing command: " + command);
                    int peerId = Integer.parseInt(commandParams[1].trim());
                    int position = Integer.parseInt(commandParams[2].trim());
                    if (peerId == identifier) {
//                    request = new DatagramPacket(packet, packet.length,
//                            InetAddress.getLocalHost(), 2812);
                        try {
                            Record record = (Record) ois.readObject();
                            if (localRecords.containsKey(position)) {
                                localRecords.get(position).add(record);
                            } else {
                                List<Record> records = new ArrayList<>();
                                records.add(record);
                                localRecords.put(position, records);
                            }
                        } catch (EOFException e) {
                            e.printStackTrace();
                        }
                    } else {
                        request = new DatagramPacket(packet, packet.length, InetAddress.getLocalHost(), 2802);
                        socket.send(request);
                    }

                    printLocalRecordsSize();
                } else if (command.startsWith("teardown")) {
                    if (localRecords.size() > 0) {
                        peerName = commandParams[1].trim();
                        String teardownCommandForRightPeer = "teardown " + rightPeer.getName() + " " + commandParams[2].trim();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(teardownCommandForRightPeer);
                        if (command.contains("join-dht")) {
                            oos.writeObject(ois.readObject());
                        }
                        byte[] teardownPacketForRightPeer = bos.toByteArray();
                        request = new DatagramPacket(teardownPacketForRightPeer, teardownPacketForRightPeer.length,
                                InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                        localRecords.clear();
                        printLocalRecordsSize();
                        socket.send(request);
                    } else {
                        resetIdRepeatCount++;
                        if (command.contains("leave-dht")) {
                            String resetIdCommand = "reset-id " + 0 + " " + (ringSize - 1) + " leave-dht";
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(resetIdCommand);

                            byte[] resetIdPacket = bos.toByteArray();

                            request = new DatagramPacket(resetIdPacket, resetIdPacket.length,
                                    InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                            socket.send(request);
                        } else if (command.contains("join-dht")) {
                            PeerNode joiningNode = (PeerNode) ois.readObject();
                            String resetLeftNeighbourCommand = "reset-left";
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(resetLeftNeighbourCommand);
                            oos.writeObject(joiningNode);

                            byte[] resetNeighbourPacket = bos.toByteArray();

                            request = new DatagramPacket(resetNeighbourPacket, resetNeighbourPacket.length,
                                    InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                            socket.send(request);

                            PeerNode tempRightPeer = rightPeer;
                            rightPeer = joiningNode;
                            String resetRightNeighbourCommand = "reset-right";
                            bos = new ByteArrayOutputStream();
                            oos = new ObjectOutputStream(bos);
                            oos.writeObject(resetRightNeighbourCommand);
                            oos.writeObject(tempRightPeer);

                            resetNeighbourPacket = bos.toByteArray();

                            request = new DatagramPacket(resetNeighbourPacket, resetNeighbourPacket.length,
                                    InetAddress.getByName(joiningNode.getIpAddress()), joiningNode.getPortLeft());
                            socket.send(request);

                            int newIdentifier = identifier + 1;
                            ringSize++;
                            String resetIdCommand = "reset-id " + newIdentifier + " " + ringSize + " join-dht";
                            bos = new ByteArrayOutputStream();
                            oos = new ObjectOutputStream(bos);
                            oos.writeObject(resetIdCommand);

                            byte[] resetIdPacket = bos.toByteArray();

                            request = new DatagramPacket(resetIdPacket, resetIdPacket.length,
                                    InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                            socket.send(request);
                        } else if (command.contains("teardown-dht")) {
                            teardownComplete(socket);
                        }
                    }
                } else if (command.startsWith("reset-id")) {
                    if (resetIdRepeatCount == 0) {
                        identifier = Integer.parseInt(commandParams[1].trim());
                        ringSize = Integer.parseInt(commandParams[2].trim());
                        int newIdentifier = identifier + 1;
                        String resetIdCommand = "reset-id " + newIdentifier + " " + ringSize + " " + commandParams[3].trim();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(resetIdCommand);

                        byte[] resetIdPacket = bos.toByteArray();

                        request = new DatagramPacket(resetIdPacket, resetIdPacket.length,
                                InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                        socket.send(request);
                    } else {
                        String serverDHTRebuiltCommand = "";
                        if (command.contains("leave-dht")) {
                            resetLeft(socket);
                            resetRight(socket);
                            serverDHTRebuiltCommand = "dht-rebuilt " + peerName + " " + rightPeer.getName() + " leave-dht";
                        }
                        if (command.contains("join-dht")) {
                            serverDHTRebuiltCommand = "dht-rebuilt " + rightPeer.getName() + " " + peerName + " join-dht";
                        }
                        rebuildDHT(socket);
                        dhtRebuilt(socket, serverDHTRebuiltCommand);
                    }
                } else if (command.startsWith("reset-left")) {
                    System.out.println("Resetting left Peer command: " + command);
                    PeerNode node = (PeerNode) ois.readObject();
                    leftPeer = node;
                } else if (command.startsWith("reset-right")) {
                    System.out.println("Resetting right Peer command: " + command);
                    PeerNode node = (PeerNode) ois.readObject();
                    rightPeer = node;
                } else if (command.startsWith("rebuild-dht")) {
                    parseAndStoreData(socket, ringSize);
                } else if (command.startsWith("terminate")) {
                    System.out.println("Terminating Peer: " + peerName);
                    System.exit(0);
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void teardownComplete(DatagramSocket socket) throws IOException, ClassNotFoundException {
        String serverDHTRebuiltCommand = "teardown-complete " + peerName;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(serverDHTRebuiltCommand);

        byte[] serverPacket = bos.toByteArray();

        DatagramPacket request = new DatagramPacket(serverPacket, serverPacket.length,
                InetAddress.getByName(Constants.SERVER_IP_ADDRESS), Constants.serverPort);
        socket.send(request);

        byte[] buffer = new byte[10000];
        DatagramPacket response = new DatagramPacket(buffer, buffer.length);
        socket.receive(response);

        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
        ObjectInputStream ois = new ObjectInputStream(bais);

        String statusCode = (String) ois.readObject();
        System.out.println("Server Returned:" + statusCode);
    }

    private void dhtRebuilt(DatagramSocket socket, String serverDHTRebuiltCommand) throws IOException, ClassNotFoundException {
        resetIdRepeatCount = 0;
//        String serverDHTRebuiltCommand = "dht-rebuilt";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(serverDHTRebuiltCommand);
        oos.writeObject(rightPeer);

        byte[] serverPacket = bos.toByteArray();

        DatagramPacket request = new DatagramPacket(serverPacket, serverPacket.length,
                InetAddress.getByName(Constants.SERVER_IP_ADDRESS), Constants.serverPort);
        socket.send(request);

        byte[] buffer = new byte[10000];
        DatagramPacket response = new DatagramPacket(buffer, buffer.length);
        socket.receive(response);

        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
        ObjectInputStream ois = new ObjectInputStream(bais);

        String statusCode = (String) ois.readObject();
        System.out.println("Server Returned:" + statusCode);
    }

    private void rebuildDHT(DatagramSocket socket) throws IOException {
        String rebuildDHTCommand = "rebuild-dht";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(rebuildDHTCommand);

        byte[] rebuildDHTPacket = bos.toByteArray();

        DatagramPacket request = new DatagramPacket(rebuildDHTPacket, rebuildDHTPacket.length,
                InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
        socket.send(request);
    }

    private void resetLeft(DatagramSocket socket) throws IOException {
        String resetLeftNeighbourCommand = "reset-left";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(resetLeftNeighbourCommand);
        oos.writeObject(leftPeer);

        byte[] resetNeighbourPacket = bos.toByteArray();

        DatagramPacket request = new DatagramPacket(resetNeighbourPacket, resetNeighbourPacket.length,
                InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
        socket.send(request);
    }

    private void resetRight(DatagramSocket socket) throws IOException {
        String resetRightNeighbourCommand = "reset-right";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(resetRightNeighbourCommand);
        oos.writeObject(rightPeer);

        byte[] resetNeighbourPacket = bos.toByteArray();

        DatagramPacket request = new DatagramPacket(resetNeighbourPacket, resetNeighbourPacket.length,
                InetAddress.getByName(leftPeer.getIpAddress()), leftPeer.getPortLeft());
        socket.send(request);
    }

    private void setupPortRight(int peerPort) {
        try {
            System.out.println("Right 0 Started");
            System.out.println("Enter Server Hostname and Port Number:");
            Scanner scan = new Scanner(System.in);

            String destinationHost = scan.nextLine();
            int destinationPort = Integer.parseInt(scan.nextLine());
            System.out.println(destinationHost);
            System.out.println(destinationPort);

            InetAddress destinationAddress = InetAddress.getByName(destinationHost);
            DatagramSocket socket = new DatagramSocket(peerPort);
            System.out.println("Enter the command:");
            String input = scan.nextLine();
            System.out.println(input);
            if (input.startsWith("setup-dht")) {
                setupDHT(input, socket, destinationAddress, destinationPort);
            }
            while (true) {
                DatagramPacket request = new DatagramPacket(new byte[10000], 10000);
                socket.receive(request);
                System.out.println(request.getPort());
                byte[] packet = request.getData();
                ByteArrayInputStream bais = new ByteArrayInputStream(request.getData());
                ObjectInputStream ois = new ObjectInputStream(bais);
                String command = (String) ois.readObject();
                System.out.println(command);
                String[] commandParams = command.split(" ");
                if (command.startsWith("set-id")) {
                    System.out.println("Setting Id command: " + command);
                    List<PeerNode> leftRightNode = (List<PeerNode>) ois.readObject();
                    System.out.println(command);
                    System.out.println(leftRightNode.get(0).getName());
                    System.out.println(leftRightNode.get(1).getName());
                    identifier = Integer.parseInt(commandParams[1].trim());
                    ringSize = Integer.parseInt(commandParams[2].trim());
                    leftPeer = leftRightNode.get(0);
                    rightPeer = leftRightNode.get(1);
                } else if (command.startsWith("store")) {
                    System.out.println("Storing command: " + command);
                    request = new DatagramPacket(packet, packet.length,
                            InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                    socket.send(request);
                }
            }
        } catch (SocketTimeoutException ex) {
            System.out.println("Timeout error: " + ex.getMessage());
            ex.printStackTrace();
        } catch (IOException ex) {
            System.out.println("Right error: " + ex.getMessage());
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void setupDHT(String input, DatagramSocket socket, InetAddress destinationAddress, int destinationPort) throws IOException, ClassNotFoundException {
        DatagramPacket request = new DatagramPacket(input.getBytes(), input.length(),
                destinationAddress, destinationPort);
        socket.send(request);

        byte[] buffer = new byte[10000];
        DatagramPacket response = new DatagramPacket(buffer, buffer.length);
        socket.receive(response);
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
        ObjectInputStream ois = new ObjectInputStream(bais);
        List<PeerNode> peerNodes = new ArrayList<>();

        String statusCode = (String) ois.readObject();
        System.out.println("Server Returned:" + statusCode);
        Object object = ois.readObject();
        peerNodes = (List<PeerNode>) object;

        identifier = 0;
        ringSize = peerNodes.size();

        int leftIndex = 2;
        int rightIndex = (identifier + 1) % ringSize;

        leftPeer = peerNodes.get(leftIndex);
        rightPeer = peerNodes.get(rightIndex);

        for (int i = 1; i < peerNodes.size(); i++) {
            String command = "set-id " + i + " " + ringSize;
            List<PeerNode> leftRightNodes = new ArrayList();
            leftRightNodes.add(peerNodes.get((i - 1) % ringSize));
            leftRightNodes.add(peerNodes.get((i + 1) % ringSize));

            System.out.println("Length of command: " + command.getBytes(StandardCharsets.UTF_8).length);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(command);
            oos.writeObject(leftRightNodes);

            byte[] packet = bos.toByteArray();

            System.out.println(InetAddress.getByName(peerNodes.get(i).getIpAddress()));
            System.out.println(peerNodes.get(i).getPortLeft());

            request = new DatagramPacket(packet, packet.length,
                    InetAddress.getByName(peerNodes.get(i).getIpAddress()), peerNodes.get(i).getPortLeft());
            socket.send(request);
        }
        System.out.println("Ring Setup is Done with ring size: " + ringSize);
        System.out.println("Distributing Data across nodes");
        parseAndStoreData(socket, peerNodes.size());

        String dhtCompleteCommand = "dht-complete " + peerName;
        byte[] serverPacket = dhtCompleteCommand.getBytes();
        request = new DatagramPacket(serverPacket, serverPacket.length,
                InetAddress.getByName(Constants.SERVER_IP_ADDRESS), Constants.serverPort);
        socket.send(request);

        buffer = new byte[10000];
        response = new DatagramPacket(buffer, buffer.length);
        socket.receive(response);

        bais = new ByteArrayInputStream(buffer);
        ois = new ObjectInputStream(bais);

        statusCode = (String) ois.readObject();
        System.out.println("Server Returned:" + statusCode);
    }

    private void parseAndStoreData(DatagramSocket socket, int numberOfPeers) {
        String fileName = "C:\\Users\\abhis\\Downloads\\StatsCountry.csv";
        try {
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            CSVParser csvParser = new CSVParser(new FileReader(fileName), format);
            for (CSVRecord record : csvParser) {
                String longName = record.get("Long Name");
                int position = getPosition(longName);
                int peerId = position % numberOfPeers;

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                Record customRecord = getCustomRecord(record);
                oos.writeObject("store " + peerId + " " + position);
                oos.writeObject(customRecord);

                byte[] packet = bos.toByteArray();

//                System.out.println(InetAddress.getByName(peerNodes.get(i).getIpAddress()));
//                System.out.println(peerNodes.get(i).getPortLeft());

                if (peerId == identifier) {
                    if (localRecords.containsKey(position)) {
                        localRecords.get(position).add(customRecord);
                    } else {
                        List<Record> records = new ArrayList<>();
                        records.add(customRecord);
                        localRecords.put(position, records);
                    }
                } else {
                    DatagramPacket request = new DatagramPacket(packet, packet.length,
                            InetAddress.getByName(rightPeer.getIpAddress()), rightPeer.getPortLeft());
                    socket.send(request);
                    Thread.sleep(100);
                }
//                System.out.println(localRecords.size());
                printLocalRecordsSize();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Record getCustomRecord(CSVRecord record) {
        String countryCode = record.get("Country Code");
        String shortName = record.get("Short Name");
        String tableName = record.get("Table Name");
        String longName = record.get("Long Name");
        String alphaCode = record.get("2-Alpha Code");
        String currencyUnit = record.get("Currency Unit");
        String region = record.get("Region");
        String code = record.get("WB-2 Code");
        String census = record.get("Latest Population Census");
        Record customRecord = new Record(countryCode, shortName, tableName, longName,
                alphaCode, currencyUnit, region, code, census);
        return customRecord;
    }

    private void printLocalRecordsSize() {
        int c = 0;
        for (Integer key : localRecords.keySet()) {
//            c++;
            c += localRecords.get(key).size();
        }
        System.out.println("Local Hash Table Size: " + c);
    }

    private int getPosition(String longName) {
        int position = 0;
        for (int i = 0; i < longName.length(); i++) {
            position += longName.charAt(i);
        }
        return position % Constants.TABLE_SIZE;
    }
}
