import java.io.Serializable;
import java.util.Objects;

public class PeerNode implements Serializable {
    private static final long serialVersionUID = 7829136421241571165L;

    private String name;
    private String ipAddress;
    private int portLeft;
    private int portRight;
    private int portQuery;
    private State state;

    public PeerNode(String name, String ipAddress, int portLeft, int portRight, int portQuery) {
        this.name = name;
        this.ipAddress = ipAddress;
        this.portLeft = portLeft;
        this.portRight = portRight;
        this.portQuery = portQuery;
        this.state = State.FREE;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPortLeft() {
        return portLeft;
    }

    public void setPortLeft(int portLeft) {
        this.portLeft = portLeft;
    }

    public int getPortRight() {
        return portRight;
    }

    public void setPortRight(int portRight) {
        this.portRight = portRight;
    }

    public int getPortQuery() {
        return portQuery;
    }

    public void setPortQuery(int portQuery) {
        this.portQuery = portQuery;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "PeerNode{" +
                "name='" + name + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", portLeft=" + portLeft +
                ", portRight=" + portRight +
                ", portQuery=" + portQuery +
                ", state=" + state +
                '}';
    }
}
