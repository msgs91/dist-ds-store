package house.api.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import house.model.Packet;

import java.util.List;

public class PacketsResponse {

    List<Packet> packets;

    @JsonCreator
    public PacketsResponse(@JsonProperty("packets") List<Packet> packets) {
        this.packets = packets;
    }

    public List<Packet> getPackets() {
        return packets;
    }
}