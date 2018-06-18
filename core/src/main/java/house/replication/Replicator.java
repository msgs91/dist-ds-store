package house.replication;

import house.service.Packet;

import java.util.List;

public interface Replicator {

    int getId();

    ReplicaResponse replicateLocally(Packet packet);

    List<ReplicaResponse> sendToReplicas(Packet packet);
}
