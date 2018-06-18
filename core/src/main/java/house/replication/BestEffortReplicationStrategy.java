package house.replication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import house.api.client.KV;
import house.exception.ApplicationException;
import house.service.Data;
import house.service.Packet;
import house.store.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class BestEffortReplicationStrategy implements ReplicationStrategy {

    private static Logger logger = LoggerFactory.getLogger(BestEffortReplicationStrategy.class);
    Replicator replicator;
    InMemoryStore store;

    public BestEffortReplicationStrategy(Replicator replicator, InMemoryStore store) {
        this.replicator = replicator;
        this.store = store;
    }

    @Override
    public boolean replicate(KV kv) {
        try {
            Long transactionId = store.getNextTransactionId();
            Data data = new Data("kv", new ObjectMapper().writeValueAsString(kv), 1);
            Packet packet = new Packet(transactionId, replicator.getId(), "replicate_kv", 1, null, data);
            List<ReplicaResponse> replicaResponses = replicator.sendToReplicas(packet);
            Stream<ReplicaResponse> failedReplicas = replicaResponses.stream().filter(replicaResponse -> replicaResponse.getResponse() != 200);
            if (failedReplicas.count() != 0) {
                return false;
            }
            store.put(transactionId, kv.getKey(), kv.getValue());
        } catch (JsonProcessingException e) {
            throw new ApplicationException(e);
        }
        return true;
    }

    @Override
    public Optional<String> get(String key) {
        return store.get(key);
    }

    @Override
    public boolean handlePacket(Packet packet) {
        try {
            String type = packet.getType();
            switch(type) {
                case "replicate_kv":
                    Data data = packet.getData();
                    if (data == null) {
                        String message = String.format("transaction id %d, type %s, data cannot be null", packet.getTransactionId(), type);
                        logger.error(message);
                        throw new ApplicationException(message);
                    }
                    ReplicaResponse response = replicator.replicateLocally(packet);
                    if (response.getResponse() != 200) {
                        return false;
                    }
                    KV kv = new ObjectMapper().readValue(data.getValue(), KV.class);
                    store.put(packet.getTransactionId(), kv.getKey(), kv.getValue());
                    return true;
                default:
                    String message = String.format("transaction id %d, unknown type %s", packet.getTransactionId(), type);
                    logger.error(message);
                    throw new ApplicationException(message);
            }
        } catch (IOException e) {
            throw new ApplicationException(e);
        }
    }

    @Override
    public void processWalPacket(Packet packet) {
        String type = packet.getType();
        switch(type) {
            case "replicate_kv":
                try {
                    Data data = packet.getData();
                    KV kv = new ObjectMapper().readValue(data.getValue(), KV.class);
                    store.put(packet.getTransactionId(), kv.getKey(), kv.getValue());
                } catch (IOException e) {
                    throw new ApplicationException(e);
                }
                break;
            default:
                throw new ApplicationException(String.format("unknown packet type %s ", type));
        }
    }

    @Override
    public Long getNextTransactionId() {
        return store.getNextTransactionId();
    }
}
