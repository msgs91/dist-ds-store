package house.replication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.api.client.KV;
import house.api.cluster.Commit;
import house.api.cluster.Prepare;
import house.exception.ApplicationException;
import house.service.Data;
import house.service.Packet;
import house.store.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class TwoPhaseCommitStrategy implements ReplicationStrategy {

    private static Logger logger = LoggerFactory.getLogger(TwoPhaseCommitStrategy.class);

    AppConfig config;
    Replicator replicator;
    InMemoryStore store;

    public TwoPhaseCommitStrategy(AppConfig config, Replicator replicator, InMemoryStore store) {
        this.replicator = replicator;
        this.store = store;
        this.config = config;
    }

    @Override
    public boolean replicate(KV kv) {
        try {
            Long prepareTransactionId = store.getNextTransactionId();
            Stream<ReplicaResponse> prepareResponses = prepare(prepareTransactionId).stream();
            Stream<ReplicaResponse> failedPrepares = prepareResponses.filter(replicaResponse -> replicaResponse.getResponse() != 200);
            Long commitTransactionId = prepareTransactionId + 1;
            store.setNextTransactionId(commitTransactionId);
            if (failedPrepares.count() == 0) {
                ObjectMapper mapper = new ObjectMapper();
                Data data = new Data("kv", mapper.writeValueAsString(kv), 1);
                Packet commitPacket = new Packet(commitTransactionId, config.getId(), Commit.MESSAGE_TYPE, 1, null, data);
                boolean isSuccess = commit(commitPacket);
                if (isSuccess) {
                    store.put(commitTransactionId, kv.getKey(), kv.getValue());
                    return true;
                }
                return false;
            }
            return false;
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            throw new ApplicationException(e);
        }
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
                case "prepare":
                    ReplicaResponse response = replicator.replicateLocally(packet);
                    if (response.getResponse() != 200) {
                        return false;
                    }
                    logger.info(String.format("Setting next txn id to %d", packet.getTransactionId()+1));
                    store.setNextTransactionId(packet.getTransactionId()+1);
                    return true;
                case "commit":
                    Data data = packet.getData();
                    if (data == null) {
                        String message = String.format("transaction id %d, type %s, data cannot be null", packet.getTransactionId(), type);
                        throw new ApplicationException(message);
                    }
                    ReplicaResponse commitResponse = replicator.replicateLocally(packet);
                    if (commitResponse.getResponse() != 200) {
                        return false;
                    }
                    KV kv = new ObjectMapper().readValue(data.getValue(), KV.class);
                    logger.info(String.format("Putting next txn id to %d", packet.getTransactionId()+1));
                    store.put(packet.getTransactionId(), kv.getKey(), kv.getValue());
                    return true;
                default:
                    break;
            }
            return true;
        } catch (IOException e) {
            throw new ApplicationException(e);
        }
    }

    @Override
    public void processWalPacket(Packet packet) {
        String type = packet.getType();
        switch(type) {
            case "prepare":
                logger.info("Reading prepare for transaction id %d ", packet.getTransactionId());
                store.setNextTransactionId(packet.getTransactionId()+1);
                break;
            case "commit":
                try {
                    Data data = packet.getData();
                    KV kv = new ObjectMapper().readValue(data.getValue(), KV.class);
                    store.put(packet.getTransactionId(), kv.getKey(), kv.getValue());
                } catch (IOException e) {
                    throw new ApplicationException(e);
                }
                break;
        }
    }

    @Override
    public Long getNextTransactionId() {
        return store.getNextTransactionId();
    }

    private List<ReplicaResponse> prepare(Long transactionId) {
        Packet packet = new Packet(transactionId, config.getId(), Prepare.MESSAGE_TYPE, 1, null, null);
        List<ReplicaResponse> replicaResponses = replicator.sendToReplicas(packet);
        return replicaResponses;
    }

    private boolean commit(Packet packet) {
        Stream<ReplicaResponse> failedCommits = replicator.sendToReplicas(packet).stream().filter(replicaResponse -> replicaResponse.getResponse() != 200);
        if (failedCommits.count() != 0) {
            return false;
        }
        return true;
    }
}
