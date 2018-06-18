package house.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.api.client.KV;
import house.api.response.ClusterResponse;
import house.api.response.PacketsResponse;
import house.replication.ReplicationStrategy;
import house.store.StoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class KVService {
    private static Logger logger = LoggerFactory.getLogger(KVService.class);

    ReplicationStrategy replicationStrategy;
    AppConfig config;
    StoreReader storeReader;

    public KVService(AppConfig config, ReplicationStrategy replicationStrategy) throws IOException {
        this.config = config;
        this.replicationStrategy = replicationStrategy;
        int masterId = 1;
        String masterUri = config.getReplicas().get(masterId-1);
        if (!isMaster()) {
            this.storeReader = new StoreReader(config, replicationStrategy, new ReplicaClient(masterId, masterUri));
        } else {
            this.storeReader = new StoreReader(config, replicationStrategy, null);
        }
        storeReader.start();
        logger.info(String.format("Expecting next transaction %d", replicationStrategy.getNextTransactionId()));
    }

    public Optional<String> get(String key) {
        return replicationStrategy.get(key);
    }

    public Optional<String> put(KV kv) {
        if (replicationStrategy.replicate(kv)) {
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    public boolean isHealthy() {
        //TODO write to wal and put value in in memory store
        return true;
    }

    public ClusterResponse onPacket(Packet packet) {
        logger.debug(String.format("Got message %s", packet.getType()));
        ClusterResponse response;
        String type = packet.getType();
        switch(type) {
            case "cluster":
                response =  handlePacket(packet);
                break;
            default:
                boolean isSuccess = replicationStrategy.handlePacket(packet);
                if (isSuccess) {
                    response = new ClusterResponse("replication", null, false);
                } else {
                    response = new ClusterResponse("replication", null, true);
                }
                break;
        }
        return response;
    }

    private ClusterResponse handlePacket(Packet packet) {
        ClusterResponse response;
        Data data = packet.getData();
        switch(data.getType()) {
            case "get_transactions_from":
                try {
                    Long transactionId = Long.parseLong(packet.getData().getValue());
                    List<Packet> packets = storeReader.getTransactionsFrom(transactionId);
                    PacketsResponse packetsResponse = new PacketsResponse(packets);
                    String packetsEntity = new ObjectMapper().writeValueAsString(packetsResponse);
                    response = new ClusterResponse("cluster", packetsEntity, false);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    response = new ClusterResponse("cluster", null, true);
                }
                break;
            default:
                response = new ClusterResponse("cluster", "unknown method", true);
                break;

        }
        return response;
    }

    public boolean isMaster() {
        return config.isMaster();
    }

}
