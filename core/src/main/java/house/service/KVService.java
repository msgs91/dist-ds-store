package house.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.api.client.KV;
import house.api.response.ClusterResponse;
import house.api.response.PacketsResponse;
import house.replication.ReplicationStrategy;
import house.replication.Replicator;
import house.store.StoreReader;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Slf4j
public class KVService {
  
  ReplicationStrategy replicationStrategy;
  Replicator replicator;
  AppConfig config;
  
  public KVService(AppConfig config, Replicator replicator, ReplicationStrategy replicationStrategy) throws IOException {
    this.config = config;
    this.replicationStrategy = replicationStrategy;
    this.replicator = replicator;
    log.info(String.format("Expecting next transaction %d", replicationStrategy.getNextTransactionId()));
  }
  
  public Optional<String> get(String key) {
    return replicationStrategy.get(key);
  }
  
  public ServiceResponse put(KV kv) {
    if (replicationStrategy.replicate(kv)) {
      return new ServiceResponse(false, null, kv);
    } else {
      return new ServiceResponse(true, "", null);
    }
  }
  
  public boolean isHealthy() {
    //TODO write to wal and put value in in memory store
    return true;
  }
  
  public ClusterResponse onPacket(Packet packet) {
    log.debug(String.format("Got message %s", packet.getType()));
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
          replicator.sendTransactionsTo(packet.getReplicaId(), transactionId);
          response = new ClusterResponse("cluster", "", false);
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          response = new ClusterResponse("cluster", "Failed to notify master", true);
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
