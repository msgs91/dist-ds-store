package house.service;

import house.AppConfig;
import house.api.client.KV;
import house.api.response.ClusterResponse;
import house.model.Data;
import house.model.Packet;
import house.replication.ReplicationStrategy;
import house.replication.Replicator;
import house.store.InMemoryStore;
import house.wal.WalReader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class KVService {

  ReplicationStrategy replicationStrategy;
  Replicator replicator;
  AppConfig config;
  InMemoryStore store;
//  MasterClient master;

  public KVService(AppConfig config,
                   Replicator replicator,
                   ReplicationStrategy replicationStrategy,
                   InMemoryStore store) throws IOException {
    this.config = config;
    this.replicationStrategy = replicationStrategy;
    this.replicator = replicator;
    this.store = store;
    
  }
  
  public void start() throws IOException {
    loadStoreFromWal();
//    if (!isMaster()) {
//      this.master = new MasterClient(config.getReplicas().get(0));
//      loadStoreFromMaster();
//    }
    log.info("Starting");
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
  
  public boolean isMaster() {
    return config.isMaster();
  }
  
  public Long nextTransactionId() {
    return store.getNextTransactionId();
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
  
  private void loadStoreFromWal() throws IOException {
    log.info("Loading store from wal...");
    WalReader reader = new WalReader(config);
    Optional<Packet> maybePacket = reader.readNext();
    while (maybePacket.isPresent()) {
      Packet packet = maybePacket.get();
      replicationStrategy.processWalPacket(packet);
      maybePacket = reader.readNext();
    }
  }
  
//  private void loadStoreFromMaster() {
//    master.checkHealth();
//    boolean isSuccess = master.getTransactionsFrom(store.getNextTransactionId());
//    if (!isSuccess) {
//      log.info("Failed to connect to master..");
//    }
//  }
}
