package house.replication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import house.AppConfig;
import house.api.client.KV;
import house.api.cluster.Commit;
import house.api.cluster.Prepare;
import house.exception.ApplicationException;
import house.model.Data;
import house.model.Packet;
import house.store.InMemoryStore;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static house.replication.TwoPhaseCommitStrategy.Ack.ALL;

@Slf4j
public class TwoPhaseCommitStrategy implements ReplicationStrategy {
  
  public enum Ack {
    ALL,
    QUORUM;
  }
  
  private final AppConfig config;
  private final Replicator replicator;
  private final InMemoryStore store;
  private final int numOfReplicas;
  private final int minNumOfSuccess;
  
  public TwoPhaseCommitStrategy(AppConfig config,
                                Replicator replicator,
                                InMemoryStore store,
                                Ack ack) throws IOException {
    this.replicator = replicator;
    this.store = store;
    this.config = config;
    this.numOfReplicas = config.getReplicas().size();
    if (ack == ALL) {
      minNumOfSuccess = numOfReplicas;
    } else {
      minNumOfSuccess = numOfReplicas/2 + 1;
    }
  }
  
  @Override
  public synchronized boolean replicate(KV kv) {
    try {
      Long prepareTransactionId = store.getNextTransactionId();
      int prepareSucceeded = prepare(prepareTransactionId);
      Long commitTransactionId = prepareTransactionId + 1;
      store.setNextTransactionId(commitTransactionId);
      
      if (prepareSucceeded >= minNumOfSuccess) {
        ObjectMapper mapper = new ObjectMapper();
        Data data = new Data("kv", mapper.writeValueAsString(kv), 1);
        Packet commitPacket = new Packet(commitTransactionId, config.getId(), Commit.MESSAGE_TYPE, 1, null, data);
        int successCommits = commit(commitPacket);
        if (successCommits >= minNumOfSuccess) {
          store.put(commitTransactionId, kv.getKey(), kv.getValue());
          return true;
        }
        log.error(String.format("%d commits succeeded", successCommits));
        return false;
      }
      log.info(String.format("%d prepares succeeded", prepareSucceeded));
      return false;
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
      throw new ApplicationException(e);
    }
  }
  
  @Override
  public Optional<String> get(String key) {
    return store.get(key);
  }
  
  @Override
  public synchronized boolean handlePacket(Packet packet) {
    try {
      String type = packet.getType();
      switch(type) {
        case "prepare":
          ReplicaResponse response = replicator.replicateLocally(packet);
          if (response.getResponse() != 200) {
            return false;
          }
          log.info(String.format("Setting next txn id to %d", packet.getTransactionId()+1));
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
          log.info(String.format("Putting next transaction id to %d", packet.getTransactionId()+1));
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
  public synchronized void processWalPacket(Packet packet) {
    String type = packet.getType();
    switch(type) {
      case "prepare":
        log.info(String.format("Reading prepare for transaction replicaId %d ", packet.getTransactionId()));
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
  public synchronized Long getNextTransactionId() {
    return store.getNextTransactionId();
  }
  
  private int prepare(Long transactionId) {
    Packet packet = new Packet(transactionId, config.getId(), Prepare.MESSAGE_TYPE, 1, null, null);
    List<ListenableFuture<ReplicaResponse>> replicaResponses = replicator.sendToReplicas(packet);
    return replicator.waitFor(transactionId, replicaResponses);
  }
  
  private int commit(Packet packet) {
    return replicator.waitFor(packet.getTransactionId(), replicator.sendToReplicas(packet));
  }
  
  
}
