package house.replication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.api.client.KV;
import house.exception.ApplicationException;
import house.model.Data;
import house.model.Packet;
import house.store.InMemoryStore;
import house.wal.WalReader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class BestEffortReplicationStrategy implements ReplicationStrategy {

  Replicator replicator;
  InMemoryStore store;
  AppConfig config;
  int numOfReplicas;

  public BestEffortReplicationStrategy(AppConfig config, Replicator replicator, InMemoryStore store) throws IOException {
    this.replicator = replicator;
    this.store = store;
    this.config = config;
    this.numOfReplicas = config.getReplicas().size();
    loadStore();
  }

  @Override
  public boolean replicate(KV kv) {
    try {
      Long transactionId = store.getNextTransactionId();
      Data data = new Data("kv", new ObjectMapper().writeValueAsString(kv), 1);
      Packet packet = new Packet(transactionId, replicator.getId(), "replicate_kv", 1, null, data);
      int succeeded = replicator.waitFor(transactionId, replicator.sendToReplicas(packet));
      log.info("%d replicas succeeded", succeeded);
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
            String message = String.format("transaction replicaId %d, type %s, data cannot be null", packet.getTransactionId(), type);
            log.error(message);
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
          String message = String.format("transaction replicaId %d, unknown type %s", packet.getTransactionId(), type);
          log.error(message);
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

  private void loadStore() throws IOException {
    WalReader reader = new WalReader(config);
    Optional<Packet> maybePacket = reader.readNext();
    while (maybePacket.isPresent()) {
      Packet packet = maybePacket.get();
      if (packet.getType() == "replicate_kv") {
        KV kv = new ObjectMapper().readValue(packet.getData().getValue(), KV.class);
        store.put(packet.getTransactionId(), kv.getKey(), kv.getValue());
      }
    }
  }
}
