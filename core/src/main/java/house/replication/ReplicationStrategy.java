package house.replication;

import house.api.client.KV;
import house.service.Packet;

import java.util.Optional;

public interface ReplicationStrategy {
  
  boolean replicate(KV kv);

  boolean handlePacket(Packet packet);

  void processWalPacket(Packet packet);

  Optional<String> get(String key);

  Long getNextTransactionId();
}
