package house.replication;

import house.service.Packet;

import java.util.List;
import java.util.concurrent.Future;

public interface Replicator {
  
  int getId();
  
  ReplicaResponse replicateLocally(Packet packet);
  
  List<Future<ReplicaResponse>> sendToReplicas(Packet packet);
  
  int waitFor(List<Future<ReplicaResponse>> replicaResponses);
  
  ReplicaResponse readFromMaster(Long transactionId);
  
  void sendTransactionsTo(int replicaId, Long fromTransactionId);
}
