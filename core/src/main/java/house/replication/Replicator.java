package house.replication;

import com.google.common.util.concurrent.ListenableFuture;
import house.service.Packet;

import java.util.List;
import java.util.concurrent.Future;

public interface Replicator {
  
  int getId();
  
  ReplicaResponse replicateLocally(Packet packet);
  
  List<ListenableFuture<ReplicaResponse>> sendToReplicas(Packet packet);
  
  int waitFor(Long transactionId, List<ListenableFuture<ReplicaResponse>> replicaResponses);
  
//  ReplicaResponse readFromMaster(Long transactionId);
  
  void sendTransactionsTo(int replicaId, Long fromTransactionId);
}
