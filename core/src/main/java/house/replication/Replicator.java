package house.replication;

import com.google.common.util.concurrent.ListenableFuture;
import house.model.Packet;

import java.util.List;
import java.util.concurrent.Future;

public interface Replicator {
  
  void start();

  void stop();

  int getId();
  
  ReplicaResponse replicateLocally(Packet packet);
  
  List<ListenableFuture<ReplicaResponse>> sendToReplicas(Packet packet);
  
  int waitFor(Long transactionId, List<ListenableFuture<ReplicaResponse>> replicaResponses);
  
//  ReplicaResponse readFromMaster(Long transactionId);
  
  boolean sendTransactionsTo(int replicaId, Long fromTransactionId);
}
