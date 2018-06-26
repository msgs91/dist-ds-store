package house.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;
import house.AppConfig;
import house.exception.ApplicationException;
import house.service.Packet;
import house.service.ReplicaClient;
import house.wal.Wal;
import house.wal.WalReader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ReplicatorImpl implements Replicator {

  int id;
  int masterId;
  List<ReplicaClient> replicaClients;
  Wal wal;
  AppConfig config;
  BlockingQueue<ReplicaResponse> responseQueue;
  Map<Long, List<SettableFuture<ReplicaResponse>>> awaitingPromises;
  AtomicBoolean stopped;

  public ReplicatorImpl(AppConfig config) throws IOException {
    this.id = config.getId();
    this.config = config;
    this.replicaClients = new LinkedList<>();
    //TODO all replicas need not create a full graph connection with all other replicas
    List<String> replicaUrls = config.getReplicas();
    this.wal = new Wal(config);
    this.responseQueue = new LinkedBlockingQueue<>(10);

    for (int i = 1; i <= replicaUrls.size() && config.isMaster(); i++) {
      if (i != config.getId()) {
        ReplicaClient client = new ReplicaClient(i, replicaUrls.get(i - 1), new WalReader(config), responseQueue);
        this.replicaClients.add(client);
      }
    }
    this.masterId = 1;
    this.stopped = new AtomicBoolean(false);
    this.awaitingPromises = new HashMap<>(10);
    new Thread(this::handleReplicaResponses, "ReplicaResponseHadler").start();
  }

  public void stop() {
    stopped.set(true);
    replicaClients.forEach(ReplicaClient::stop);
  }

  @Override
  public int getId() {
    return this.id;
  }

  @Override
  public ReplicaResponse replicateLocally(Packet packet) {
    ReplicaResponse response;
    try {
      packet.setReplicaId(id);
      String message = new ObjectMapper().writeValueAsString(packet);
      wal.append(message);
      response = new ReplicaResponse(packet.getTransactionId(), id, 200);
    } catch (IOException e) {
      log.error("Failed to replicate %d locally", packet.getTransactionId());
      response = new ReplicaResponse(packet.getTransactionId(), id, 503);
    } catch (Exception e) {
      log.error("Failed to replicate %d locally", packet.getTransactionId());
      response = new ReplicaResponse(packet.getTransactionId(), id, 503);
    }
    return response;
  }



  @Override
  public List<ListenableFuture<ReplicaResponse>> sendToReplicas(Packet packet) {
    ListenableFutureTask<ReplicaResponse> localResponse = ListenableFutureTask.create(() -> replicateLocally(packet));
    List<ListenableFuture<ReplicaResponse>> remoteResponses = new ArrayList<>(replicaClients.size());
    List<SettableFuture<ReplicaResponse>> settableFutures = new ArrayList<>(replicaClients.size());
    for (ReplicaClient client: replicaClients) {
      SettableFuture<ReplicaResponse> future = SettableFuture.create();
      remoteResponses.add(future);
      settableFutures.add(future);
    }
    synchronized (this) {
      awaitingPromises.put(packet.getTransactionId(), settableFutures);
    }
    remoteResponses.add(localResponse);
    localResponse.run();
    return remoteResponses;
  }

  private void handleReplicaResponses() {
    while (!stopped.get()) {
      try {
        ReplicaResponse response = responseQueue.take();
        //TODO assume its always available
        List<SettableFuture<ReplicaResponse>> futures;
        synchronized (this) {
          futures = awaitingPromises.get(response.getTransactionId());
        }
        //if null, means the future list has been removed in waitFor
        if (futures != null) {
          SettableFuture<ReplicaResponse> future = futures.get(response.getReplicaId());
          future.set(response);
        }
      } catch (InterruptedException e) {
        String message = "Failed to read replica response from queue";
        log.error(message, e);
        throw new ApplicationException(message, e);
      }

    }
  }

  @Override
  public int waitFor(Long transactionId, List<ListenableFuture<ReplicaResponse>> responseFutures) {
    int succeeded = 0;
    int failed = 0;
    ListenableFuture<List<ReplicaResponse>> transactionFuture = Futures.allAsList(responseFutures);
    try {
      List<ReplicaResponse> replicaResponses = transactionFuture.get(5, TimeUnit.SECONDS);
      if (transactionFuture.isDone()) {
        for (ReplicaResponse response : replicaResponses) {
          if (response.getResponse() != 200) {
            failed += 1;
          } else {
            succeeded += 1;
          }
        }
      }
    } catch (TimeoutException e) {
      failed = 3;
    } catch (Exception e) {
      failed = 3;
    }
    synchronized (this) {
      awaitingPromises.remove(transactionId);
    }
    return succeeded;
  }

  @Override
  public void sendTransactionsTo(int replicaId, Long fromTransactionId) {

  }

//  @Override
//  public ReplicaResponse readFromMaster(Long transactionId) {
//    boolean end = false;
//    log.info(String.format("Reading transactions from master starting from %d", transactionId));
//    Data data = new Data("get_transactions_from", transactionId.toString(), 1);
//    Packet clusterPacket = new Packet(0L, config.getReplicaId(), "cluster", 1, null, data);
//    ReplicaClient master = getMaster();
//    Future<ReplicaResponse> ack = master.sendIfStarting(clusterPacket);
//    try {
//      ReplicaResponse response = ack.get();
//      return new ReplicaResponse(master.getReplicaId(), response.getResponse());
//    } catch (Exception e) {
//      log.error(e.getMessage(), e);
//      return new ReplicaResponse(master.getReplicaId(), 503);
//    }
//  }

  private ReplicaClient getMaster() {
    return replicaClients.get(this.masterId - 1);
  }

  /*private  List<Future<ReplicaResponse>> sendToRemoteReplicas(Packet packet) {
    List<Future<ReplicaResponse>> futureResponses = new ArrayList<>(replicaClients.size()-1);
    for (ReplicaClient replicaClient : replicaClients) {
      Future<ReplicaResponse> ack = replicaClient.sendIfUp(packet);
      futureResponses.add(ack);
    }
    return futureResponses;
  }

  private  void sendToRemoteReplica(int replicaId) {
    while (true) {

    }
    List<Future<ReplicaResponse>> futureResponses = new ArrayList<>(replicaClients.size()-1);
    for (ReplicaClient replicaClient : replicaClients) {
      Future<ReplicaResponse> ack = replicaClient.sendIfUp(packet);
      futureResponses.add(ack);
    }
  }*/
}
