package house.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;
import house.AppConfig;
import house.exception.ApplicationException;
import house.model.Packet;
import house.service.ReplicaClient;
import house.wal.Wal;
import house.wal.WalReader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ReplicatorImpl implements Replicator {
  
  private final int id;
  private final Map<Integer, ReplicaClient> replicaClients;
  private final Wal wal;
  private final AppConfig config;
  private final BlockingQueue<ReplicaResponse> responseQueue;
  private final Map<Long, Map<Integer, SettableFuture<ReplicaResponse>>> awaitingPromises;
  private final AtomicBoolean stopped;
  
  public ReplicatorImpl(AppConfig config) throws IOException {
    this.id = config.getId();
    this.config = config;
    this.replicaClients = new HashMap<>();
    //TODO all replicas need not create a full graph connection with all other replicas
    List<String> replicaUrls = config.getReplicas();
    this.wal = new Wal(config);
    this.responseQueue = new LinkedBlockingQueue<>(10);
    
    for (int i = 1; i <= replicaUrls.size() && config.isMaster(); i++) {
      if (i != config.getId()) {
        ReplicaClient client = new ReplicaClient(i, replicaUrls.get(i - 1), new WalReader(config), responseQueue);
        this.replicaClients.put(client.getId(), client);
      }
    }
    this.stopped = new AtomicBoolean(false);
    this.awaitingPromises = new HashMap<>(10);
  }
  
  @Override
  public void start() {
    stopped.set(false);
    if (isMaster()) {
      new Thread(this::handleReplicaResponses, "ReplicaResponseHadler").start();
      for (ReplicaClient client : replicaClients.values()) {
        client.start(1L);
      }
    }
  }
  
  @Override
  public void stop() {
    stopped.set(true);
    if (isMaster()) {
      replicaClients.forEach((id, replicaClient) -> replicaClient.stop());
    }
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
    Map<Integer, SettableFuture<ReplicaResponse>> settableFutures = new HashMap<>(replicaClients.size());
    for (Map.Entry<Integer, ReplicaClient> client: replicaClients.entrySet()) {
      SettableFuture<ReplicaResponse> future = SettableFuture.create();
      remoteResponses.add(future);
      settableFutures.put(client.getValue().getId(), future);
    }
    synchronized (this) {
      awaitingPromises.put(packet.getTransactionId(), settableFutures);
    }
    remoteResponses.add(localResponse);
    localResponse.run();
    return remoteResponses;
  }
  
  @Override
  public int waitFor(Long transactionId, List<ListenableFuture<ReplicaResponse>> responseFutures) {
    int succeeded = 0;
    int failed = 0;
    ListenableFuture<List<ReplicaResponse>> transactionFuture = Futures.allAsList(responseFutures);
    try {
      Long start = System.nanoTime();
      List<ReplicaResponse> replicaResponses = transactionFuture.get(1, TimeUnit.SECONDS);
      Long read = System.nanoTime();
      Long latency = (read - start)/1000000L;
      log.info(String.format("Got response in %d milliseconds", latency));
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
  public boolean sendTransactionsTo(int replicaId, Long fromTransactionId) {
    ReplicaClient client = replicaClients.get(replicaId);
    client.start(fromTransactionId);
    return true;
  }
  
  private void handleReplicaResponses() {
    while (!stopped.get()) {
      try {
        ReplicaResponse response = responseQueue.take();
        //TODO assume its always available
        Map<Integer, SettableFuture<ReplicaResponse>> futures;
        synchronized (this) {
          futures = awaitingPromises.get(response.getTransactionId());
        }
        //if null, means the future list has been removed in waitFor
        if (futures != null) {
          SettableFuture<ReplicaResponse> future = futures.get(response.getReplicaId());
          if (future != null) {
            future.set(response);
          } else {
            //TODO how to handle null here ? ApplicationException ?
          }
        }
      } catch (InterruptedException e) {
        String message = "Failed to read replica response from queue";
        log.error(message, e);
        throw new ApplicationException(message, e);
      }
      
    }
  }
  
  private int getMasterId() {
    return 1;
  }
  
  private boolean isMaster() {
    return config.isMaster();
  }
}
