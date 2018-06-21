package house.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.exception.ApplicationException;
import house.service.Data;
import house.service.Packet;
import house.service.ReplicaClient;
import house.wal.ILog;
import house.wal.Wal;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

@Slf4j
public class ReplicatorImpl implements Replicator {
  
  int id;
  int masterId;
  List<ReplicaClient> replicaClients;
  ILog wal;
  AppConfig config;
  
  public ReplicatorImpl(AppConfig config) throws IOException {
    this.id = config.getId();
    this.config = config;
    this.replicaClients = new LinkedList<>();
    //TODO all replicas need not create a full graph connection with all other replicas
    List<String> replicaUrls = config.getReplicas();
    for (int i = 1; i <= replicaUrls.size(); i++) {
      if (i != config.getId()) {
        this.replicaClients.add(new ReplicaClient(i, replicaUrls.get(i-1)));
      }
    }
    this.wal = new Wal(config);
    this.masterId = 1;
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
      response = new ReplicaResponse(id, 200);
    } catch (IOException e) {
      log.error("Failed to replicate %d locally", packet.getTransactionId());
      response = new ReplicaResponse(id, 503);
    } catch (Exception e) {
      log.error("Failed to replicate %d locally", packet.getTransactionId());
      response = new ReplicaResponse(id, 503);
    }
    return response;
  }
  
  @Override
  public List<Future<ReplicaResponse>> sendToReplicas(Packet packet) {
    FutureTask<ReplicaResponse> localResponse = new FutureTask<>(() -> replicateLocally(packet));
    localResponse.run();
    List<Future<ReplicaResponse>> remoteResponses = sendToRemoteReplicas(packet);
    remoteResponses.add(localResponse);
    return remoteResponses;
  }
  
  @Override
  public int waitFor(List<Future<ReplicaResponse>> replicaResponses) {
    int succeeded = 0;
    int failed = 0;
    for (Future<ReplicaResponse> replicaResponseFuture : replicaResponses) {
      try {
        ReplicaResponse replicaResponse = replicaResponseFuture.get(5, TimeUnit.SECONDS);
        if (replicaResponse.getResponse() != 200) {
          failed += 1;
        } else {
          succeeded += 1;
        }
      } catch (TimeoutException e) {
        failed += 1;
      } catch (Exception e) {
        failed +=1;
      }
    }
    return succeeded;
  }
  
  @Override
  public void sendTransactionsTo(int replicaId, Long fromTransactionId) {
    ReplicaClient replicaClient = replicaClients.get(replicaId-1);
    ReplicaClient.State state = replicaClient.checkHealth();
    if (state == ReplicaClient.State.DOWN) {
      return false;
    }
    try {
      Stream<Integer> responses =
        wal.readLines()
        .stream()
        .map(line -> {
          try {
            Packet packet = new ObjectMapper().readValue(line, Packet.class);
            return packet;
          } catch (IOException e) {
            String message = String.format("Failed to parse line %s", line);
            throw new ApplicationException(message, e);
          }
        })
        .filter(packet -> packet.getTransactionId() >= fromTransactionId)
        .map(packet -> {
          int response;
          try{
            Future<ReplicaResponse> replicaResponseFuture = replicaClient.sendIfStarting(packet);
            response = replicaResponseFuture.get(5, TimeUnit.SECONDS).getResponse();
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            response = 503;
          }
          return response;
        });
      
      if (responses.allMatch(response -> response == 200)) {
        replicaClient.setState(ReplicaClient.State.UP);
      }
    } catch (IOException e) {
        throw new ApplicationException("Failed to read transactions from wal");
    }
  }
  
  @Override
  public ReplicaResponse readFromMaster(Long transactionId) {
    boolean end = false;
    log.info(String.format("Reading transactions from master starting from %d", transactionId));
    Data data = new Data("get_transactions_from", transactionId.toString(), 1);
    Packet clusterPacket = new Packet(0L, config.getId(), "cluster", 1, null, data);
    ReplicaClient master = getMaster();
    Future<ReplicaResponse> ack = master.sendIfStarting(clusterPacket);
    try {
      ReplicaResponse response = ack.get();
      return new ReplicaResponse(master.getId(), response.getResponse());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return new ReplicaResponse(master.getId(), 503);
    }
  }
  
  private ReplicaClient getMaster() {
    return replicaClients.get(this.masterId - 1);
  }
  
  private  List<Future<ReplicaResponse>> sendToRemoteReplicas(Packet packet) {
    List<Future<ReplicaResponse>> futureResponses = new ArrayList<>(replicaClients.size()-1);
    for (ReplicaClient replicaClient : replicaClients) {
      Future<ReplicaResponse> ack = replicaClient.sendIfUp(packet);
      futureResponses.add(ack);
    }
    return futureResponses;
  }
}
