package house.service;

import com.google.common.util.concurrent.SettableFuture;
import house.replication.ReplicaResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static house.service.ReplicaClient.State.DOWN;
import static house.service.ReplicaClient.State.UP;

@Slf4j
public class ReplicaClient {
  
  public enum State {
    UP,
    DOWN,
    STARTING;
  }
  
  @Getter
  @Setter
  @AllArgsConstructor
  class QueueItem {
    @NotNull Packet packet;
    @NotNull SettableFuture<ReplicaResponse> response;
  }
  
  private WebTarget baseTarget;
  ArrayBlockingQueue<QueueItem> queue;
  
  private int id;
  
  private volatile State state;
  
  public ReplicaClient(int id, String uri) {
    baseTarget = ClientBuilder.newClient().target(uri);
    this.id = id;
    this.state = State.STARTING;
    this.queue = new ArrayBlockingQueue<>(10);
    
    new Thread(this::sendAndRespond, String.format("ReplicaClient-%d", this.id)).stop();
    checkHealth();
  }
  
  public Future<ReplicaResponse> put(Packet packet) {
    SettableFuture<ReplicaResponse> promise = SettableFuture.create();
    QueueItem item = new QueueItem(packet, promise);
    try {
      queue.put(item);
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
    return promise;
  }
  
  
  
  public int getId() {
    return id;
  }
  
  public Future<ReplicaResponse> sendIfUp(Packet packet) {
    if (state != UP) {
      return new FutureTask<>(() -> new ReplicaResponse(id, 503));
    }
    return send(packet);
  }
  
  public Future<ReplicaResponse> sendIfStarting(Packet packet) {
    if (state != State.STARTING) {
      return new FutureTask<>(() -> new ReplicaResponse(id, 503));
    }
    return send(packet);
  }
  
  private void sendAndRespond() {
    while (state != DOWN) {
      SettableFuture<ReplicaResponse> replicaResponseFuture;
      try {
        QueueItem item = queue.take();
        replicaResponseFuture = item.getResponse();
        Packet packet = item.getPacket();
        log.info(String.format("Sending %d to replica %d", packet.getTransactionId(), id));
        WebTarget target = baseTarget.path("cluster/packet");
        Response response =
          target
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(packet, MediaType.APPLICATION_JSON));
        
        if (response.getStatus() != 200) {
          String msg = String.format("Failed to replicate to replica %d", id);
          log.error(msg);
        }
        replicaResponseFuture.set(new ReplicaResponse(id, response.getStatus()));
      } catch (InterruptedException e) {
      
      } catch (Exception e) {
        log.error(String.format("Failed to connect to replica %d", id), e);
        replicaResponseFuture.set(new ReplicaResponse(id, 503));
        
      }
      this.state = DOWN;
    }
  }
  
  public State checkHealth() {
    WebTarget target = baseTarget.path("cluster/health");
    Response response = target.request().get();
    if (response.getStatus() != 200) {
      this.state = DOWN;
    } else {
      this.state = State.STARTING;
    }
    return state;
  }
  
  public State getState() {
    return state;
  }
  
  public void setState(State state) {
    this.state = state;
  }
}
