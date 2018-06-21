package house.service;

import house.replication.ReplicaResponse;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static house.service.ReplicaClient.State.UP;

@Slf4j
public class ReplicaClient {
  
  public enum State {
    UP,
    DOWN,
    STARTING;
  }

  private WebTarget baseTarget;
  private ExecutorService executorService;
  private int id;
  
  private volatile State state;
  
  public ReplicaClient(int id, String uri) {
    baseTarget = ClientBuilder.newClient().target(uri);
    executorService = Executors.newSingleThreadExecutor();
    this.id = id;
    this.state = State.STARTING;
    checkHealth();
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
  
  private Future<ReplicaResponse> send(Packet packet) {
    Callable<ReplicaResponse> callable = () -> {
      log.info(String.format("Sending %d to replica %d", packet.getTransactionId(), id));
      ReplicaResponse replicaResponse;
      WebTarget target = baseTarget.path("cluster/packet");
      try {
        Response response =
          target
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(packet, MediaType.APPLICATION_JSON));
        
        if (response.getStatus() != 200) {
          String msg = String.format("Failed to replicate to replica %d", id);
          log.error(msg);
        }
        replicaResponse = new ReplicaResponse(id, response.getStatus());
      } catch (Exception e ) {
        log.error(String.format("Failed to connect to replica %d", id), e);
        replicaResponse = new ReplicaResponse(id, 503);
      }
      this.state = State.DOWN;
      return replicaResponse;
    };
    
    return executorService.submit(callable);
  }
  
  public State checkHealth() {
    WebTarget target = baseTarget.path("cluster/health");
    Response response = target.request().get();
    if (response.getStatus() != 200) {
      this.state = State.DOWN;
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
