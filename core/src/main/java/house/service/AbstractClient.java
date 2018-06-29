package house.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.api.client.KV;
import house.model.Packet;
import house.replication.ReplicaResponse;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static house.service.AbstractClient.State.DOWN;

@Slf4j
public abstract class AbstractClient {
  
  public enum State {
    UP,
    DOWN
  }
  
  WebTarget baseTarget;
  int id;
  volatile State state;
  AtomicBoolean stopped;
  AtomicLong nextTransactionId;
  
  public int getId() {
    return id;
  }
  
  public void checkHealth() {
    while (!stopped.get() && state == DOWN) {
      try {
        log.info(String.format("Trying to connect to replica %d", id));
        WebTarget target = baseTarget.path("cluster/health");
        Response response = target.request().get();
        if (response.getStatus() != 200) {
          try {
            log.info("Sleeping ...");
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            //TODO ignore for now
          }
        } else {
          this.state = State.UP;
          this.nextTransactionId = new AtomicLong(response.readEntity(Long.class));
        }
      } catch (Exception e) {
        log.info(e.getMessage(), e);
        log.info("Sleeping ...");
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ie) {
          //TODO ignore for now
          //TODO how to handle exceptions within catch block and finally block
        }
      }
    }
    if (state == DOWN) {
      log.info(String.format("Replica %d is down", id));
    } else {
      log.info("Connected.");
    }
  }
  
  ReplicaResponse sendSync(Packet packet) {
    ReplicaResponse replicaResponse;
    Long transactionId = packet.getTransactionId();
    if (state == DOWN) {
      return new ReplicaResponse(transactionId, id, 503);
    }
    try {
      log.info(String.format("Sending %d to replica %d", packet.getTransactionId(), id));
      WebTarget target = baseTarget.path("cluster/packet");
      Response response =
          target
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .post(Entity.entity(packet, MediaType.APPLICATION_JSON));
      replicaResponse = new ReplicaResponse(transactionId, id, response.getStatus());
    } catch (Exception e) {
      log.error(String.format("Failed to connect to replica %d", id), e);
      replicaResponse = new ReplicaResponse(transactionId, id, 503);
    }
    return replicaResponse;
  }
  
  public State getState() {
    return state;
  }
}