package house.service;

import house.exception.ApplicationException;
import house.replication.ReplicaResponse;
import house.wal.WalReader;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static house.service.ReplicaClient.State.DOWN;

@Slf4j
public class ReplicaClient {

  public enum State {
    UP,
    DOWN,
    STARTING;
  }

  private WebTarget baseTarget;
  private int id;
  private volatile State state;
  private WalReader reader;
  Long lastSeenTransactionId;
  BlockingQueue<ReplicaResponse> responseQueue;
  AtomicBoolean stopped;

  public ReplicaClient(int id, String uri, WalReader reader, BlockingQueue<ReplicaResponse> responseQueue) {
    baseTarget = ClientBuilder.newClient().target(uri);
    this.id = id;
    this.state = State.STARTING;
    this.reader = reader;
    this.lastSeenTransactionId = 0L;
    this.responseQueue =  responseQueue;
    this.stopped = new AtomicBoolean(false);
    checkHealth();
    new Thread(this::replicate, String.format("Replicator-thread-%s", id)).start();
  }

  public void stop() {
    stopped.set(true);
  }

  public int getId() {
    return id;
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

  private void replicate() {
    while (!stopped.get() && state != DOWN) {
      Optional<Packet> maybePacket = reader.readNext();
      maybePacket.map(packet -> {
        ReplicaResponse response = sendSync(packet);
        if (response.getResponse() == 200) {
          lastSeenTransactionId = packet.getTransactionId();
        } else {
          String msg = String.format("Failed to replicate to replica %d", id);
          this.state = DOWN;
          log.error(msg);
        }
        try {
          responseQueue.put(response);
        } catch (InterruptedException e) {
          String message = String.format("Failed to put response for transaction id %d", packet.getTransactionId());
          log.error(message, e);
          throw new ApplicationException(message, e);
        }
        return 0;
      });
    }
  }

  private ReplicaResponse sendSync(Packet packet) {
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
