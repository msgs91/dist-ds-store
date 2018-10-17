package house.service;

import house.exception.ApplicationException;
import house.model.Packet;
import house.replication.ReplicaResponse;
import house.wal.WalReader;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static house.service.AbstractClient.State.DOWN;

@Slf4j
public class ReplicaClient extends AbstractClient {

  private final WalReader reader;
  private final BlockingQueue<ReplicaResponse> responseQueue;

  public ReplicaClient(int id, String uri, WalReader reader, BlockingQueue<ReplicaResponse> responseQueue) {
    Client client = ClientBuilder.newClient();
    client.property(ClientProperties.CONNECT_TIMEOUT, 300);
    client.property(ClientProperties.READ_TIMEOUT,    300);
    baseTarget = client.target(uri);
    this.id = id;
    this.state = DOWN;
    this.reader = reader;
    this.responseQueue =  responseQueue;
    this.stopped = new AtomicBoolean(true);
    this.nextTransactionId = new AtomicLong(1L);
  }

  public void start(Long transactionId) {
    if (stopped.get()) {
      stopped.set(false);
      nextTransactionId.set(transactionId);
      log.info(String.format("Starting replicator thread for replica id %d", id));
      new Thread(() -> replicateFrom(), String.format("Replicator-thread-%d", id)).start();
    }
  }

  public void stop() {
    stopped.set(true);
  }

  private void replicateFrom() {
    while (!stopped.get()) {
      checkHealth();
      reader.seek(nextTransactionId.get());
      while (!stopped.get() && state != DOWN) {
        Optional<Packet> maybePacket = reader.readNext();
        maybePacket.map(packet -> {
          ReplicaResponse response = sendSync(packet);
          if (response.getResponse() == 200) {
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
  }
}
