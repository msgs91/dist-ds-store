package house.service;

import house.model.Packet;
import house.model.Packets;
import house.replication.ReplicaResponse;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.client.ClientBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

import static house.service.AbstractClient.State.DOWN;

@Slf4j
public class MasterClient extends AbstractClient {
  
  public MasterClient(String uri) {
    baseTarget = ClientBuilder.newClient().target(uri);
    stopped = new AtomicBoolean(false);
    state = DOWN;
    checkHealth();
  }
  
  public void stop() {
    stopped.set(true);
  }
  
  public boolean getTransactionsFrom(Long transactionId) {
    Packet packet = Packets.clusterGetTransactionsFrom(transactionId);
    ReplicaResponse response = sendSync(packet);
    if (response.getResponse() != 200) {
      log.info(String.format("Master replied with error while connecting"));
      return false;
    }
    return true;
  }
  
}
