package house.service;

import house.exception.ApplicationException;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ReplicaClient {
  
  private WebTarget baseTarget;
  private ExecutorService executorService;
  private int id;

  public ReplicaClient(int id, String uri) {
    baseTarget = ClientBuilder.newClient().target(uri);
    executorService = Executors.newSingleThreadExecutor();
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public Future<Response> sendMessage(Packet packet) {
    Callable<Response> callable = () -> {
      WebTarget target = baseTarget.path("cluster/packet");
  
      Response response =
            target
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .post(Entity.entity(packet, MediaType.APPLICATION_JSON));
  
      if (response.getStatus() != 200) {
        throw new ApplicationException(
            String.format("Failed to send %s to replica %d", packet.getType(), packet.getReplicaId())
        );
      }
      return response;
    };
    
    Future<Response> ack = executorService.submit(callable);
    return ack;
  }
}
