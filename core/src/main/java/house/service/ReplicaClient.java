package house.service;

import house.api.cluster.Message;
import house.exception.ApplicationException;

import javax.ws.rs.client.Client;
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
  
  public ReplicaClient(String uri) {
    baseTarget = ClientBuilder.newClient().target(uri);
    executorService = Executors.newSingleThreadExecutor();
  }
  
  public void sendMessage(Message message) {
    Callable<Response> callable = () -> {
      WebTarget target = baseTarget.path("message");
  
      Response response =
            target
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .post(Entity.entity(message, MediaType.APPLICATION_JSON));
  
      if (response.getStatus() != 200) {
        throw new ApplicationException(
            String.format("Failed to send %s to uri", message.getType())
        );
      }
      return response;
    };
    
    Future<Response> ack = executorService.submit(callable);
  }
}
