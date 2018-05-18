package house.api.cluster;

import house.service.KVService;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("cluster")
public class ClusterResource {
  
  KVService service;
  
  public ClusterResource(KVService service) {
    this.service = service;
  }
  
  @GET
  @Path("health")
  public Response getHealth() {
    return Response.ok().build();
  }
  
  @POST
  @Path("message")
  public Response sendMessage(Message message) {
    boolean isSuccess = service.onMessage(message);
    Response response;
    if (isSuccess) {
      response = Response.ok().build();
    } else {
      response = Response.serverError().build();
    }
    return response;
  }
}
