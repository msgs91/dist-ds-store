package house.api.cluster;

import house.api.response.ClusterResponse;
import house.model.Packet;
import house.service.KVService;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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
    return Response.ok().entity(service.nextTransactionId()).build();
  }
  
  @POST
  @Path("packet")
  @Produces("application/json")
  public Response sendPacket(Packet packet) {
    ClusterResponse clusterResponse = service.onPacket(packet);
    if (!clusterResponse.isError()) {
      return Response.ok().entity(clusterResponse.getEntity()).build();
    } else {
      return Response.status(503).build();
    }
  }
}
