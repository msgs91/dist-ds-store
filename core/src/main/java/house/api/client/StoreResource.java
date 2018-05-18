package house.api.client;

import house.service.KVService;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

@Path("store")
@Produces(MediaType.APPLICATION_JSON)
public class StoreResource {
  
  private KVService service;
  
  public StoreResource(KVService service) {
    this.service = service;
  }
  
  @GET
  public Response get(@QueryParam("key") String key) {
    Optional<String> maybeValue = service.get(key);
    String value;
    if (maybeValue.isPresent()) {
      value = maybeValue.get();
    } else {
      value = "undefined";
    }
    
    return Response.ok().entity(new KV(key, value)).build();
  }
  
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public Response put(KV kv) {
    service.put(kv.getKey(), kv.getValue());
    return Response.ok().entity(new KV(kv.getKey(), kv.getValue())).build();
  }
}
