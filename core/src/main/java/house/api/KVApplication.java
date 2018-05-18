package house.api;

import house.api.client.StoreResource;
import house.api.cluster.ClusterResource;
import house.service.KVService;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

public class KVApplication extends ResourceConfig {
  
  public KVApplication(KVService service) {
    register(new StoreResource(service));
    register(new ClusterResource(service));
    //to support responses in application/json
    register(JacksonFeature.class);
    property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);
  }
}
