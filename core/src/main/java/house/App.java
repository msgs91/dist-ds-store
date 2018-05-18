package house;

import house.api.KVApplication;
import house.service.KVService;
import house.store.InMemoryStore;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class App {
  
  private static Logger logger = LoggerFactory.getLogger(App.class);
  
  public static void main(String[] args) throws Exception {
    AppConfig config = AppConfig.load(args[0]);
  
    URI baseUri = UriBuilder.fromUri("http://localhost").port(config.getPort()).build();
    KVService service = new KVService(new InMemoryStore());
    KVApplication schemaRegistryApplication = new KVApplication(service);
    Server server = JettyHttpContainerFactory.createServer(baseUri, schemaRegistryApplication);
  
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        logger.info("Shutting down schema registry");
        try {
          server.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }, "ShutdownHook"));
    
    server.start();
  }
}
