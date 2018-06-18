package house;

import house.api.KVApplication;
import house.exception.ApplicationException;
import house.replication.*;
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
  
    URI baseUri = UriBuilder.fromUri("http://" + config.getHost()).port(config.getPort()).build();
    Replicator replicator = new ReplicatorImpl(config);
    ReplicationStrategy replicationStrategy = getReplicationStrategy(config, replicator);
    KVService service = new KVService(config, replicationStrategy);
    KVApplication schemaRegistryApplication = new KVApplication(service);

    Server server = JettyHttpContainerFactory.createServer(baseUri, schemaRegistryApplication);
  
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        logger.info("Shutting down");
        try {
          server.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }, "ShutdownHook"));
    
    server.start();
  }

  private static ReplicationStrategy getReplicationStrategy(AppConfig config, Replicator replicator) {
    switch (config.getReplicationStrategy()) {
      case "bestEffort" :
        return new BestEffortReplicationStrategy(replicator, new InMemoryStore(config));
      case "twoPhaseCommit" :
        return new TwoPhaseCommitStrategy(config, replicator, new InMemoryStore(config));
      default:
        throw new ApplicationException(String.format("Invalid replication strategy %s", config.getReplicationStrategy()));
    }
  }
}
