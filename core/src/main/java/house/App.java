package house;

import house.api.KVApplication;
import house.exception.ApplicationException;
import house.replication.*;
import house.service.KVService;
import house.store.InMemoryStore;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;

@Slf4j
public class App {
  
  public static void main(String[] args) throws Exception {
    AppConfig config = AppConfig.load(args[0]);
    
    URI baseUri = UriBuilder.fromUri("http://" + config.getHost()).port(config.getPort()).build();
    InMemoryStore store = new InMemoryStore(config);
    Replicator replicator = new ReplicatorImpl(config);
    replicator.start();
    ReplicationStrategy replicationStrategy = getReplicationStrategy(config, replicator, store);
    KVService service = new KVService(config, replicator, replicationStrategy, store);
    service.start();
    KVApplication kvApplication = new KVApplication(service);
    
    Server server = JettyHttpContainerFactory.createServer(baseUri, kvApplication);
    
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down");
      try {
        server.stop();
        replicator.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "ShutdownHook"));
    
    server.start();
  }
  
  private static ReplicationStrategy getReplicationStrategy(AppConfig config, Replicator replicator, InMemoryStore store) throws IOException {
    switch (config.getReplicationStrategy()) {
      case "bestEffort" :
        return new BestEffortReplicationStrategy(config, replicator, store);
      case "twoPhaseCommit" :
        return new TwoPhaseCommitStrategy(config, replicator, store, TwoPhaseCommitStrategy.Ack.ALL);
      default:
        throw new ApplicationException(String.format("Invalid replication strategy %s", config.getReplicationStrategy()));
    }
  }
}
