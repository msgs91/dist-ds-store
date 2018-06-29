package house;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

public class AppConfig {
  
  private int port;
  private String host;
  private List<String> replicas;
  private boolean isMaster;
  private String walDir;
  private String replicationStrategy;
  private int id;

  public AppConfig(Config config) throws Exception {
    id = config.getInt("id");
    replicas = config.getStringList("replicas");
    String selfUri = replicas.get(id-1);
    String[] parts = selfUri.split(":");
    host = parts[0];
    port = Integer.parseInt(parts[1]);
    isMaster = (id == 1);
    walDir = config.getString("wal-dir");
    replicationStrategy = config.getString("replicationStrategy");
  }

  public static AppConfig load(String configFilePath) throws Exception {
    Reader reader = new InputStreamReader(new FileInputStream(configFilePath));
    Config config = ConfigFactory.parseReader(reader);

    return new AppConfig(config);
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public List<String> getReplicas() {
    return replicas;
  }
  
  public boolean isMaster() {
    return isMaster;
  }
  
  public String getWalDir() {
    return walDir;
  }

  public String getReplicationStrategy() {
    return replicationStrategy;
  }

  public int getId() {
    return id;
  }
}
