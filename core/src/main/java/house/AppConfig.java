package house;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
    
public class AppConfig {
  
  private int port;
  private String[] replicas;
  private boolean isMaster;
  
  public AppConfig(Config config) throws Exception {
    port = config.getInt("port");
    replicas = new String[1];
    replicas = config.getStringList("replicas").toArray(replicas);
    isMaster = config.getBoolean("master");
  }
  
  public static AppConfig load(String configFilePath) throws Exception {
    Reader reader = new InputStreamReader(new FileInputStream(configFilePath));
    Config config = ConfigFactory.parseReader(reader);
    
    return new AppConfig(config);
  }
  
  public int getPort() {
    return port;
  }
  
  public String[] getReplicas() {
    return replicas;
  }
  
  public boolean isMaster() {
    return isMaster;
  }
}
