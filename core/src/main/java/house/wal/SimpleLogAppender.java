package house.wal;

import house.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class SimpleLogAppender implements LogAppender {
  private static Logger logger = LoggerFactory.getLogger(SimpleLogAppender.class);
  
  private AppConfig config;
  private FileOutputStream os;
  private BufferedWriter writer;
  
  public SimpleLogAppender(AppConfig config) throws IOException {
    this.config = config;
      os = new FileOutputStream(getFile(), true);
      writer = new BufferedWriter(new OutputStreamWriter(os));
  }
  
  @Override
  public void append(String message) throws IOException {
    //TODO change this to <size><data><size><data><size><data>...
    writer.write(message);
    writer.newLine();
    writer.flush();
  }
  
  @Override
  public void getLastOffset() {
  }
  
  @Override
  public void close() throws IOException {
    writer.close();
  }
  
  public void readLines() throws IOException {
    InputStream is = new FileInputStream(getFile());
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    String next = reader.readLine();
    while (next != null) {
      System.out.println(next);
      next = reader.readLine();
    }
    reader.close();
  }
  
  private File getFile() {
    StringBuilder sb = new StringBuilder(config.getWalDir());
    return new File(sb.append("/wal.log").toString());
  }
  
//  public static void main(String[] args) throws Exception {
//
//    Map<String, Object> map = new HashMap<>();
//    map.put("port", 1);
//    List<String> list = new ArrayList<>();
//    list.add("localhost");
//    map.put("replicas", list);
//    map.put("master", false);
//    map.put("wal-dir", "/tmp");
//    AppConfig config = new AppConfig(ConfigFactory.parseMap(map));
//    SimpleLogAppender appender = new SimpleLogAppender(config);
//    appender.append("Hello");
//    appender.append("World");
//    appender.close();
//    appender.readLines();
//  }
  
}
