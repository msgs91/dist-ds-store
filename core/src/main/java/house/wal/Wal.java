package house.wal;

import house.AppConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class Wal implements ILog {
  
  private AppConfig config;
  private FileOutputStream os;
  private BufferedWriter writer;
  
  public Wal(AppConfig config) throws IOException {
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
  
  public List<String> readLines() throws IOException {
    List<String> lines = new LinkedList<>();
    try {
      InputStream is = new FileInputStream(getFile());
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String next = reader.readLine();
      while (next != null) {
        lines.add(next);
        next = reader.readLine();
      }
      reader.close();
    } catch (FileNotFoundException e) {
      log.info("No wal file found");
    }
    return lines;
  }
  
  private File getFile() {
    StringBuilder sb = new StringBuilder(config.getWalDir());
    return new File(sb.append("/wal.log").toString());
  }
}
