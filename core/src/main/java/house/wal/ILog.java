package house.wal;

import house.service.Packet;

import java.io.IOException;
import java.util.List;

public interface ILog {
  
  void append(String message) throws IOException;
  
  void getLastOffset();
  
  void close() throws IOException;
  
  List<String> readLines() throws IOException;
}
