package house.wal;

import house.api.cluster.MessageOld;

import java.io.IOException;

public interface LogAppender {
  
  public void append(String message) throws IOException;
  
  public void getLastOffset();
  
  public void close() throws IOException;
    
}
