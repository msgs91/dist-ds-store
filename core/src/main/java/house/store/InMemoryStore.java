package house.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryStore {
  
  private Map<String, String> kv;
  
  public InMemoryStore() {
    this.kv = new HashMap<>();
  }
  
  //TODO use concurrent hashmap instead
  synchronized public Optional<String> get(String key) {
    return Optional.ofNullable(kv.get(key));
  }
  
  synchronized public Optional<String> put(String key, String value) {
    return Optional.ofNullable(kv.put(key, value));
  }
}
