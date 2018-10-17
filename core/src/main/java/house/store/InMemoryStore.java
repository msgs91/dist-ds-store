package house.store;

import house.AppConfig;
import house.exception.ApplicationException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class InMemoryStore {
  private Map<String, String> kv;
  private final AppConfig config;
  private AtomicLong nextTransactionId;

  public InMemoryStore(AppConfig config) {
    this.kv = new HashMap<>();
    this.config = config;
    this.nextTransactionId = new AtomicLong(1);
  }

  //TODO use concurrent hashmap instead
  synchronized public Optional<String> get(String key) {
    return Optional.ofNullable(kv.get(key));
  }
  
  synchronized public Optional<String> put(Long transactionId, String key, String value) {
    boolean isSet = nextTransactionId.compareAndSet(transactionId, transactionId+1);
    if (isSet) {
      return Optional.ofNullable(kv.put(key, value));
    }
    String message =
            String.format("Given transaction id %d is not equal to next transaction id %d", transactionId, nextTransactionId.get());
    log.warn(message);
    throw new ApplicationException(message);
  }

  synchronized public Long getNextTransactionId() {
    return nextTransactionId.get();
  }

  public void setNextTransactionId(Long newTransactionId) {
    if (newTransactionId > nextTransactionId.get()) {
      nextTransactionId.set(newTransactionId);
    }
  }
}
