package house.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import house.api.client.KV;
import house.api.cluster.Message;
import house.exception.ApplicationException;
import house.store.InMemoryStore;

import java.io.IOException;
import java.util.Optional;

import static house.api.cluster.Message.MessageType.*;

public class KVService {
  
  InMemoryStore store;
  ReplicaClient replicaClient;
  
  public KVService(InMemoryStore store) {
    this.store = store;
  }
  
  public Optional<String> get(String key) {
    return store.get(key);
  }
  
  public Optional<String> put(String key, String value) {
    Optional<String> oldValue = store.put(key, value);
    sendReplicateKV(key, value);
    return oldValue;
  }
  
  public boolean onMessage(Message message) {
    System.out.println(String.format("Got message %s", message.getType()));
    boolean success;
    switch(message.getType()) {
      case REPLICATE_KV:
        success = handleReplicateKV(message);
        break;
      default:
        success = false;
        break;
    }
    return success;
  }
  
  private boolean handleReplicateKV(Message message) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      KV kv = mapper.readValue(message.getData(), KV.class);
      store.put(kv.getKey(), kv.getValue());
      return true;
    } catch (IOException e) {
      throw new ApplicationException(e);
    }
  }
  
  private boolean sendReplicateKV(String key, String value) {
    try {
      Message message = new Message();
      message.setType(REPLICATE_KV);
      ObjectMapper mapper = new ObjectMapper();
      message.setData(mapper.writeValueAsString(new KV(key, value)));
      replicaClient.sendMessage(message);
    } catch (JsonProcessingException e) {
      throw new ApplicationException(e);
    }
    return true;
  }
}
