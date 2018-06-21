package house.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import house.api.client.KV;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServiceResponse {
  
  boolean error;
  String message;
  
  @JsonProperty("data")
  KV kv;
  
  @JsonCreator
  public ServiceResponse(@JsonProperty("error") boolean error,
                         @JsonProperty("message") String message,
                         @JsonProperty("data") KV kv) {
    this.error = error;
    this.message = message;
    this.kv = kv;
  }
  
  public boolean isError() {
    return error;
  }
  
  public String getMessage() {
    return message;
  }
  
  public KV getKv() {
    return kv;
  }
}
