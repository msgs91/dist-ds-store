package house.api.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterResponse {
    String type;
    String entity;
    boolean error;

    @JsonCreator
    public ClusterResponse(@JsonProperty("type") String type,
                           @JsonProperty("entity") String entity,
                           boolean error) {
        this.type = type;
        this.entity = entity;
        this.error = error;
    }

    public String getType() {
        return type;
    }

    public String getEntity() {
        return entity;
    }

    public boolean isError() {
        return error;
    }
}
