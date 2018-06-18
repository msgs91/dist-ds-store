package house.replication;

public class ReplicaResponse {

    int id;
    int response;

    public ReplicaResponse(int id, int response) {
        this.id = id;
        this.response = response;
    }

    public int getId() {
        return id;
    }

    public int getResponse() {
        return response;
    }
}
