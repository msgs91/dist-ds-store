package house.replication;

public class Replica {
    int id;
    String uri;

    public Replica(int id, String uri) {
        this.id = id;
        this.uri = uri;
    }

    public int getId() {
        return id;
    }

    public String getUri() {
        return uri;
    }
}
