package house.model;

public class Packet {

    private Long transactionId;
    private int replicaId;

    private String type;
    private int version;

    private Meta meta;
    private Data data;

    public Packet(Long transactionId, int replicaId, String type, int version, Meta meta, Data data) {
        this.transactionId = transactionId;
        this.replicaId = replicaId;
        this.type = type;
        this.version = version;
        this.meta = meta;
        this.data = data;
    }

    public Packet() { }

    public Long getTransactionId() {
        return transactionId;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public String getType() {
        return type;
    }

    public int getVersion() {
        return version;
    }

    public Meta getMeta() {
        return meta;
    }

    public Data getData() {
        return data;
    }

    public Packet setTransactionId(Long transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public Packet setReplicaId(int replicaId) {
        this.replicaId = replicaId;
        return this;
    }

    public Packet setType(String type) {
        this.type = type;
        return this;
    }

    public Packet setVersion(int version) {
        this.version = version;
        return this;
    }

    public Packet setMeta(Meta meta) {
        this.meta = meta;
        return this;
    }

    public Packet setData(Data data) {
        this.data = data;
        return this;
    }
}
