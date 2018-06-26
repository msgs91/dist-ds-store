package house.replication;

public class ReplicaResponse {

    private int replicaId;
    private int response;
    private Long transactionId;

    public ReplicaResponse(Long transactionId, int replicaId, int response) {
        this.transactionId = transactionId;
        this.replicaId = replicaId;
        this.response = response;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public Long getTransactionId() {
        return transactionId;
    }

    public int getResponse() {
        return response;
    }
}
