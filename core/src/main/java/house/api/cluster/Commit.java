package house.api.cluster;

public class Commit {
    public final static String MESSAGE_TYPE = "commit";

    Long transactionId;
    String data;

    public Commit(Long transactionId, String data) {
        this.transactionId = transactionId;
        this.data = data;
    }

    public Long getTransactionId() {
        return transactionId;
    }

    public String getData() {
        return data;
    }
}
