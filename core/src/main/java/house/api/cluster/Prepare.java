package house.api.cluster;

public class Prepare {
    public final static String MESSAGE_TYPE = "prepare";

    Long transactionId;

    public Prepare(Long transactionId) {
        this.transactionId = transactionId;
    }

    public Long getTransactionId() {
        return transactionId;
    }
}
