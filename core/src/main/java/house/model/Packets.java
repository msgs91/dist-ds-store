package house.model;

public class Packets {
    
    public static Packet clusterGetTransactionsFrom(Long transactionId) {
        Data data = new Data("get_transactions_from", transactionId.toString(), 1);
        return cluster().setData(data).setVersion(1);
    }
    
    public static Packet cluster() {
        return new Packet().setType("cluster");
    }
}
