//package house.store;
//
//import house.AppConfig;
//import house.exception.ApplicationException;
//import house.replication.ReplicationStrategy;
//import house.service.Packet;
//import house.service.ReplicaClient;
//import lombok.extern.slf4j.Slf4j;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.LinkedList;
//import java.util.List;
//
//@Slf4j
//public class StoreReader {
//
//    AppConfig config;
//    ReplicationStrategy replicationStrategy;
//    ReplicaClient master;
//
//    public StoreReader(AppConfig config, ReplicationStrategy replicationStrategy, ReplicaClient master) {
//        this.config = config;
//        this.replicationStrategy = replicationStrategy;
//        this.master = master;
//    }
//
//    public void start() {
//        try {
//            readLines(replicationStrategy::processWalPacket);
//            if (!config.isMaster()) {
//                readFromMaster();
//            }
//        } catch (IOException e) {
//            log.error(e.getMessage(), e);
//            throw new ApplicationException("Failed to load data", e);
//        }
//    }
//
//    public List<Packet> getTransactionsFrom(Long transactionId) throws IOException {
//        List<Packet> packets = new LinkedList<>();
//        readLines(packet -> {
//            if (packet.getTransactionId() >= transactionId) {
//                packets.add(packet);
//            }});
//        return packets;
//    }
//
//
//
//
//
//    private File getFile() {
//        StringBuilder sb = new StringBuilder(config.getWalDir());
//        return new File(sb.append("/wal.log").toString());
//    }
//}
