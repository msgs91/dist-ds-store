package house.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.api.response.PacketsResponse;
import house.exception.ApplicationException;
import house.replication.ReplicationStrategy;
import house.service.Data;
import house.service.Packet;
import house.service.ReplicaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class StoreReader {
    private static Logger logger = LoggerFactory.getLogger(StoreReader.class);

    AppConfig config;
    ReplicationStrategy replicationStrategy;
    ReplicaClient master;

    public StoreReader(AppConfig config, ReplicationStrategy replicationStrategy, ReplicaClient master) {
        this.config = config;
        this.replicationStrategy = replicationStrategy;
        this.master = master;
    }

    public void start() {
        try {
            readLines(replicationStrategy::processWalPacket);
            if (!config.isMaster()) {
                readFromMaster();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new ApplicationException("Failed to load data", e);
        }
    }

    public List<Packet> getTransactionsFrom(Long transactionId) throws IOException {
        List<Packet> packets = new LinkedList<>();
        readLines(packet -> {
                    if (packet.getTransactionId() >= transactionId) {
                        packets.add(packet);
                    }});
        return packets;
    }

    private void readFromMaster() {
        Long lastTransactionId = replicationStrategy.getNextTransactionId();
        boolean end = false;
        while (!end) {
            logger.info(String.format("Reading transactions from master starting from %d", lastTransactionId));
            Data data = new Data("get_transactions_from", lastTransactionId.toString(), 1);
            Packet clusterPacket = new Packet(0L, config.getId(), "cluster", 1, null, data);
            Future<Response> ack = master.sendMessage(clusterPacket);
            try {
                Response response = ack.get();
                PacketsResponse packetsResponse = response.readEntity(PacketsResponse.class);
                List<Packet> packets = packetsResponse.getPackets();
                for (Packet packet : packets) {
                    replicationStrategy.handlePacket(packet);
                }
                lastTransactionId = replicationStrategy.getNextTransactionId();
                if (packets.size() == 0) {
                    end = true;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void readLines(Consumer<Packet> fn) throws IOException {
        try {
            InputStream is = new FileInputStream(getFile());
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String next = reader.readLine();
            ObjectMapper mapper = new ObjectMapper();
            while (next != null) {
                Packet packet = mapper.readValue(next, Packet.class);
                fn.accept(packet);
                next = reader.readLine();
            }
            reader.close();
        } catch (FileNotFoundException e) {
            logger.info("No wal file found");
        }

    }

    private File getFile() {
        StringBuilder sb = new StringBuilder(config.getWalDir());
        return new File(sb.append("/wal.log").toString());
    }
}
