package house.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.service.Packet;
import house.service.ReplicaClient;
import house.wal.LogAppender;
import house.wal.SimpleLogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReplicatorImpl implements Replicator {
    private static Logger logger = LoggerFactory.getLogger(ReplicatorImpl.class);

    int id;
    List<ReplicaClient> replicaClients;
    LogAppender wal;

    public ReplicatorImpl(AppConfig config) throws IOException {
        this.id = config.getId();
        this.replicaClients = new LinkedList<ReplicaClient>();
        List<String> replicaUrls = config.getReplicas();
        for (int i = 1; i <= replicaUrls.size(); i++) {
            if (i != config.getId()) {
                this.replicaClients.add(new ReplicaClient(i, replicaUrls.get(i-1)));
            }
        }
        this.wal = new SimpleLogAppender(config);
    }

    @Override
    public ReplicaResponse replicateLocally(Packet packet) {
        ReplicaResponse response;
        try {
            String message = new ObjectMapper().writeValueAsString(packet);
            wal.append(message);
            response = new ReplicaResponse(id, 200);
        } catch (IOException e) {
            response = new ReplicaResponse(id, 503);
        }
        return response;
    }

    private List<ReplicaResponse> sendToRemoteReplicas(Packet packet) {
        List<ReplicaResponse> replicaResponses = new LinkedList<>();
        for (ReplicaClient replicaClient: replicaClients) {
            Future<Response> ack = replicaClient.sendMessage(packet);
            try {
                //TODO add retries
                Response response = ack.get(30, TimeUnit.SECONDS);
                replicaResponses.add(new ReplicaResponse(replicaClient.getId(), response.getStatus()));
                if (response.getStatus() != 200) {
                    String msg = String.format("Failed to replicate to replica %d", replicaClient.getId());
                    logger.error(msg);
                }
            } catch (TimeoutException e) {
                replicaResponses.add(new ReplicaResponse(replicaClient.getId(), 503));
                logger.error(String.format("Request to replica %d timed out", replicaClient.getId()), e);
            } catch (ExecutionException e) {
                replicaResponses.add(new ReplicaResponse(replicaClient.getId(), 503));
                logger.error(String.format("Failed to connect to replica %d", replicaClient.getId()), e);
            } catch (InterruptedException e) {
                replicaResponses.add(new ReplicaResponse(replicaClient.getId(), 503));
                logger.error(String.format("Request to replica %d interrupted", replicaClient.getId()), e);
            }
        }
        return replicaResponses;
    }

    @Override
    public List<ReplicaResponse> sendToReplicas(Packet packet) {
        ReplicaResponse localResponse = replicateLocally(packet);
        List<ReplicaResponse> remoteResponses = sendToRemoteReplicas(packet);
        remoteResponses.add(localResponse);
        return remoteResponses;
    }

    @Override
    public int getId() {
        return this.id;
    }
}
