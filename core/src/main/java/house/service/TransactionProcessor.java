package house.service;

import com.google.common.util.concurrent.SettableFuture;
import house.exception.ApplicationException;
import house.model.Packet;
import house.replication.ReplicationStrategy;
import house.replication.Replicator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
public class TransactionProcessor {

    @Getter
    @AllArgsConstructor
    class Item {
        @NotNull
        Packet packet;
        @NotNull SettableFuture<TransactionResponse> responseFuture;
    }

    Replicator replicator;
    BlockingQueue<Item> transactions;
    Thread thread;

    public TransactionProcessor(Replicator replicator){
        this.replicator = replicator;
        this.transactions = new LinkedBlockingDeque<>(10);
        this.thread = new Thread(this::processTransaction, "TransactionProcessor");
    }

    public void start() {
        thread.start();
    }

    public void stop() {
        this.stop();
    }

    public Future<TransactionResponse> put(Packet packet){
        SettableFuture<TransactionResponse> response = SettableFuture.create();
        try {
            transactions.put(new Item(packet, response));
        } catch (InterruptedException e) {
            String message = "Unable to process transaction";
            response.setException(new ApplicationException(message));
            log.error(message, e);
        }
        return response;
    }

    private void processTransaction() {
        while (true) {
            try {
                Item item = transactions.take();
                Packet packet = item.getPacket();
                replicator.replicateLocally(packet);
                item.getResponseFuture().set(new TransactionResponse(200, 1L));
            } catch (InterruptedException e) {
                String message = "Failed to read transactions queue";
                log.error(message, e);

            }
        }
    }
}
