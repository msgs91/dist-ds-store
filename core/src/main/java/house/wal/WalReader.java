package house.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import house.AppConfig;
import house.exception.ApplicationException;
import house.model.Packet;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;

@Slf4j
public class WalReader {

    RandomAccessFile file;
    AppConfig config;

    public WalReader(AppConfig config) throws IOException {
        this.config = config;
        this.file = new RandomAccessFile(getFile(), "r");
    }

    public Optional<Packet> readNext() {
        try {
            String next = file.readLine();
            if (next == null) {
                return Optional.empty();
            }
            return Optional.of(new ObjectMapper().readValue(next, Packet.class));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new ApplicationException(e.getMessage(), e);
        }
    }

    public void seek(Long transactionId) {
        try {
            file.seek(0L);
            String next = file.readLine();
            Long currentTransactionId;
            while (next != null) {
                Packet packet = new ObjectMapper().readValue(next, Packet.class);
                currentTransactionId = packet.getTransactionId();
                if (currentTransactionId == transactionId - 1) {
                    break;
                }
                next = file.readLine();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new ApplicationException(e);
        }
    }

    public void close() throws IOException {
        file.close();
    }

    private File getFile() {
        StringBuilder sb = new StringBuilder(config.getWalDir());
        return new File(sb.append("/wal.log").toString());
    }
}
