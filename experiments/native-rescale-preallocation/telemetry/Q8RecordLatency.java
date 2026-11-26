package be.uclouvain.gepiciad.nexmark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sampled wall-clock witnesses for auction emission and delivery to the existing Q8 sink. */
public final class Q8RecordLatency {
    private static final Logger LOG = LoggerFactory.getLogger(Q8RecordLatency.class);
    private static final String HEX = "0123456789abcdef";
    private static final long SAMPLE_MASK = 511;

    private Q8RecordLatency() {}

    private static boolean sampled(Auction auction) {
        return ((auction.id ^ auction.dateTime ^ auction.seller ^ auction.reserve) & SAMPLE_MASK) == 0;
    }

    private static <T> String fingerprint(Coder<T> coder, T event) throws IOException {
        // Replay can generate distinct records with the same Nexmark model ID.
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        coder.encode(event, encoded);
        byte[] hash;
        try {
            hash = MessageDigest.getInstance("SHA-256").digest(encoded.toByteArray());
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
        StringBuilder result = new StringBuilder(2 * hash.length);
        for (byte value : hash) {
            result.append(HEX.charAt((value & 0xF0) >>> 4)).append(HEX.charAt(value & 0x0F));
        }
        return result.toString();
    }

    public static void input(Object event) {
        if (event instanceof Auction) {
            Auction auction = (Auction) event;
            if (sampled(auction)) {
                long time = System.currentTimeMillis();
                try {
                    LOG.info("Q8_LATENCY_INPUT auction={} epoch_ms={}",
                            fingerprint(Auction.CODER, auction), time);
                } catch (IOException error) {
                    LOG.warn("Q8_LATENCY_ENCODING_ERROR source", error);
                }
            }
        }
    }

    public static void output(Person person, Auction auction) {
        if (sampled(auction)) {
            long time = System.currentTimeMillis();
            try {
                LOG.info("Q8_LATENCY_OUTPUT auction={} person={} epoch_ms={}",
                        fingerprint(Auction.CODER, auction), fingerprint(Person.CODER, person), time);
            } catch (IOException error) {
                LOG.warn("Q8_LATENCY_ENCODING_ERROR result", error);
            }
        }
    }
}
