package io.github.rivomesh.transport.artemis;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static io.github.rivomesh.transport.artemis.ArtemisTransportAcceptance.publication;
import static io.github.rivomesh.transport.artemis.ArtemisTransportAcceptance.read;
import static io.github.rivomesh.transport.artemis.ArtemisTransportAcceptance.require;

/** Verifies a retained receipt and its original bytes through the same address after broker promotion. */
public final class ArtemisSharedStoreAcceptance {
    private static void waypoint(Path markers, String received, String expected) throws Exception {
        Files.createFile(markers.resolve(received));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (!Files.exists(markers.resolve(expected)) && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        require(Files.isRegularFile(markers.resolve(expected)), "Fixture did not complete " + expected);
    }

    public static void main(String[] arguments) throws Exception {
        ArtemisTransport.Settings settings = new ArtemisTransport.Settings();
        settings.url = arguments[0];
        settings.username = "rivomesh";
        settings.password = System.getenv("RIVOMESH_ARTEMIS_TEST_PASSWORD");
        settings.queuePrefix = "rivomesh";
        settings.brokerName = "rivomesh-artemis";
        settings.requestTimeoutMillis = 1800;

        Path markers = Paths.get(arguments[1]);
        String binding = "rivomesh:" + String.join("", Collections.nCopies(64, "a"));
        String queue = ArtemisTransport.physicalQueue(settings.queuePrefix, binding);
        String identity;
        String receipt;
        try (ArtemisTransport primary = new ArtemisTransport(settings, binding)) {
            primary.prepare(true);
            identity = primary.identity();
            primary.publish(Collections.singletonList(publication("before-promotion")));
            receipt = primary.confirmedProgress();
            require(receipt != null, "Primary did not confirm the original publication");
            require(read(settings, queue, true).equals(Collections.singletonList("before-promotion")),
                    "The consumer did not read the first event with the original bytes");
            System.out.println("PRIMARY_RETAINS_EXACT_CONFIRMED_EVENT_AFTER_ACK");
            waypoint(markers, "primary-checked", "gateway-isolated");

            boolean failed = false;
            try {
                primary.publish(Collections.singletonList(publication("retry-after-promotion")));
            } catch (ArtemisTransport.Failure blocked) {
                failed = true;
            }
            require(failed, "An inaccessible endpoint reported an acknowledged publication");
            require(receipt.equals(primary.confirmedProgress()), "The stable receipt moved without a broker");
        }
        System.out.println("ISOLATED_INGRESS_PRESERVES_THE_LAST_CONFIRMATION");
        waypoint(markers, "outage-checked", "backup-routed");

        try (ArtemisTransport backup = new ArtemisTransport(settings, binding)) {
            backup.resumeProgress(receipt);
            backup.prepare(false);
            require(identity.equals(backup.identity()), "Backup changed the persistent node or physical queue identity");
            require(read(settings, queue, false).equals(Collections.singletonList("before-promotion")),
                    "Original retained history was absent or changed after promotion");
            backup.publish(Collections.singletonList(publication("retry-after-promotion")));
            require(!receipt.equals(backup.confirmedProgress()), "Retry did not advance the confirmed receipt");
            require(read(settings, queue, true).equals(Arrays.asList("before-promotion", "retry-after-promotion")),
                    "Backup did not contain exactly the original and retry events");
        }
        System.out.println("PROMOTED_BACKUP_REUSES_IDENTITY_AND_CONFIRMS_THE_SAME_RETRY");
    }
}
