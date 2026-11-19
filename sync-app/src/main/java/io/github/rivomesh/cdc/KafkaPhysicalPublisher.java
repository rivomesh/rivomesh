package io.github.rivomesh.cdc;

import java.util.Set;

/** Optional access to real native COMMIT observations under the business transport. */
interface KafkaPhysicalPublisher extends ConfirmedPublisher {
    void beginPhysicalObservation(Set<String> namespaces)throws InterruptedException;
    KafkaPhysicalReceipt.Observation finishPhysicalObservation()throws InterruptedException;
}
