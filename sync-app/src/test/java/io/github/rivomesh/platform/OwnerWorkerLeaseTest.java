package io.github.rivomesh.platform;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OwnerWorkerLeaseTest {
    private static final class Worker implements TaskWorker {
        boolean alive=true;
        int fences;
        public boolean alive(){return alive;}
        public int exitCode(){return 137;}
        public void stop(){alive=false;}
        public void fence(){fences++;}
    }

    @Test void cannotLaunchOrReadmitUntilTheFormerProcessStops()throws Exception{
        String incarnation=UUID.randomUUID().toString();
        ControlStore.Lease initial=new ControlStore.Lease("owner",incarnation,1);
        ControlStore.Lease restored=new ControlStore.Lease("owner",incarnation,2);
        AtomicReference<ControlStore.Lease> current=new AtomicReference<>(initial);
        AtomicInteger starts=new AtomicInteger();
        OwnerWorkerLease guard=new OwnerWorkerLease(()->{
            ControlStore.Lease lease=current.get();
            if(lease==null)throw new ControlStore.Fenced();
            return lease;
        });
        assertNull(guard.launch(()->{starts.incrementAndGet();return new Worker();}));
        assertTrue(guard.admit(initial));
        Worker former=(Worker)guard.launch(()->{starts.incrementAndGet();return new Worker();});
        current.set(null);
        guard.monitor();
        guard.suspend();
        assertEquals(1,former.fences);
        assertFalse(guard.permitsStart());
        assertFalse(guard.quiescent());

        current.set(restored);
        assertNull(guard.launch(()->{starts.incrementAndGet();return new Worker();}));
        assertFalse(guard.admit(restored));
        former.stop();
        assertFalse(guard.admit(initial));
        assertTrue(guard.admit(restored));
        assertTrue(guard.retired(former));
        assertNotNull(guard.launch(()->{starts.incrementAndGet();return new Worker();}));
        assertEquals(2,starts.get());
    }

    @Test void aProcessCreatedDuringGenerationLossIsImmediatelyFenced()throws Exception{
        ControlStore.Lease initial=new ControlStore.Lease("owner",UUID.randomUUID().toString(),1);
        AtomicReference<ControlStore.Lease> current=new AtomicReference<>(initial);
        OwnerWorkerLease guard=new OwnerWorkerLease(current::get);
        assertTrue(guard.admit(initial));

        Worker launched=(Worker)guard.launch(()->{
            current.set(new ControlStore.Lease("owner",UUID.randomUUID().toString(),1));
            return new Worker();
        });
        assertEquals(1,launched.fences);
        assertFalse(guard.permitsStart());
        assertFalse(guard.admit(current.get()));
    }
}
