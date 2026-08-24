package exchange.core;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

import java.util.concurrent.TimeUnit;

public final class AdaptiveWaitStrategy implements WaitStrategy {
    private static final long BURST_THRESHOLD_EPS = 1000;

    private final WaitStrategy busySpin = new BusySpinWaitStrategy();
    private final WaitStrategy phasedBackoff = PhasedBackoffWaitStrategy.withLiteLock(10L, 20L, TimeUnit.MICROSECONDS);

    // Single volatile write for lock-free switching
    private volatile boolean burstMode = false;
    
    // Background telemetry thread to monitor throughput
    private final Thread monitorThread;

    public AdaptiveWaitStrategy() {
        this.monitorThread = new Thread(() -> {
            long lastTime = System.nanoTime();
            long lastSequence = 0; // We can't access cursor easily here unless we pass it. 
            // Wait, AdaptiveWaitStrategy doesn't have the cursor!
        });
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException {
        
        if (burstMode) {
            return busySpin.waitFor(sequence, cursor, dependentSequence, barrier);
        } else {
            return phasedBackoff.waitFor(sequence, cursor, dependentSequence, barrier);
        }
    }

    @Override
    public void signalAllWhenBlocking() {
        phasedBackoff.signalAllWhenBlocking();
        busySpin.signalAllWhenBlocking();
    }
}
