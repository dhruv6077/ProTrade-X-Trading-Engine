package exchange.dispatch;

public interface RingBufferEventListener {
    void onRingBufferEvent(RingBufferEvent event, long sequence, boolean endOfBatch);
}
