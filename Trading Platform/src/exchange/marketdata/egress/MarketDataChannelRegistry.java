package exchange.marketdata.egress;

import io.netty.channel.Channel;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.Objects;

/**
 * Copy-on-write subscriber registry for market-data egress.
 *
 * <p>Hot path: the Disruptor egress thread calls {@link #channelsFor(long)} and
 * receives a flat immutable array snapshot. No boxed Long keys, no iterators,
 * and no locks are used while broadcasting.</p>
 *
 * <p>Cold path: Netty event loops call add/remove during subscription changes.
 * These updates copy the primitive map and the affected channel array, then
 * publish the new map through one volatile store.</p>
 */
public final class MarketDataChannelRegistry {
    private static final Channel[] EMPTY_CHANNELS = new Channel[0];

    private final Object mutationLock = new Object();
    private volatile Long2ObjectHashMap<Channel[]> snapshots = new Long2ObjectHashMap<>();

    /**
     * Hot-path primitive lookup. The volatile read gives the egress thread a
     * stable immutable map snapshot, and Agrona's get(long) avoids Long boxing.
     */
    public Channel[] channelsFor(long symbolId) {
        Channel[] channels = snapshots.get(symbolId);
        return channels == null || channels.length == 0 ? EMPTY_CHANNELS : channels;
    }

    public int subscriberCount(long symbolId) {
        return channelsFor(symbolId).length;
    }

    public boolean isEmpty(long symbolId) {
        return channelsFor(symbolId).length == 0;
    }

    public void add(long symbolId, Channel channel) {
        Objects.requireNonNull(channel, "channel");
        synchronized (mutationLock) {
            Channel[] current = channelsForFromCurrentSnapshot(symbolId);
            if (containsIdentity(current, channel)) {
                return;
            }

            Channel[] updated = Arrays.copyOf(current, current.length + 1);
            updated[current.length] = channel;

            Long2ObjectHashMap<Channel[]> next = new Long2ObjectHashMap<>(snapshots);
            next.put(symbolId, updated);
            snapshots = next;
        }
    }

    public void remove(long symbolId, Channel channel) {
        if (channel == null) {
            return;
        }
        synchronized (mutationLock) {
            Channel[] current = channelsForFromCurrentSnapshot(symbolId);
            int index = indexOfIdentity(current, channel);
            if (index < 0) {
                return;
            }

            Long2ObjectHashMap<Channel[]> next = new Long2ObjectHashMap<>(snapshots);
            if (current.length == 1) {
                next.remove(symbolId);
            } else {
                Channel[] updated = new Channel[current.length - 1];
                System.arraycopy(current, 0, updated, 0, index);
                System.arraycopy(current, index + 1, updated, index, current.length - index - 1);
                next.put(symbolId, updated);
            }
            snapshots = next;
        }
    }

    public void remove(Channel channel) {
        if (channel == null) {
            return;
        }
        synchronized (mutationLock) {
            Long2ObjectHashMap<Channel[]> currentMap = snapshots;
            MapRef nextRef = new MapRef();
            currentMap.forEachLong((symbolId, current) -> {
                int index = indexOfIdentity(current, channel);
                if (index < 0) {
                    return;
                }
                Long2ObjectHashMap<Channel[]> writable = nextRef.map;
                if (writable == null) {
                    writable = new Long2ObjectHashMap<>(currentMap);
                    nextRef.map = writable;
                }
                if (current.length == 1) {
                    writable.remove(symbolId);
                } else {
                    Channel[] updated = new Channel[current.length - 1];
                    System.arraycopy(current, 0, updated, 0, index);
                    System.arraycopy(current, index + 1, updated, index, current.length - index - 1);
                    writable.put(symbolId, updated);
                }
            });
            if (nextRef.map != null) {
                snapshots = nextRef.map;
            }
        }
    }

    public void clear() {
        synchronized (mutationLock) {
            if (!snapshots.isEmpty()) {
                snapshots = new Long2ObjectHashMap<>();
            }
        }
    }

    private Channel[] channelsForFromCurrentSnapshot(long symbolId) {
        Channel[] channels = snapshots.get(symbolId);
        return channels == null || channels.length == 0 ? EMPTY_CHANNELS : channels;
    }

    private static boolean containsIdentity(Channel[] channels, Channel channel) {
        return indexOfIdentity(channels, channel) >= 0;
    }

    private static int indexOfIdentity(Channel[] channels, Channel channel) {
        if (channels == null || channels.length == 0) {
            return -1;
        }
        for (int i = 0; i < channels.length; i++) {
            if (channels[i] == channel) {
                return i;
            }
        }
        return -1;
    }

    private static final class MapRef {
        private Long2ObjectHashMap<Channel[]> map;
    }
}
