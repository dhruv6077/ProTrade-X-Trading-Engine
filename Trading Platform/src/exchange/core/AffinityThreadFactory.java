package exchange.core;

import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory that pins production workers to isolated CPU cores using OpenHFT Affinity.
 * Local macOS and test runs deliberately use ordinary JVM threads to avoid draining the
 * small local core pool and spamming "No reservable Core" warnings.
 */
public final class AffinityThreadFactory implements ThreadFactory {
    private static final Logger logger = LoggerFactory.getLogger(AffinityThreadFactory.class);

    private final String prefix;
    private final boolean daemon;
    private final boolean affinityEnabled;
    private final AtomicInteger nextId = new AtomicInteger(1);

    public AffinityThreadFactory(String prefix) {
        this(prefix, true);
    }

    public AffinityThreadFactory(String prefix, boolean daemon) {
        this(prefix, daemon, shouldUseAffinity());
    }

    public AffinityThreadFactory(String prefix, boolean daemon, boolean affinityEnabled) {
        this.prefix = prefix;
        this.daemon = daemon;
        this.affinityEnabled = affinityEnabled;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = prefix + "-" + nextId.getAndIncrement();
        Thread thread = new Thread(() -> run(runnable, affinityEnabled), threadName);
        thread.setDaemon(daemon);
        return thread;
    }

    private static void run(Runnable runnable, boolean affinityEnabled) {
        if (!affinityEnabled) {
            runnable.run();
            return;
        }

        try (AffinityLock affinityLock = AffinityLock.acquireCore()) {
            if (affinityLock.cpuId() >= 0) {
                logger.debug("Pinned {} to CPU {}", Thread.currentThread().getName(), affinityLock.cpuId());
            }
            runnable.run();
        } catch (RuntimeException e) {
            logger.warn("CPU affinity pinning failed for {}; continuing unpinned: {}",
                    Thread.currentThread().getName(), e.getMessage());
            runnable.run();
        }
    }

    private static boolean shouldUseAffinity() {
        if (booleanSetting("DISABLE_THREAD_AFFINITY", "disableThreadAffinity", false)) {
            return false;
        }
        if (booleanSetting("FORCE_THREAD_AFFINITY", "forceThreadAffinity", false)) {
            return true;
        }
        String profile = stringSetting("APP_PROFILE", "app.profile", "local").toLowerCase(Locale.ROOT);
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return profile.equals("production") && osName.contains("linux");
    }

    private static boolean booleanSetting(String envName, String propertyName, boolean defaultValue) {
        String value = stringSetting(envName, propertyName, Boolean.toString(defaultValue));
        return Boolean.parseBoolean(value);
    }

    private static String stringSetting(String envName, String propertyName, String defaultValue) {
        return System.getProperty(propertyName, System.getenv().getOrDefault(envName, defaultValue));
    }
}
