package exchange.core;

import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Selects a ring wait policy appropriate for the runtime's CPU budget. */
public final class DisruptorWaitStrategies {
    private DisruptorWaitStrategies() {
    }

    public static WaitStrategy latencySensitive() {
        String configured = setting("DISRUPTOR_WAIT_MODE", "disruptor.waitMode", "auto")
                .toLowerCase(Locale.ROOT);
        return switch (configured) {
            case "yielding" -> new YieldingWaitStrategy();
            case "blocking", "lite-blocking" -> new LiteBlockingWaitStrategy();
            case "phased" -> localPhasedWait();
            case "auto" -> productionLinux()
                    ? new YieldingWaitStrategy()
                    : localPhasedWait();
            default -> throw new IllegalArgumentException("Unsupported DISRUPTOR_WAIT_MODE: " + configured);
        };
    }

    private static WaitStrategy localPhasedWait() {
        return PhasedBackoffWaitStrategy.withLiteLock(10L, 20L, TimeUnit.MICROSECONDS);
    }

    private static boolean productionLinux() {
        String profile = setting("APP_PROFILE", "app.profile", "local").toLowerCase(Locale.ROOT);
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return profile.equals("production") && osName.contains("linux");
    }

    private static String setting(String envName, String propertyName, String defaultValue) {
        return System.getProperty(propertyName, System.getenv().getOrDefault(envName, defaultValue));
    }
}
