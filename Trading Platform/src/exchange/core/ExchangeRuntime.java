package exchange.core;

import exchange.clearing.ClearingService;
import exchange.dispatch.InMemoryEventDispatcher;
import exchange.gateway.GatewayValidator;
import exchange.gateway.OrderGateway;
import exchange.journal.CommandJournal;
import exchange.journal.AsyncDiskCommandJournal;
import exchange.journal.ChronicleCommandJournal;
import exchange.journal.InMemoryCommandJournal;
import exchange.journal.MappedCommandJournal;
import exchange.marketdata.MarketDataEngine;
import exchange.persistence.PostgresAuditService;
import exchange.replication.CommandReplicator;
import exchange.replication.InMemoryReplicationChannel;
import exchange.replication.NoOpCommandReplicator;
import exchange.risk.InMemoryRiskEngine;
import exchange.risk.RiskProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import persistence.DatabaseManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExchangeRuntime {
    private static final Logger logger = LoggerFactory.getLogger(ExchangeRuntime.class);
    private static final ExchangeRuntime INSTANCE = new ExchangeRuntime();

    private final Set<String> symbols = ConcurrentHashMap.newKeySet();
    private final GatewayValidator validator;
    private final DeterministicMatchingEngine matchingEngine;
    private final InMemoryRiskEngine riskEngine;
    private final Sequencer sequencer;
    private final CommandJournal journal;
    private final CommandReplicator replicator;
    private final InMemoryEventDispatcher dispatcher;
    private final ClearingService clearingService;
    private final MarketDataEngine marketDataEngine;
    private final OrderGateway gateway;
    private final AtomicBoolean hydrated = new AtomicBoolean(false);
    private volatile PostgresAuditService postgresAuditService;

    private ExchangeRuntime() {
        this.validator = new GatewayValidator(symbols, 1, 1, 1_000_000);
        this.matchingEngine = new DeterministicMatchingEngine(symbols);
        this.riskEngine = new InMemoryRiskEngine(new RiskProfile(10_000_000_000L, 10_000_000,
                10_000_000_000L, false));
        this.sequencer = new Sequencer();
        CommandJournal jnl = null;
        try {
            boolean useChronicle = "true".equalsIgnoreCase(System.getProperty("useChronicleJournal",
                    System.getenv("USE_CHRONICLE_JOURNAL")));
            boolean useMapped = "true".equalsIgnoreCase(System.getProperty("useMappedJournal",
                    System.getenv("USE_MAPPED_JOURNAL")));
            boolean useDisk = "true".equalsIgnoreCase(System.getProperty("useDiskJournal", System.getenv("USE_DISK_JOURNAL")));
            if (useMapped) {
                Path journalPath = Path.of(System.getProperty("mappedJournalPath",
                        System.getenv().getOrDefault("MAPPED_JOURNAL_PATH", "mapped-command-journal.dat")));
                int maxRecords = Integer.parseInt(System.getProperty("mappedJournalMaxRecords",
                        System.getenv().getOrDefault("MAPPED_JOURNAL_MAX_RECORDS", "1000000")));
                jnl = new MappedCommandJournal(journalPath, maxRecords);
            } else if (useChronicle) {
                Path journalPath = Path.of(System.getProperty("chronicleJournalPath",
                        System.getenv().getOrDefault("CHRONICLE_JOURNAL_PATH", "chronicle-command-journal")));
                int ringSize = Integer.parseInt(System.getProperty("chronicleJournalRingSize",
                        System.getenv().getOrDefault("CHRONICLE_JOURNAL_RING_SIZE", "65536")));
                jnl = new ChronicleCommandJournal(journalPath, ringSize);
            } else if (useDisk) {
                jnl = new AsyncDiskCommandJournal(java.nio.file.Path.of("journal.log"));
            } else {
                jnl = new InMemoryCommandJournal();
            }
        } catch (Exception e) {
            logger.error("Failed to initialize configured command journal, falling back to in-memory", e);
            jnl = new InMemoryCommandJournal();
        }
        this.journal = jnl;
        this.replicator = replicationEnabled()
                ? new InMemoryReplicationChannel(replicationCapacity())
                : NoOpCommandReplicator.INSTANCE;
        this.clearingService = new ClearingService(riskEngine);
        this.marketDataEngine = new MarketDataEngine();
        this.dispatcher = new InMemoryEventDispatcher();
        this.dispatcher.attachTrailingHandler(marketDataEngine);
        this.gateway = new OrderGateway(validator, riskEngine, sequencer, journal, replicator, matchingEngine,
                dispatcher, false);
        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "exchange-runtime-shutdown"));
    }

    public static ExchangeRuntime getInstance() {
        return INSTANCE;
    }

    public void addSymbol(String symbol) {
        symbols.add(symbol);
        int symbolId = validator.addSymbol(symbol);
        matchingEngine.addSymbol(symbol, symbolId);
    }

    public void addSymbols(Collection<String> symbols) {
        for (String symbol : symbols) {
            addSymbol(symbol);
        }
    }

    public OrderGateway gateway() {
        return gateway;
    }

    public CommandJournal journal() {
        return journal;
    }

    public CommandReplicator replicator() {
        return replicator;
    }

    public InMemoryEventDispatcher dispatcher() {
        return dispatcher;
    }

    public InMemoryRiskEngine riskEngine() {
        return riskEngine;
    }

    public MarketDataEngine marketDataEngine() {
        return marketDataEngine;
    }

    public boolean isHydrated() {
        return hydrated.get();
    }

    public synchronized void bootstrapFromPostgresIfAvailable() {
        if (hydrated.get()) {
            return;
        }
        if (skipStateHydration()) {
            logger.info("State hydration skipped by SKIP_STATE_HYDRATION; opening gateway with empty runtime state");
            hydrated.set(true);
            gateway.openIngress();
            return;
        }
        DatabaseManager databaseManager = DatabaseManager.getInstance();
        if (!databaseManager.isInitialized()) {
            logger.info("PostgreSQL audit store is not initialized; attempting command-journal replay");
            bootstrapFromCommandJournal();
            return;
        }

        try {
            bootstrapFromDataSource(databaseManager.getDataSource());
        } catch (SQLException e) {
            // Fail-open: if replay fails, start with empty state rather than refusing to boot.
            // This is a deliberate trade-off for development/staging environments that may not
            // have a working PostgreSQL instance. In production, this should be monitored via
            // the WARN log and treated as a critical incident requiring manual state reconciliation.
            logger.warn("PostgreSQL replay failed; opening gateway without database replay: {}", e.getMessage());
            hydrated.set(true);
            gateway.openIngress();
        }
    }

    public synchronized void bootstrapFromDataSource(DataSource dataSource) throws SQLException {
        if (hydrated.get()) {
            return;
        }
        gateway.pauseIngress();

        PostgresAuditService auditService = new PostgresAuditService(dataSource);
        PostgresAuditService.ReplayResult replay = auditService.replayEvents();
        ClearingService replayClearing = new ClearingService(riskEngine, true);

        for (List<exchange.model.ExchangeEvent> batch : replay.batches()) {
            for (exchange.model.ExchangeEvent event : batch) {
                addSymbol(event.symbol());
            }
            matchingEngine.hydrate(batch);
            replayClearing.onEvents(batch);
            marketDataEngine.onEvents(batch);
        }

        sequencer.advanceToAtLeast(replay.maxSequenceId() + 1);
        postgresAuditService = auditService;
        dispatcher.attachTrailingHandler(auditService);
        hydrated.set(true);
        gateway.openIngress();
        logger.info("Exchange runtime hydrated from {} persisted event batch(es)", replay.batches().size());
    }

    public synchronized void bootstrapFromCommandJournal() {
        if (hydrated.get()) {
            return;
        }
        gateway.pauseIngress();

        List<exchange.model.OrderCommand> commands = journal.replay();
        ClearingService replayClearing = new ClearingService(riskEngine, true);
        long maxSequenceId = 0L;
        int eventBatches = 0;

        for (exchange.model.OrderCommand command : commands) {
            addSymbol(command.symbol());
            List<exchange.model.ExchangeEvent> events = matchingEngine.process(command);
            replayClearing.onEvents(events);
            marketDataEngine.onEvents(events);
            maxSequenceId = Math.max(maxSequenceId, command.sequenceNumber());
            eventBatches++;
        }

        sequencer.advanceToAtLeast(maxSequenceId + 1);
        hydrated.set(true);
        gateway.openIngress();
        logger.info("Exchange runtime hydrated from {} command-journal command(s)", eventBatches);
    }

    public void close() {
        gateway.close();
        dispatcher.close();
        PostgresAuditService auditService = postgresAuditService;
        if (auditService != null) {
            auditService.close();
        }
        marketDataEngine.close();
        replicator.close();
        journal.close();
    }

    private static boolean replicationEnabled() {
        return "true".equalsIgnoreCase(System.getProperty("useInMemoryReplication",
                System.getenv("USE_IN_MEMORY_REPLICATION")));
    }

    private static boolean skipStateHydration() {
        return "true".equalsIgnoreCase(System.getProperty("skipStateHydration",
                System.getenv("SKIP_STATE_HYDRATION")));
    }

    private static int replicationCapacity() {
        return Integer.parseInt(System.getProperty("replicationCapacity",
                System.getenv().getOrDefault("REPLICATION_CAPACITY", "65536")));
    }
}
