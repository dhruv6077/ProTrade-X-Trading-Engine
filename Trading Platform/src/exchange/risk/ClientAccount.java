package exchange.risk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ClientAccount {
    private final String clientId;
    private final AtomicLong availableCashCents;
    private final AtomicLong reservedCashCents = new AtomicLong();
    private final AtomicLong cashVersion = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> positions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> reservedPositions = new ConcurrentHashMap<>();
    private long[] positionsBySymbolId = new long[16];
    private long[] reservedPositionsBySymbolId = new long[16];
    private String[] symbolsById = new String[16];

    public ClientAccount(String clientId, long availableCashCents) {
        this.clientId = clientId;
        this.availableCashCents = new AtomicLong(availableCashCents);
    }

    public String clientId() {
        return clientId;
    }

    public long availableCashCents() {
        return cashSnapshot().availableCashCents();
    }

    public long reservedCashCents() {
        return cashSnapshot().reservedCashCents();
    }

    public CashSnapshot cashSnapshot() {
        while (true) {
            long before = cashVersion.get();
            if ((before & 1L) != 0L) {
                Thread.onSpinWait();
                continue;
            }
            long available = availableCashCents.get();
            long reserved = reservedCashCents.get();
            long after = cashVersion.get();
            if (before == after && (after & 1L) == 0L) {
                return new CashSnapshot(available, reserved);
            }
            Thread.onSpinWait();
        }
    }

    public Map<String, Long> positions() {
        ConcurrentHashMap<String, Long> snapshot = new ConcurrentHashMap<>();
        positions.forEach((symbol, quantity) -> snapshot.put(symbol, quantity.get()));
        for (int symbolId = 1; symbolId < positionsBySymbolId.length; symbolId++) {
            String symbol = symbolsById[symbolId];
            if (symbol != null) {
                snapshot.put(symbol, positionsBySymbolId[symbolId]);
            }
        }
        return Map.copyOf(snapshot);
    }

    public long position(String symbol) {
        int symbolId = symbolIdFor(symbol);
        if (symbolId > 0) {
            return positionsBySymbolId[symbolId];
        }
        AtomicLong position = positions.get(symbol);
        return position == null ? 0L : position.get();
    }

    long position(int symbolId) {
        return symbolId > 0 && symbolId < positionsBySymbolId.length ? positionsBySymbolId[symbolId] : 0L;
    }

    public long reservedPosition(String symbol) {
        int symbolId = symbolIdFor(symbol);
        if (symbolId > 0) {
            return reservedPositionsBySymbolId[symbolId];
        }
        AtomicLong reserved = reservedPositions.get(symbol);
        return reserved == null ? 0L : reserved.get();
    }

    long reservedPosition(int symbolId) {
        return symbolId > 0 && symbolId < reservedPositionsBySymbolId.length ? reservedPositionsBySymbolId[symbolId] : 0L;
    }

    public Map<String, Long> reservedPositions() {
        ConcurrentHashMap<String, Long> snapshot = new ConcurrentHashMap<>();
        reservedPositions.forEach((symbol, quantity) -> snapshot.put(symbol, quantity.get()));
        for (int symbolId = 1; symbolId < reservedPositionsBySymbolId.length; symbolId++) {
            String symbol = symbolsById[symbolId];
            if (symbol != null) {
                snapshot.put(symbol, reservedPositionsBySymbolId[symbolId]);
            }
        }
        return Map.copyOf(snapshot);
    }

    boolean reserveCash(long amountCents) {
        if (amountCents < 0) {
            throw new IllegalArgumentException("amountCents must be non-negative");
        }
        long version = beginCashWrite();
        try {
            long available = availableCashCents.get();
            if (available < amountCents) {
                return false;
            }
            availableCashCents.set(available - amountCents);
            reservedCashCents.set(reservedCashCents.get() + amountCents);
            return true;
        } finally {
            endCashWrite(version);
        }
    }

    void releaseReservedCash(long amountCents) {
        if (amountCents <= 0) {
            return;
        }
        long version = beginCashWrite();
        try {
            reservedCashCents.set(reservedCashCents.get() - amountCents);
            availableCashCents.set(availableCashCents.get() + amountCents);
        } finally {
            endCashWrite(version);
        }
    }

    void settleReservedCash(long reservedDebitCents, long availableCreditCents) {
        if (reservedDebitCents <= 0 && availableCreditCents <= 0) {
            return;
        }
        long version = beginCashWrite();
        try {
            if (reservedDebitCents > 0) {
                reservedCashCents.set(reservedCashCents.get() - reservedDebitCents);
            }
            if (availableCreditCents > 0) {
                availableCashCents.set(availableCashCents.get() + availableCreditCents);
            }
        } finally {
            endCashWrite(version);
        }
    }

    void creditCash(long amountCents) {
        if (amountCents > 0) {
            long version = beginCashWrite();
            try {
                availableCashCents.set(availableCashCents.get() + amountCents);
            } finally {
                endCashWrite(version);
            }
        }
    }

    void setAvailableCash(long amountCents) {
        long version = beginCashWrite();
        try {
            availableCashCents.set(amountCents);
        } finally {
            endCashWrite(version);
        }
    }

    void setPosition(String symbol, long quantity) {
        atomicCell(positions, symbol).set(quantity);
    }

    void setPosition(String symbol, int symbolId, long quantity) {
        if (symbolId > 0) {
            ensureSymbolCapacity(symbolId);
            rememberSymbol(symbol, symbolId);
            positionsBySymbolId[symbolId] = quantity;
            return;
        }
        setPosition(symbol, quantity);
    }

    void addPosition(String symbol, long quantityDelta) {
        if (quantityDelta != 0) {
            atomicCell(positions, symbol).addAndGet(quantityDelta);
        }
    }

    void addPosition(String symbol, int symbolId, long quantityDelta) {
        if (quantityDelta == 0) {
            return;
        }
        if (symbolId > 0) {
            ensureSymbolCapacity(symbolId);
            rememberSymbol(symbol, symbolId);
            positionsBySymbolId[symbolId] += quantityDelta;
            return;
        }
        addPosition(symbol, quantityDelta);
    }

    boolean reservePosition(String symbol, long quantity) {
        if (quantity < 0) {
            throw new IllegalArgumentException("quantity must be non-negative");
        }
        if (quantity == 0) {
            return true;
        }
        AtomicLong reserved = atomicCell(reservedPositions, symbol);
        while (true) {
            long currentReserved = reserved.get();
            long available = position(symbol) - currentReserved;
            if (available < quantity) {
                return false;
            }
            if (reserved.compareAndSet(currentReserved, currentReserved + quantity)) {
                return true;
            }
        }
    }

    boolean reservePosition(String symbol, int symbolId, long quantity) {
        if (symbolId <= 0) {
            return reservePosition(symbol, quantity);
        }
        if (quantity < 0) {
            throw new IllegalArgumentException("quantity must be non-negative");
        }
        if (quantity == 0) {
            return true;
        }
        ensureSymbolCapacity(symbolId);
        rememberSymbol(symbol, symbolId);
        if (positionsBySymbolId[symbolId] == 0L) {
            AtomicLong fallbackPosition = positions.get(symbol);
            if (fallbackPosition != null) {
                positionsBySymbolId[symbolId] = fallbackPosition.get();
            }
        }
        long available = positionsBySymbolId[symbolId] - reservedPositionsBySymbolId[symbolId];
        if (available < quantity) {
            return false;
        }
        reservedPositionsBySymbolId[symbolId] += quantity;
        return true;
    }

    void releaseReservedPosition(String symbol, long quantity) {
        if (quantity <= 0) {
            return;
        }
        AtomicLong reserved = reservedPositions.get(symbol);
        if (reserved == null) {
            return;
        }
        reserved.updateAndGet(current -> Math.max(0, current - quantity));
    }

    void releaseReservedPosition(String symbol, int symbolId, long quantity) {
        if (symbolId <= 0) {
            releaseReservedPosition(symbol, quantity);
            return;
        }
        if (quantity <= 0) {
            return;
        }
        ensureSymbolCapacity(symbolId);
        rememberSymbol(symbol, symbolId);
        reservedPositionsBySymbolId[symbolId] = Math.max(0, reservedPositionsBySymbolId[symbolId] - quantity);
    }

    private long beginCashWrite() {
        while (true) {
            long current = cashVersion.get();
            if ((current & 1L) != 0L) {
                Thread.onSpinWait();
                continue;
            }
            if (cashVersion.compareAndSet(current, current + 1L)) {
                return current + 1L;
            }
            Thread.onSpinWait();
        }
    }

    private void endCashWrite(long oddVersion) {
        cashVersion.set(oddVersion + 1L);
    }

    public record CashSnapshot(long availableCashCents, long reservedCashCents) {
    }

    private void ensureSymbolCapacity(int symbolId) {
        if (symbolId < positionsBySymbolId.length) {
            return;
        }
        int nextLength = positionsBySymbolId.length;
        while (symbolId >= nextLength) {
            nextLength <<= 1;
        }
        long[] nextPositions = new long[nextLength];
        long[] nextReserved = new long[nextLength];
        System.arraycopy(positionsBySymbolId, 0, nextPositions, 0, positionsBySymbolId.length);
        System.arraycopy(reservedPositionsBySymbolId, 0, nextReserved, 0, reservedPositionsBySymbolId.length);
        String[] nextSymbols = new String[nextLength];
        System.arraycopy(symbolsById, 0, nextSymbols, 0, symbolsById.length);
        positionsBySymbolId = nextPositions;
        reservedPositionsBySymbolId = nextReserved;
        symbolsById = nextSymbols;
    }

    private void rememberSymbol(String symbol, int symbolId) {
        if (symbolId > 0 && symbolsById[symbolId] == null) {
            symbolsById[symbolId] = symbol;
        }
    }

    private int symbolIdFor(String symbol) {
        for (int symbolId = 1; symbolId < symbolsById.length; symbolId++) {
            if (symbol.equals(symbolsById[symbolId])) {
                return symbolId;
            }
        }
        return 0;
    }

    private static AtomicLong atomicCell(ConcurrentHashMap<String, AtomicLong> map, String key) {
        AtomicLong value = map.get(key);
        if (value != null) {
            return value;
        }
        AtomicLong created = new AtomicLong();
        AtomicLong existing = map.putIfAbsent(key, created);
        return existing == null ? created : existing;
    }
}
