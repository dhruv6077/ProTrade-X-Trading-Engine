package exchange.core;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;

import java.util.Objects;

/**
 * Copy-on-write symbol master that assigns stable primitive IDs to symbols.
 *
 * Mutations are cold-path operations performed during bootstrap or subscription
 * changes. Hot readers take a volatile snapshot and avoid boxed Long/Integer keys.
 */
public final class SymbolDictionary {
    private static final int UNKNOWN_SYMBOL_ID = 0;

    private volatile Object2IntHashMap<String> symbolToId = new Object2IntHashMap<>(64, 0.6f, UNKNOWN_SYMBOL_ID);
    private volatile Int2ObjectHashMap<String> idToSymbol = new Int2ObjectHashMap<>(64, 0.6f);
    private int nextSymbolId = 1;

    public int register(String symbol) {
        Objects.requireNonNull(symbol, "symbol");
        synchronized (this) {
            int existing = symbolToId.getValue(symbol);
            if (existing != UNKNOWN_SYMBOL_ID) {
                return existing;
            }

            int assigned = nextSymbolId++;
            Object2IntHashMap<String> nextSymbolToId = new Object2IntHashMap<>(symbolToId);
            Int2ObjectHashMap<String> nextIdToSymbol = new Int2ObjectHashMap<>(idToSymbol);
            nextSymbolToId.put(symbol, assigned);
            nextIdToSymbol.put(assigned, symbol);
            symbolToId = nextSymbolToId;
            idToSymbol = nextIdToSymbol;
            return assigned;
        }
    }

    public int idFor(String symbol) {
        if (symbol == null) {
            return UNKNOWN_SYMBOL_ID;
        }
        return symbolToId.getValue(symbol);
    }

    public String symbolFor(int symbolId) {
        return idToSymbol.get(symbolId);
    }

    public boolean contains(String symbol) {
        return idFor(symbol) != UNKNOWN_SYMBOL_ID;
    }

    public boolean contains(int symbolId) {
        return symbolId != UNKNOWN_SYMBOL_ID && idToSymbol.get(symbolId) != null;
    }
}
