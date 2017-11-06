package software.amazon.ionhash;

import software.amazon.ion.IonType;
import software.amazon.ion.SymbolToken;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * Simple cache for digests of symbols and select primitives (e.g., nulls, booleans).
 */
class DigestCache {
    private final byte[][] boolCache = new byte[2][];
    private final byte[][] nullCache = new byte[IonType.values().length][];
    private final Map<String,byte[]> symbolCache = new WeakHashMap<>();

    final byte[] getBool(boolean bool) {
        return bool ? boolCache[1] : boolCache[0];
    }

    final byte[] getNull(IonType ionType) {
        return nullCache[ionType.ordinal()];
    }

    final byte[] getSymbol(SymbolToken symbol) {
        if (symbol == null || symbol.getText() == null) {
            return null;
        }
        return symbolCache.get(symbol.getText());
    }

    void putBool(boolean bool, byte[] digest) {
        if (bool) {
            boolCache[1] = digest;
        } else {
            boolCache[0] = digest;
        }
    }

    void putNull(IonType ionType, byte[] digest) {
        nullCache[ionType.ordinal()] = digest;
    }

    void putSymbol(SymbolToken symbol, byte[] digest) {
        if (symbol == null || symbol.getText() == null) {
            return;
        }
        symbolCache.put(symbol.getText(), digest);
    }

    /**
     * A no-op extension of DigestCache.
     */
    final static class NoOpDigestCache extends DigestCache {
        @Override
        final void putBool(boolean bool, byte[] digest) {
        }

        @Override
        final void putNull(IonType ionType, byte[] digest) {
        }

        @Override
        final void putSymbol(SymbolToken symbol, byte[] digest) {
        }
    }
}
