package software.amazon.ionhash;

import software.amazon.ion.IonType;
import software.amazon.ion.SymbolToken;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * Simple cache for digests of symbols and select primitives (e.g., nulls, booleans).
 */
class DigestCache {
    private final Map<String,byte[]> cache = new WeakHashMap<>();

    final byte[] get(String key) {
        if (key == null) {
            return null;
        }
        return cache.get(key);
    }

    void put(String key, byte[] digest) {
        if (key == null) {
            return;
        }
        cache.put(key, digest);
    }

    final String getKey(IonType ionType, SymbolToken symbol) {
        if (symbol == null || symbol.getText() == null) {
            return null;
        }
        return getKey(ionType, symbol.getText());
    }

    final String getKey(IonType ionType, String value) {
        return value == null
                ? new StringBuilder()
                        .append(IonType.NULL.ordinal())
                        .append(".")
                        .append(ionType.toString()).toString()
                : new StringBuilder()
                        .append(ionType.ordinal())
                        .append(".")
                        .append(value).toString();
    }


    /**
     * A no-op extension of DigestCache.
     */
    final static class NoOpDigestCache extends DigestCache {
        @Override
        final void put(String key, byte[] digest) {
        }
    }
}
