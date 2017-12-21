package software.amazon.ionhash;

import software.amazon.ion.IonType;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static software.amazon.ionhash.HasherImpl.EMPTY_BYTE_ARRAY;

/**
 * Hasher decorator that allows the caller to disable/enable
 * calls to the underlying hasher.
 */
public final class HasherEngagerImpl implements Hasher {
    private final Hasher delegate;
    private boolean enabled = true;

    HasherEngagerImpl(Hasher delegate) {
        if (delegate == null) {
            throw new NullPointerException("Delegate hasher must not be null");
        }

        this.delegate = delegate;
    }

    @Override
    public void enable() {
        if (!enabled) {
            delegate.enable();
            enabled = true;
        }
    }

    @Override
    public void disable() {
        if (enabled) {
            delegate.disable();
            enabled = false;
        }
    }

    @Override
    public void stepIn(IonType containerType, SymbolToken fieldName, SymbolToken[] annotations) {
        if (enabled) {
            delegate.stepIn(containerType, fieldName, annotations);
        }
    }

    @Override
    public void stepOut() {
        if (enabled) {
            delegate.stepOut();
        }
    }

    @Override
    public byte[] digest() {
        if (enabled) {
            return delegate.digest();
        }
        return EMPTY_BYTE_ARRAY;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public ScalarHasher scalar() {
        return enabled ? delegate.scalar() : NOOP_SCALAR_HASHER;
    }

    private static final ScalarHasher NOOP_SCALAR_HASHER = new ScalarHasher() {
        @Override public ScalarHasher withFieldName(SymbolToken fieldName) { return this; }
        @Override public ScalarHasher withAnnotations(SymbolToken[] annotations) { return this; }
        @Override public ScalarHasher withHasher(IonHasher hasher) { return this; }
        @Override public void prepare() { }
        @Override public void updateBlob(byte[] value) throws IOException { }
        @Override public void updateBlob(byte[] value, int start, int len) throws IOException { }
        @Override public void updateBool(boolean value) throws IOException { }
        @Override public void updateClob(byte[] value) throws IOException { }
        @Override public void updateClob(byte[] value, int start, int len) throws IOException { }
        @Override public void updateDecimal(BigDecimal value) throws IOException { }
        @Override public void updateFloat(double value) throws IOException { }
        @Override public void updateInt(BigInteger value) throws IOException { }
        @Override public void updateNull() throws IOException { }
        @Override public void updateNull(IonType type) throws IOException { }
        @Override public void updateString(String value) throws IOException { }
        @Override public void updateSymbol(String value) throws IOException { }
        @Override public void updateSymbolToken(SymbolToken value) throws IOException { }
        @Override public void updateTimestamp(Timestamp value) throws IOException { }
        @Override public void close() throws IOException { }
    };
}
