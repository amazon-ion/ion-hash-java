package software.amazon.ionhash;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Provides core hash functionality for use by streaming hash readers and writers.
 * It relies on the binary encoding implementation in IonRawBinaryWriter when possible.
 * <p/>
 * Callers should assume that Digest objects returned by this API will be reused, and
 * may be changed during the next call to this API.
 * <p/>
 * Implementations of this interface are not expected to be thread-safe.
 */
interface Hasher extends Closeable {
    void enable();

    void disable();

    void stepIn(IonType containerType, SymbolToken fieldName, SymbolToken[] annotations);

    void stepOut();

    byte[] digest();

    ScalarHasher scalar();

    /**
     * API for updating the hash with scalar and null values.  Typical usage is to call
     * withFieldName() and withAnnotations(), then prepare(), followed by the appropriate
     * updateXYZ() method.
     */
    interface ScalarHasher extends Closeable {
        ScalarHasher withFieldName(SymbolToken fieldName);
        ScalarHasher withAnnotations(SymbolToken[] annotations);
        ScalarHasher withHasher(IonHasher hasher);
        void prepare();

        void updateBlob(byte[] value) throws IOException;
        void updateBlob(byte[] value, int start, int len) throws IOException;
        void updateBool(boolean value) throws IOException;
        void updateClob(byte[] value) throws IOException;
        void updateClob(byte[] value, int start, int len) throws IOException;
        void updateDecimal(BigDecimal value) throws IOException;
        void updateFloat(double value) throws IOException;
        void updateInt(BigInteger value) throws IOException;
        void updateNull() throws IOException;
        void updateNull(IonType type) throws IOException;
        void updateString(String value) throws IOException;
        void updateSymbol(String value) throws IOException;
        void updateSymbolToken(SymbolToken value) throws IOException;
        void updateTimestamp(Timestamp value) throws IOException;
    }

    static SymbolToken newSymbolToken(String value) {
        return new SymbolToken() {
            @Override
            public String getText() {
                return value;
            }

            @Override
            public String assumeText() {
                return value;
            }

            @Override
            public int getSid() {
                return -1;
            }
        };
    }
}
