package software.amazon.ionhash;

import software.amazon.ion.Decimal;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

import static software.amazon.ionhash.Hasher.EMPTY_BYTE_ARRAY;

/**
 * This IonReader decorator calculates a currentHash of the Ion data model.
 * The currentHash of the IonValue just nexted past or stepped out of is available via currentHash().
 * <p/>
 * This class is not thread-safe.
 */
class IonHashReaderImpl implements IonHashReader {
    private final IonReader delegate;
    private final Hasher hasher;

    private IonType ionType;
    private byte[] currentHash = EMPTY_BYTE_ARRAY;

    IonHashReaderImpl(IonReader delegate, IonHasherProvider hasherProvider) {
        if (delegate == null) {
            throw new NullPointerException("IonReader must not be null");
        }
        if (hasherProvider == null) {
            throw new NullPointerException("IonHasherProvider must not be null");
        }

        this.delegate = delegate;
        this.hasher = new Hasher(hasherProvider);
    }

    @Override
    public byte[] currentHash() {
        return currentHash;
    }

    @Override
    public IonType next() {
        if (ionType != null) {
            if (!isNullValue() && IonType.isContainer(ionType)) {
                // caller is skipping over a container;  step in and consume it
                // in order to compute the currentHash correctly
                stepIn();
                consumeRemainder();
                stepOut();
            } else {
                hasher.scalar().withFieldName(getFieldNameSymbol())
                               .withAnnotations(getTypeAnnotationSymbols());
                hasher.scalar().prepare();

                try {
                    if (isNullValue()) {
                        hasher.scalar().updateNull(ionType);
                    } else {
                        switch (ionType) {
                            case BLOB:
                                hasher.scalar().updateBlob(newBytes());
                                break;
                            case BOOL:
                                hasher.scalar().updateBool(booleanValue());
                                break;
                            case CLOB:
                                hasher.scalar().updateClob(newBytes());
                                break;
                            case DECIMAL:
                                hasher.scalar().updateDecimal(decimalValue());
                                break;
                            case FLOAT:
                                hasher.scalar().updateFloat(doubleValue());
                                break;
                            case INT:
                                hasher.scalar().updateInt(bigIntegerValue());
                                break;
                            case STRING:
                                hasher.scalar().updateString(stringValue());
                                break;
                            case SYMBOL:
                                hasher.scalar().updateSymbolToken(symbolValue());
                                break;
                            case TIMESTAMP:
                                hasher.scalar().updateTimestamp(timestampValue());
                                break;
                            default:
                                throw new IonHashException("Unsupported IonType (" + ionType + ")");
                        }
                    }
                } catch (IOException e) {
                    throw new IonHashException(e);
                }

                // update such that currentHash always represents
                // the hash of the value we just "nexted" past
                currentHash = hasher.scalar().digest();
            }
        }

        ionType = delegate.next();

        return ionType;
    }

    @Override
    public void stepIn() {
        hasher.stepIn(ionType, getFieldNameSymbol(), getTypeAnnotationSymbols());
        delegate.stepIn();

        ionType = null;
        currentHash = EMPTY_BYTE_ARRAY;
    }

    @Override
    public void stepOut() {
        // the caller may be bailing on the current container;
        // ensure we consume the rest of it in order to compute currentHash correctly
        consumeRemainder();

        currentHash = hasher.stepOut();
        delegate.stepOut();
    }

    // the caller may opt to skip over portions of a value;  when a caller decides
    // to skip, invoking this method recursively next()s over everything at the
    // current depth to ensure correct hashing
    private void consumeRemainder() {
        while ((ionType = next()) != null) {
            if (IonType.isContainer(ionType) && !isNullValue()) {
                stepIn();
                consumeRemainder();
                stepOut();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            hasher.close();
        } catch (Exception e) {
        }
        delegate.close();
    }


    ///////// The remaining methods are all handled solely by the delegate ///////////

    @Override
    public int getDepth() {
        return delegate.getDepth();
    }

    @Override
    public SymbolTable getSymbolTable() {
        return delegate.getSymbolTable();
    }

    @Override
    public IonType getType() {
        return delegate.getType();
    }

    @Override
    public IntegerSize getIntegerSize() {
        return delegate.getIntegerSize();
    }

    @Override
    public String[] getTypeAnnotations() {
        return delegate.getTypeAnnotations();
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        return delegate.getTypeAnnotationSymbols();
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        return delegate.iterateTypeAnnotations();
    }

    @Override
    public String getFieldName() {
        return delegate.getFieldName();
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return delegate.getFieldNameSymbol();
    }

    @Override
    public boolean isNullValue() {
        return delegate.isNullValue();
    }

    @Override
    public boolean isInStruct() {
        return delegate.isInStruct();
    }

    @Override
    public boolean booleanValue() {
        return delegate.booleanValue();
    }

    @Override
    public int intValue() {
        return delegate.intValue();
    }

    @Override
    public long longValue() {
        return delegate.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        return delegate.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        return delegate.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        return delegate.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        return delegate.decimalValue();
    }

    @Override
    public Date dateValue() {
        return delegate.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        return delegate.timestampValue();
    }

    @Override
    public String stringValue() {
        return delegate.stringValue();
    }

    @Override
    public SymbolToken symbolValue() {
        return delegate.symbolValue();
    }

    @Override
    public int byteSize() {
        return delegate.byteSize();
    }

    @Override
    public byte[] newBytes() {
        return delegate.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        return delegate.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return delegate.asFacet(facetType);
    }
}
