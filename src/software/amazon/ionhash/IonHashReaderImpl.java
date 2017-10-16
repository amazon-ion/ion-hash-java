package software.amazon.ionhash;

import software.amazon.ion.Decimal;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * This IonReader decorator calculates a currentHash of the Ion data model.
 * The currentHash of the IonValue just nexted past or stepped out of is available via currentHash().
 * <p/>
 * This class is not thread-safe.
 */
class IonHashReaderImpl implements IonHashReader {
    private final IonReader delegate;
    private final IonHasherProvider hasherProvider;
    private final ScalarHasher scalarHasher;
    private final IonHasher symbolHasher;
    private final IonHashBytes hashBytes = new IonHashBytes();

    private final Deque<ContainerHasher> containerHasherStack = new ArrayDeque<>();
    private IonType ionType;
    private byte[] currentHash = IonHashBytes.EMPTY_BYTE_ARRAY;
    private byte[][] scalarParts;

    IonHashReaderImpl(IonReader delegate, IonHasherProvider hasherProvider) {
        if (delegate == null) {
            throw new NullPointerException("IonReader must not be null");
        }
        if (hasherProvider == null) {
            throw new NullPointerException("IonHasherProvider must not be null");
        }

        this.delegate = delegate;
        this.hasherProvider = hasherProvider;
        this.scalarHasher = new ScalarHasher();
        this.symbolHasher = hasherProvider.newHasher();
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
                scalarHasher.withFieldName(getFieldNameSymbol())
                            .withAnnotations(getTypeAnnotationSymbols());
                scalarHasher.prepare();
                hashParts(scalarHasher, scalarParts);

                // update such that currentHash always represents
                // the hash of the value we just "nexted" past
                currentHash = scalarHasher.digest();

                if (!containerHasherStack.isEmpty()) {
                    containerHasherStack.peekFirst().update(currentHash);
                }
            }
        }

        ionType = delegate.next();

        if (ionType != null) {
            if (isNullValue() || !IonType.isContainer(ionType)) {
                // capture the scalarParts here to avoid a scenario in which the caller is using
                // the deprecated hasNext() method, which clears value state from the wrapped reader
                scalarParts = hashBytes.scalarOrNullParts(ionType, this);
            }
        }

        return ionType;
    }

    @Override
    public void stepIn() {
        SymbolToken fieldName = getFieldNameSymbol();
        SymbolToken[] annotations = getTypeAnnotationSymbols();

        delegate.stepIn();

        ContainerHasher hasher = ionType == IonType.STRUCT
                                   ? new StructHasher(fieldName, annotations)
                                   : new ContainerHasher(ionType, fieldName, annotations);
        hasher.prepare();

        containerHasherStack.addFirst(hasher);
        ionType = null;
        currentHash = IonHashBytes.EMPTY_BYTE_ARRAY;
    }

    @Override
    public void stepOut() {
        if (containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Cannot stepOut any further, already at top level.");
        }

        // the caller may be bailing on the current container;
        // ensure we consume the rest of it in order to compute currentHash correctly
        consumeRemainder();

        currentHash = containerHasherStack.pop().digest();

        if (!containerHasherStack.isEmpty()) {
            containerHasherStack.peekFirst().update(currentHash);
        }

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

    /**
     * Centralizes fieldname and annotation handling for scalar and container values.
     */
    private abstract class AbstractHasher implements IonHasher {
        SymbolToken fieldName;
        SymbolToken[] annotations;

        final IonHasher hasher = hasherProvider.newHasher();
        private IonHasher fieldHasher;
        private IonHasher annotationHasher;

        private AbstractHasher(SymbolToken fieldName, SymbolToken[] annotations) {
            this.fieldName = fieldName;
            this.annotations = annotations;
        }

        private AbstractHasher(IonHasher fieldHasher, IonHasher annotationHasher) {
            this.fieldHasher = fieldHasher;
            this.annotationHasher = annotationHasher;
        }

        void prepare() {
            if (fieldName != null) {
                if (fieldHasher == null) {
                    fieldHasher = hasherProvider.newHasher();
                }
                fieldHasher.update(symbolTokenHash(fieldName));
            }

            if (annotations.length > 0) {
                if (annotationHasher == null) {
                    annotationHasher = hasherProvider.newHasher();
                }

                annotationHasher.update(new byte[] {hashBytes.containerTQ(null)});
                for (SymbolToken annotation : annotations) {
                    annotationHasher.update(symbolTokenHash(annotation));
                }
            }
        }

        @Override
        public void update(byte[] bytes) {
            hasher.update(bytes);
        }

        @Override
        public byte[] digest() {
            byte[] hash = hasher.digest();

            if (annotations.length > 0) {
                annotationHasher.update(hash);
                hash = annotationHasher.digest();
            }

            if (fieldName != null) {
                fieldHasher.update(hash);
                hash = fieldHasher.digest();
            }

            return hash;
        }

        private final byte[] symbolTokenHash(SymbolToken token) {
            hashParts(symbolHasher, hashBytes.symbolParts(token));
            return symbolHasher.digest();
        }
    }

    /**
     * A new ContainerHasher (or StructHasher) is used for each list, sexp, or struct.
     */
    private class ContainerHasher extends AbstractHasher {
        private IonType ionType;

        private ContainerHasher(IonType ionType, SymbolToken fieldName, SymbolToken[] annotations) {
            super(fieldName, annotations);
            assert IonType.isContainer(ionType);
            this.ionType = ionType;
        }

        @Override
        void prepare() {
            super.prepare();
            byte tq = hashBytes.containerTQ(ionType);
            hasher.update(new byte[] {tq});
        }
    }

    /**
     * Collects and sorts hashes of struct fields before providing a digest.
     */
    private class StructHasher extends ContainerHasher {
        private final Collection<byte[]> hashes = new TreeSet<>(BYTE_ARRAY_COMPARATOR);

        private StructHasher(SymbolToken fieldName, SymbolToken[] annotations) {
            super(IonType.STRUCT, fieldName, annotations);
        }

        @Override
        public void update(byte[] hash) {
            hashes.add(hash);
        }

        @Override
        public byte[] digest() {
            for(byte[] hash : hashes) {
                hasher.update(hash);
            }
            return super.digest();
        }
    }

    /**
     * Singleton responsible for hashing all scalar and null values.
     */
    private class ScalarHasher extends AbstractHasher {
        private ScalarHasher() {
            super(hasherProvider.newHasher(), hasherProvider.newHasher());
        }

        ScalarHasher withFieldName(SymbolToken fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        ScalarHasher withAnnotations(SymbolToken[] annotations) {
            this.annotations = annotations;
            return this;
        }
    }

    private static void hashParts(IonHasher hasher, byte[][] parts) {
        for (byte[] part : parts) {
            if (part.length > 0) {
                hasher.update(part);
            }
        }
    }

    private static final ByteArrayComparator BYTE_ARRAY_COMPARATOR = new ByteArrayComparator();
    static class ByteArrayComparator implements Comparator<byte[]>, Serializable {
        @Override
        public int compare(byte[] a, byte[] b) {
            int i = 0;
            while (i < a.length && i < b.length) {
                int aByte = a[i] & 0xFF;
                int bByte = b[i] & 0xFF;
                if (aByte != bByte) {
                    return (aByte - bByte) < 0 ? -1 : 1;
                }
                i++;
            }
            int lenDiff = a.length - b.length;
            return lenDiff == 0 ? 0 : (lenDiff < 0 ? -1 : 1);
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        hashBytes.close();
    }


    ///////// The remaining methods are all handled by the delegate ///////////

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
