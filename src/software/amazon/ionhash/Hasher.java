package software.amazon.ionhash;

import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;
import software.amazon.ion.impl.bin.PrivateIonHashTrampoline;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.TreeSet;

/**
 * Provides core hash functionality via update/digest/stepIn/stepOut operations
 * for use by streaming hash readers and writers.  It relies on the binary
 * encoding implementation in IonRawBinaryWriter when possible.
 * <p/>
 * This class is not thread-safe.
 */
class Hasher implements Closeable {
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final IonHasherProvider hasherProvider;
    private final IonHasher symbolHasher;
    private final ScalarHasher scalarHasher;

    private final Deque<ContainerHasher> containerHasherStack = new ArrayDeque<>();

    private final ByteArrayOutputStream baos;
    private final IonWriter writer;

    Hasher(IonHasherProvider hasherProvider) {
        this.hasherProvider = hasherProvider;
        this.symbolHasher = hasherProvider.newHasher();
        this.scalarHasher = new ScalarHasher();

        try {
            this.baos = new ByteArrayOutputStream();
            writer = PrivateIonHashTrampoline.newIonWriter(baos);
        } catch (IOException e) {
            throw new IonHashException(e);
        }
    }

    ScalarHasher scalar() {
        return scalarHasher;
    }

    void stepIn(IonType containerType, SymbolToken fieldName, SymbolToken[] annotations) {
        ContainerHasher hasher = containerType == IonType.STRUCT
                ? new StructHasher(fieldName, annotations)
                : new ContainerHasher(containerType, fieldName, annotations);

        hasher.prepare();

        containerHasherStack.addFirst(hasher);
    }

    byte[] stepOut() {
        if (containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Cannot stepOut any further, already at top level.");
        }

        byte[] currentHash = containerHasherStack.pop().digest();

        if (!containerHasherStack.isEmpty()) {
            containerHasherStack.peekFirst().update(currentHash);
        }

        return currentHash;
    }

    // split representation and TQ parts;  also handles any special case binary cleanup
    private byte[][] scalarOrNullSplitParts(IonType type, byte[] bytes) throws IOException {
        int tl = bytes[0];
        int offset = 1 + getLengthLength(bytes);

        if (type == IonType.INT && bytes.length > offset) {
            // ignore sign byte prepended by BigInteger.toByteArray() when the magnitude
            // ends at byte boundary (the 'intLength512' test is an example of this)
            if ((bytes[offset] & 0xFF) == 0) {
                offset++;
            }
        }

        // the representation is everything after TL and length
        byte[] representation = Arrays.copyOfRange(bytes, offset, bytes.length);
        byte tq = (tl & 0x0F) == 0x0F ? (byte)tl : (byte)(tl & 0xF0);

        if (type == IonType.BOOL) {
            tq = (byte)tl;
        }

        return new byte[][] { new byte[] {tq}, representation };
    }

    // TQ, representation
    private byte[][] symbolParts(SymbolToken symbol) {
        String text = symbol == null ? null : symbol.getText();
        if (text == null && (symbol == null || symbol.getSid() != 0)) {
            throw new IonHashException("Unable to resolve SID "
                    + (symbol != null ? symbol.getSid() : "null"));
        }

        byte[] symbolBytes;
        try {
            byte tq = 0x70;
            if (text == null && symbol.getSid() == 0) {
                tq |= 0x01;
            }
            baos.reset();
            writer.writeString(text);
            writer.finish();
            symbolBytes = baos.toByteArray();

            int offset = 1 + getLengthLength(symbolBytes);
            byte[] representation = Arrays.copyOfRange(symbolBytes, offset, symbolBytes.length);

            return new byte[][] {new byte[] {tq}, representation};
        } catch (IOException e) {
            throw new IonHashException(e);
        }
    }

    // returns a count of bytes in the "length" field
    private int getLengthLength(byte[] arr) {
        if ((arr[0] & 0x0F) == 0x0E) {
            // read subsequent byte(s) as the "length" field
            for (int i = 1; i < arr.length; i++) {
                if ((arr[i] & 0x80) != 0) {
                    return i;
                }
            }
            throw new IllegalStateException("Problem while reading VarUInt!");
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        baos.close();
        scalarHasher.close();
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

                annotationHasher.update(new byte[] {(byte)0xE0});
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
            hashParts(symbolHasher, symbolParts(token));
            return symbolHasher.digest();
        }

        void hashParts(byte[][] parts) {
            hashParts(this, parts);
        }

        private void hashParts(IonHasher hasher, byte[][] parts) {
            for (byte[] part : parts) {
                if (part.length > 0) {
                    hasher.update(part);
                }
            }
        }
    }

    /**
     * A new ContainerHasher (or StructHasher) is used for each list, sexp, or struct.
     */
    class ContainerHasher extends AbstractHasher {
        private IonType ionType;

        ContainerHasher(IonType ionType, SymbolToken fieldName, SymbolToken[] annotations) {
            super(fieldName, annotations);
            assert IonType.isContainer(ionType);
            this.ionType = ionType;
        }

        @Override
        void prepare() {
            super.prepare();
            byte tq;
            switch (ionType) {
                case LIST:
                    tq = (byte)0xB0;
                    break;
                case SEXP:
                    tq = (byte)0xC0;
                    break;
                case STRUCT:
                    tq = (byte)0xD0;
                    break;
                default:
                    throw new IonHashException("Unexpected container type " + ionType);
            }
            hasher.update(new byte[] {tq});
        }
    }

    /**
     * Collects and sorts hashes of struct fields before providing a digest.
     */
    class StructHasher extends ContainerHasher {
        private final Collection<byte[]> hashes = new TreeSet<>(BYTE_ARRAY_COMPARATOR);

        StructHasher(SymbolToken fieldName, SymbolToken[] annotations) {
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
    class ScalarHasher extends AbstractHasher implements Closeable {
        private final IonWriter scalarWriter;
        private final ByteArrayOutputStream scalarBaos;

        ScalarHasher() {
            super(hasherProvider.newHasher(), hasherProvider.newHasher());

            try {
                this.scalarBaos = new ByteArrayOutputStream();
                this.scalarWriter = PrivateIonHashTrampoline.newIonWriter(scalarBaos);
            } catch (IOException e) {
                throw new IonHashException(e);
            }
        }

        ScalarHasher withFieldName(SymbolToken fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        ScalarHasher withAnnotations(SymbolToken[] annotations) {
            this.annotations = annotations;
            return this;
        }

        void updateBlob(byte[] value) throws IOException {
            updateScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value));
        }

        void updateBlob(byte[] value, int start, int len) throws IOException {
            updateScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value, start, len));
        }

        void updateBool(boolean value) throws IOException {
            updateScalar(IonType.BOOL, () -> scalarWriter.writeBool(value));
        }

        void updateClob(byte[] value) throws IOException {
            updateScalar(IonType.CLOB, () -> scalarWriter.writeClob(value));
        }

        void updateClob(byte[] value, int start, int len) throws IOException {
            updateScalar(IonType.CLOB, () -> scalarWriter.writeClob(value, start, len));
        }

        void updateDecimal(BigDecimal value) throws IOException {
            updateScalar(IonType.DECIMAL, () -> scalarWriter.writeDecimal(value));
        }

        void updateFloat(double value) throws IOException {
            if (new Double(value).equals(0.0)) {
                // value is 0.0, not -0.0
                hashParts(new byte[][] {new byte[] {0x40}});
            } else {
                updateScalar(IonType.FLOAT, () -> scalarWriter.writeFloat(value));
            }
        }

        void updateInt(BigInteger value) throws IOException {
            updateScalar(IonType.INT, () -> scalarWriter.writeInt(value));
        }

        void updateNull() throws IOException {
            updateScalar(IonType.NULL, () -> scalarWriter.writeNull());
        }

        void updateNull(IonType type) throws IOException {
            updateScalar(type, () -> scalarWriter.writeNull(type));
        }

        void updateString(String value) throws IOException {
            updateScalar(IonType.STRING, () -> scalarWriter.writeString(value));
        }

        void updateSymbol(String value) throws IOException {
            updateSymbolToken(newSymbolToken(value));
        }

        void updateSymbolToken(SymbolToken value) throws IOException {
            hashParts(symbolParts(value));
        }

        void updateTimestamp(Timestamp value) throws IOException {
            updateScalar(IonType.TIMESTAMP, () -> scalarWriter.writeTimestamp(value));
        }

        private void updateScalar(IonType ionType, Updatable scalarUpdater) throws IOException {
            scalarBaos.reset();
            scalarUpdater.update();
            scalarWriter.finish();
            hashParts(scalarOrNullSplitParts(ionType, scalarBaos.toByteArray()));
        }

        @Override
        public byte[] digest() {
            byte[] hash = super.digest();
            if (!containerHasherStack.isEmpty()) {
                containerHasherStack.peekFirst().update(hash);
            }
            return hash;
        }

        @Override
        public void close() throws IOException {
            scalarWriter.close();
            scalarBaos.close();
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
