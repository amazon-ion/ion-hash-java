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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

/**
 * Provides core hash functionality for use by streaming hash readers and writers.
 * It relies on the binary encoding implementation in IonRawBinaryWriter when possible.
 * <p/>
 * Callers should assume that Digest objects returned by this API will be reused, and
 * may be changed during the next call to this API.
 * <p/>
 * This class is not thread-safe.
 */
class Hasher implements Closeable {
    private static final byte DELIMITER_BYTE         = (byte)0xEF;
    private static final byte DELIMITER_ESCAPE_BYTE  = DELIMITER_BYTE;
    private static final byte[] VALUE_DELIMITER      = new byte[] {DELIMITER_BYTE};

    private static final byte[] TQ_SYMBOL            = new byte[] {      0x70};
    private static final byte[] TQ_SYMBOL_SID0       = new byte[] {      0x71};
    private static final byte[] TQ_LIST              = new byte[] {(byte)0xB0};
    private static final byte[] TQ_SEXP              = new byte[] {(byte)0xC0};
    private static final byte[] TQ_STRUCT            = new byte[] {(byte)0xD0};
    private static final byte[] TQ_ANNOTATED_VALUE   = new byte[] {(byte)0xE0};

    static final byte[] EMPTY_BYTE_ARRAY             = new byte[0];

    private static final byte[][] FLOAT_POSITIVE_ZERO_PARTS = new byte[][] {new byte[] {0x40}};

    private final IonHasherProvider hasherProvider;
    private final IonHasher hasher;
    private final SymbolHasher symbolHasher;
    private final ScalarHasher scalarHasher;
    private final Deque<ContainerHasher> containerHasherStack = new ArrayDeque<>();

    Hasher(IonHasherProvider hasherProvider) {
        this.hasherProvider = hasherProvider;
        this.hasher = hasherProvider.newHasher();
        this.symbolHasher = new SymbolHasher();
        this.scalarHasher = new ScalarHasher(hasher);
    }

    ScalarHasher scalar() {
        return scalarHasher;
    }

    IonHasher currentHasher() {
        if (!containerHasherStack.isEmpty()) {
            return containerHasherStack.peekFirst().hasher();
        }
        return hasher;
    }

    IonHasher currentChildHasher() {
        if (!containerHasherStack.isEmpty()) {
            return containerHasherStack.peekFirst().childHasher();
        }
        return hasher;
    }


    void stepIn(IonType containerType, SymbolToken fieldName, SymbolToken[] annotations) {
        ContainerHasher containerHasher;
        if (containerType == IonType.STRUCT) {
            containerHasher = new StructHasher(currentChildHasher(), fieldName, annotations);
        } else {
            containerHasher = new ContainerHasher(currentChildHasher(), containerType, fieldName, annotations);
        }

        containerHasherStack.addFirst(containerHasher);
        scalarHasher.withHasher(currentChildHasher());
    }

    void stepOut() {
        if (containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Cannot stepOut any further, already at top level.");
        }

        ContainerHasher containerHasher = containerHasherStack.pop();
        containerHasher.finish();

        if (!containerHasherStack.isEmpty()) {
            ContainerHasher ch = containerHasherStack.peekFirst();
            if (ch instanceof StructHasher) {
                ch.updateWithDigest(containerHasher.hasher().digest());
            }
        }
        scalarHasher.withHasher(currentChildHasher());
    }

    byte[] digest() {
        // TBD can we enforce that this is only called at an appropriate time?
        return currentHasher().digest();
    }

    @Override
    public void close() throws IOException {
        symbolHasher.close();
        scalarHasher.close();
    }


    /**
     * Centralizes logic for constructing the bytes for symbols;  this includes
     * annotations, field names, and values that are symbols.
     */
    class SymbolHasher implements Closeable {
        private final ByteArrayOutputStream baos;
        private final IonWriter writer;

        private SymbolHasher() {
            try {
                this.baos = new ByteArrayOutputStream();
                writer = PrivateIonHashTrampoline.newIonWriter(baos);
            } catch (IOException e) {
                throw new IonHashException(e);
            }
        }

        // returns TQ, representation
        private byte[][] symbolParts(SymbolToken symbol) {
            String text = symbol == null ? null : symbol.getText();
            if (text == null && (symbol == null || symbol.getSid() != 0)) {
                throw new IonHashException("Unable to resolve SID "
                        + (symbol != null ? symbol.getSid() : "null"));
            }

            byte[] symbolBytes;
            try {
                byte[] tq = TQ_SYMBOL;
                if (text == null && symbol.getSid() == 0) {
                    tq  = TQ_SYMBOL_SID0;
                }
                baos.reset();
                writer.writeString(text);
                writer.finish();
                symbolBytes = baos.toByteArray();

                int offset = 1 + getLengthLength(symbolBytes);
                byte[] representation = Arrays.copyOfRange(symbolBytes, offset, symbolBytes.length);

                return new byte[][] {tq, representation};
            } catch (IOException e) {
                throw new IonHashException(e);
            }
        }

        public void close() throws IOException {
            writer.close();
            baos.close();
        }
    }

    /**
     * Centralizes fieldname and annotation handling for scalar and container values.
     */
    private abstract class AbstractHasher {
        IonHasher hasher;
        SymbolToken fieldName;
        SymbolToken[] annotations;

        private AbstractHasher(IonHasher hasher, SymbolToken fieldName, SymbolToken[] annotations) {
            this.hasher = hasher;
            this.fieldName = fieldName;
            this.annotations = annotations;

            prepare();
        }

        final void prepare() {
            if (!containerHasherStack.isEmpty() && fieldName != null) {
                updateDelimiter();

                updateDelimiter();
                updateTQandRepresentation(symbolHasher.symbolParts(fieldName));
                updateDelimiter();
            }

            if (annotations != null && annotations.length > 0) {
                updateDelimiter();
                hasher.update(TQ_ANNOTATED_VALUE);
                for (SymbolToken annotation : annotations) {
                    updateDelimiter();
                    updateTQandRepresentation(symbolHasher.symbolParts(annotation));
                    updateDelimiter();
                }
            }
        }

        final IonHasher hasher() {
            return hasher;
        }

        final void updateDelimiter() {
            hasher.update(VALUE_DELIMITER);
        }

        final void updateTQandRepresentation(byte[][] tqAndRepresentation) {
            byte[] tq = tqAndRepresentation[0];
            hasher.update(tq);

            if (tqAndRepresentation.length == 2) {
                byte[] representation = tqAndRepresentation[1];
                if (representation.length > 0) {
                    hasher.update(escape(representation));
                }
            }
        }

        void updateWithDigest(byte[] bytes) {
            hasher.update(escape(bytes));
        }

        // impl assumes this method is called AFTER this object is removed from the containerHasherStack (if present)
        void finish() {
            if (annotations != null && annotations.length > 0) {
                updateDelimiter();
            }

            if (!containerHasherStack.isEmpty() && fieldName != null) {
                updateDelimiter();
            }
        }
    }

    /**
     * A new ContainerHasher (or StructHasher) is used for each list, sexp, or struct.
     */
    class ContainerHasher extends AbstractHasher {
        private IonType ionType;

        ContainerHasher(IonHasher hasher, IonType ionType, SymbolToken fieldName, SymbolToken[] annotations) {
            super(hasher, fieldName, annotations);
            assert IonType.isContainer(ionType);
            this.ionType = ionType;

            updateDelimiter();
            switch (ionType) {
                case LIST:
                    hasher.update(TQ_LIST);
                    break;
                case SEXP:
                    hasher.update(TQ_SEXP);
                    break;
                case STRUCT:
                    hasher.update(TQ_STRUCT);
                    break;
                default:
                    throw new IonHashException("Unexpected container type " + ionType);
            }
        }

        IonHasher childHasher() {
            return hasher;
        }

        @Override
        void finish() {
            super.finish();
            updateDelimiter();
        }
    }

    /**
     * Collects and sorts hashes of struct fields before providing a digest.
     */
    class StructHasher extends ContainerHasher {
        private final List<byte[]> hashes = new ArrayList<>();
        private final IonHasher childHasher;

        StructHasher(IonHasher hasher, SymbolToken fieldName, SymbolToken[] annotations) {
            super(hasher, IonType.STRUCT, fieldName, annotations);
            childHasher = hasherProvider.newHasher();
        }

        @Override
        IonHasher childHasher() {
            return childHasher;
        }

        @Override
        void updateWithDigest(byte[] hash) {
            hashes.add(escape(hash));
        }

        @Override
        void finish() {
            // these hashes have already been escaped
            Collections.sort(hashes, BYTE_ARRAY_COMPARATOR);
            for(byte[] hash : hashes) {
                hasher.update(hash);
            }
            super.finish();
        }
    }

    /**
     * Responsible for hashing all scalar and null values.  There is a single ScalarHasher
     * instance per Hasher;  typical usage is to call withFieldName() and withAnnotations(),
     * then prepare() (in contrast to ContainerHashers instantiated during stepIn(),
     * which call prepare() during instantiation).
     */
    class ScalarHasher extends AbstractHasher implements Closeable {
        private final IonWriter scalarWriter;
        private final ByteArrayOutputStream scalarBaos;

        ScalarHasher(IonHasher hasher) {
            super(hasher, null, null);
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

        ScalarHasher withHasher(IonHasher hasher) {
            this.hasher = hasher;
            return this;
        }

        @Override
        void finish() {
            super.finish();
            updateDelimiter();
        }

        void updateBlob(byte[] value) throws IOException {
            writeScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value));
        }

        void updateBlob(byte[] value, int start, int len) throws IOException {
            writeScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value, start, len));
        }

        void updateBool(boolean value) throws IOException {
            writeScalar(IonType.BOOL, () -> scalarWriter.writeBool(value));
        }

        void updateClob(byte[] value) throws IOException {
            writeScalar(IonType.CLOB, () -> scalarWriter.writeClob(value));
        }

        void updateClob(byte[] value, int start, int len) throws IOException {
            writeScalar(IonType.CLOB, () -> scalarWriter.writeClob(value, start, len));
        }

        void updateDecimal(BigDecimal value) throws IOException {
            writeScalar(IonType.DECIMAL, () -> scalarWriter.writeDecimal(value));
        }

        void updateFloat(double value) throws IOException {
            if (new Double(value).equals(0.0)) {
                // value is 0.0, not -0.0
                writeScalar(IonType.FLOAT, null, FLOAT_POSITIVE_ZERO_PARTS);
            } else {
                writeScalar(IonType.FLOAT, () -> scalarWriter.writeFloat(value));
            }
        }

        void updateInt(BigInteger value) throws IOException {
            writeScalar(IonType.INT, () -> scalarWriter.writeInt(value));
        }

        void updateNull() throws IOException {
            updateNull(IonType.NULL);
        }

        void updateNull(IonType type) throws IOException {
            writeScalar(type, () -> scalarWriter.writeNull(type));
        }

        void updateString(String value) throws IOException {
            writeScalar(IonType.STRING, () -> scalarWriter.writeString(value));
        }

        void updateSymbol(String value) throws IOException {
            updateSymbolToken(newSymbolToken(value));
        }

        void updateSymbolToken(SymbolToken value) throws IOException {
            writeScalar(IonType.SYMBOL, null, symbolHasher.symbolParts(value));
        }

        void updateTimestamp(Timestamp value) throws IOException {
            writeScalar(IonType.TIMESTAMP, () -> scalarWriter.writeTimestamp(value));
        }

        private void writeScalar(IonType ionType, Updatable scalarUpdater) throws IOException {
            writeScalar(ionType, scalarUpdater, null);
        }

        private void writeScalar(IonType ionType, Updatable scalarUpdater, byte[][] tqAndRepresentation) throws IOException {
            updateDelimiter();
            if (tqAndRepresentation == null) {
                scalarBaos.reset();
                scalarUpdater.update();
                scalarWriter.finish();
                tqAndRepresentation = scalarOrNullSplitParts(ionType, scalarBaos.toByteArray());
            }
            updateTQandRepresentation(tqAndRepresentation);
            finish();
            if (!containerHasherStack.isEmpty()) {
                ContainerHasher containerHasher = containerHasherStack.peekFirst();
                if (containerHasher instanceof StructHasher) {
                    containerHasher.updateWithDigest(hasher.digest());
                }
            }
        }

        @Override
        public void close() throws IOException {
            scalarWriter.close();
            scalarBaos.close();
        }

        // split scalar bytes into TQ and representation;  also handles any special case binary cleanup
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
            byte tq;
            if (type == IonType.BOOL) {
                tq = (byte)tl;
            } else {
                tq = (tl & 0x0F) == 0x0F ? (byte)tl : (byte)(tl & 0xF0);
            }

            return new byte[][] { new byte[] {tq}, representation };
        }
    }

    // if bytes contains the DELIMITER_BYTE, returns a new array with
    // each DELIMITER_BYTE preceeded by a DELIMITER_ESCAPE_BYTE;
    // otherwise, returns the original array unchanged
    static byte[] escape(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        int cnt = 0;
        for (byte b : bytes) {
            if (b == DELIMITER_BYTE) {
                cnt++;
            }
        }
        if (cnt == 0) {
            // happy case, no escaping required
            return bytes;
        }

        byte[] escapedBytes = new byte[bytes.length + cnt];
        int idx = 0;
        for (byte b : bytes) {
            if (b == DELIMITER_BYTE) {
                escapedBytes[idx++] = DELIMITER_ESCAPE_BYTE;
            }
            escapedBytes[idx++] = b;
        }
        return escapedBytes;
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

    // returns a count of bytes in the "length" field
    private static int getLengthLength(byte[] arr) {
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
}
