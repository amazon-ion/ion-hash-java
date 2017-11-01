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
    private static final byte[] TQ_SYMBOL      = new byte[] {      0x70};
    private static final byte[] TQ_SYMBOL_SID0 = new byte[] {      0x71};
    private static final byte[] TQ_LIST        = new byte[] {(byte)0xB0};
    private static final byte[] TQ_SEXP        = new byte[] {(byte)0xC0};
    private static final byte[] TQ_STRUCT      = new byte[] {(byte)0xD0};
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final IonHasherProvider hasherProvider;
    private final SymbolHasher symbolHasher;
    private final ScalarHasher scalarHasher;

    private final Deque<ContainerHasher> containerHasherStack = new ArrayDeque<>();

    Hasher(IonHasherProvider hasherProvider) {
        this.hasherProvider = hasherProvider;

        // cache for digests of symbols and common values
        DigestCache digestCache = System.getProperty("ion-hash-java.useDigestCache", "true").equals("true")
                ? new DigestCache() : new DigestCache.NoOpDigestCache();

        this.symbolHasher = new SymbolHasher(hasherProvider.newHasher(), digestCache);
        this.scalarHasher = new ScalarHasher(digestCache);
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

    Digest stepOut() {
        if (containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Cannot stepOut any further, already at top level.");
        }

        Digest digest = containerHasherStack.pop().digest();

        if (!containerHasherStack.isEmpty()) {
            containerHasherStack.peekFirst().update(digest.fieldNameAnnotatedValue());
        }

        return digest;
    }

    @Override
    public void close() throws IOException {
        symbolHasher.close();
        scalarHasher.close();
    }


    /**
     * Centralizes logic for hashing symbols caching their digests;  this includes
     * annotations, field names, and values that are symbols.
     */
    class SymbolHasher implements Closeable {
        private final IonHasher hasher;
        private final ByteArrayOutputStream baos;
        private final IonWriter writer;
        private final DigestCache digestCache;

        private SymbolHasher(IonHasher hasher, DigestCache digestCache) {
            this.hasher = hasher;

            try {
                this.baos = new ByteArrayOutputStream();
                writer = PrivateIonHashTrampoline.newIonWriter(baos);
            } catch (IOException e) {
                throw new IonHashException(e);
            }

            this.digestCache = digestCache;
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

        private byte[] digest(SymbolToken symbol) {
            byte[] digest = digestCache.getSymbol(symbol);
            if (digest == null) {
                byte[][] parts = symbolParts(symbol);
                hasher.update(parts[0]);
                hasher.update(parts[1]);
                digest = hasher.digest();
                digestCache.putSymbol(symbol, digest);
            }

            return digest;
        }

        public void close() throws IOException {
            writer.close();
            baos.close();
        }
    }

    /**
     * Lightweight, reusable value class that represent various hashes of a value.  For field 'a' in
     * the following struct:
     * <pre>
     *   {
     *     a:annot::5
     *   }
     * </pre>
     * <ul>
     *   <li>value represents the hash of:  5
     *   <li>annotatedValue represents the hash of:  annot::5
     *   <li>fieldNameAnnotatedValue represents the hash of:  a:annot::5
     * </ul>
     */
    static class Digest {
        private byte[] value;
        private byte[] annotatedValue;
        private byte[] fieldNameAnnotatedValue;

        byte[] value() {
            return value;
        }

        byte[] annotatedValue() {
            return annotatedValue;
        }

        byte[] fieldNameAnnotatedValue() {
            return fieldNameAnnotatedValue;
        }
    }

    /**
     * Centralizes fieldname and annotation handling for scalar and container values.
     */
    private abstract class AbstractHasher {
        SymbolToken fieldName;
        SymbolToken[] annotations;
        Digest digest = new Digest();

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
                byte[] fieldNameHash = symbolHasher.digest(fieldName);
                fieldHasher.update(fieldNameHash);
            }

            if (annotations != null && annotations.length > 0) {
                if (annotationHasher == null) {
                    annotationHasher = hasherProvider.newHasher();
                }

                annotationHasher.update(new byte[] {(byte)0xE0});
                for (SymbolToken annotation : annotations) {
                    annotationHasher.update(symbolHasher.digest(annotation));
                }
            }
        }

        public void update(byte[] bytes) {
            hasher.update(bytes);
        }

        byte[] valueDigest() {
            return hasher.digest();
        }

        Digest digest() {
            return digest(null);
        }

        Digest digest(byte[] valueDigest) {
            if (valueDigest != null) {
                digest.value = valueDigest;
            } else {
                digest.value = hasher.digest();
            }

            if (annotations != null && annotations.length > 0) {
                annotationHasher.update(digest.value);
                digest.annotatedValue = annotationHasher.digest();
            } else {
                digest.annotatedValue = digest.value;
            }

            if (fieldName != null) {
                fieldHasher.update(digest.annotatedValue);
                digest.fieldNameAnnotatedValue = fieldHasher.digest();
            } else {
                digest.fieldNameAnnotatedValue = digest.annotatedValue;
            }

            return digest;
        }

        void hashParts(byte[][] parts) {
            for (byte[] part : parts) {
                if (part.length > 0) {
                    update(part);
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
    }

    /**
     * Collects and sorts hashes of struct fields before providing a digest.
     */
    class StructHasher extends ContainerHasher {
        private final List<byte[]> hashes = new ArrayList<>();

        StructHasher(SymbolToken fieldName, SymbolToken[] annotations) {
            super(IonType.STRUCT, fieldName, annotations);
        }

        @Override
        public void update(byte[] hash) {
            hashes.add(hash);
        }

        @Override
        public Digest digest() {
            Collections.sort(hashes, BYTE_ARRAY_COMPARATOR);
            for(byte[] hash : hashes) {
                hasher.update(hash);
            }
            return super.digest();
        }
    }

    /**
     * Responsible for hashing all scalar and null values.
     */
    class ScalarHasher extends AbstractHasher implements Closeable {
        private final IonWriter scalarWriter;
        private final ByteArrayOutputStream scalarBaos;
        private final DigestCache digestCache;

        ScalarHasher(DigestCache digestCache) {
            super(hasherProvider.newHasher(), hasherProvider.newHasher());

            try {
                this.scalarBaos = new ByteArrayOutputStream();
                this.scalarWriter = PrivateIonHashTrampoline.newIonWriter(scalarBaos);
            } catch (IOException e) {
                throw new IonHashException(e);
            }

            this.digestCache = digestCache;
        }

        ScalarHasher withFieldName(SymbolToken fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        ScalarHasher withAnnotations(SymbolToken[] annotations) {
            this.annotations = annotations;
            return this;
        }

        Digest digestBlob(byte[] value) throws IOException {
            return digestScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value));
        }

        Digest digestBlob(byte[] value, int start, int len) throws IOException {
            return digestScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value, start, len));
        }

        Digest digestBool(boolean value) throws IOException {
            return digestScalar(IonType.BOOL,
                    () -> scalarWriter.writeBool(value),
                    digestCache.getBool(value),
                    (digest) -> digestCache.putBool(value, digest));
        }

        Digest digestClob(byte[] value) throws IOException {
            return digestScalar(IonType.CLOB, () -> scalarWriter.writeClob(value));
        }

        Digest digestClob(byte[] value, int start, int len) throws IOException {
            return digestScalar(IonType.CLOB, () -> scalarWriter.writeClob(value, start, len));
        }

        Digest digestDecimal(BigDecimal value) throws IOException {
            return digestScalar(IonType.DECIMAL, () -> scalarWriter.writeDecimal(value));
        }

        Digest digestFloat(double value) throws IOException {
            if (new Double(value).equals(0.0)) {
                // value is 0.0, not -0.0
                hashParts(new byte[][] {new byte[] {0x40}});
                return digest();
            } else {
                return digestScalar(IonType.FLOAT, () -> scalarWriter.writeFloat(value));
            }
        }

        Digest digestInt(BigInteger value) throws IOException {
            return digestScalar(IonType.INT, () -> scalarWriter.writeInt(value));
        }

        Digest digestNull() throws IOException {
            return digestNull(IonType.NULL);
        }

        Digest digestNull(IonType type) throws IOException {
            return digestScalar(type,
                    () -> scalarWriter.writeNull(type),
                    digestCache.getNull(type),
                    (digest) -> digestCache.putNull(type, digest));
        }

        Digest digestString(String value) throws IOException {
            return digestScalar(IonType.STRING, () -> scalarWriter.writeString(value));
        }

        Digest digestSymbol(String value) throws IOException {
            return digestSymbolToken(newSymbolToken(value));
        }

        Digest digestSymbolToken(SymbolToken value) throws IOException {
            byte[] valueDigest = symbolHasher.digest(value);
            Digest digest = digest(valueDigest);   // this is a no-op unless there's a fieldName or annotation(s)
            updateContainer(digest.fieldNameAnnotatedValue());
            return digest;
        }

        Digest digestTimestamp(Timestamp value) throws IOException {
            return digestScalar(IonType.TIMESTAMP, () -> scalarWriter.writeTimestamp(value));
        }

        private Digest digestScalar(IonType ionType, Updatable scalarUpdater) throws IOException {
            return digestScalar(ionType, scalarUpdater, null, null);
        }

        private Digest digestScalar(IonType ionType, Updatable scalarUpdater,
                byte[] valueDigest, CacheableValue cacheableValue) throws IOException {

            if (cacheableValue != null) {
                if (valueDigest == null) {
                    writeScalar(ionType, scalarUpdater);
                    valueDigest = valueDigest();
                    cacheableValue.put(valueDigest);
                }
                digest = digest(valueDigest);
            } else {
                writeScalar(ionType, scalarUpdater);
                digest = digest();
            }

            updateContainer(digest.fieldNameAnnotatedValue());
            return digest;
        }

        private void writeScalar(IonType ionType, Updatable scalarUpdater) throws IOException {
            scalarBaos.reset();
            scalarUpdater.update();
            scalarWriter.finish();
            hashParts(scalarOrNullSplitParts(ionType, scalarBaos.toByteArray()));
        }

        private void updateContainer(byte[] digest) {
            if (!containerHasherStack.isEmpty()) {
                containerHasherStack.peekFirst().update(digest);
            }
        }

        @Override
        public void close() throws IOException {
            scalarWriter.close();
            scalarBaos.close();
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
    }

    // lambda interface that facilitates caching the digest of a value
    private interface CacheableValue {
        void put(byte[] digest);
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
