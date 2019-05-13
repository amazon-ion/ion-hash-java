/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.ionhash;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

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
class HasherImpl implements Hasher {
    private static final byte BEGIN_MARKER_BYTE      = 0x0B;
    private static final byte END_MARKER_BYTE        = 0x0E;
    private static final byte ESCAPE_BYTE            = 0x0C;
    private static final byte[] BEGIN_MARKER         = new byte[] { BEGIN_MARKER_BYTE };
    private static final byte[] END_MARKER           = new byte[] { END_MARKER_BYTE };

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

    HasherImpl(IonHasherProvider hasherProvider) {
        this.hasherProvider = hasherProvider;
        this.hasher = hasherProvider.newHasher();
        this.symbolHasher = new SymbolHasher();
        this.scalarHasher = new ScalarHasherImpl(hasher);
    }

    public ScalarHasher scalar() {
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

    @Override
    public void enable() {
        if (!containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Unexpected call to enable();  hasher is already enabled.");
        }
    }

    @Override
    public void disable() {
        if (!containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Hasher can only be disabled at the same level it was enabled.");
        }
    }

    public void stepIn(IonType containerType, SymbolToken fieldName, SymbolToken[] annotations) {
        ContainerHasher containerHasher;
        if (containerType == IonType.STRUCT) {
            containerHasher = new StructHasher(currentChildHasher(), fieldName, annotations);
        } else {
            containerHasher = new ContainerHasher(currentChildHasher(), containerType, fieldName, annotations);
        }

        containerHasherStack.addFirst(containerHasher);
        scalarHasher.withHasher(currentChildHasher());
    }

    public void stepOut() {
        if (containerHasherStack.isEmpty()) {
            throw new IllegalStateException("Cannot stepOut any further, already at top level.");
        }

        ContainerHasher containerHasher = containerHasherStack.pop();
        containerHasher.finish();

        if (!containerHasherStack.isEmpty()) {
            ContainerHasher ch = containerHasherStack.peekFirst();
            if (ch instanceof StructHasher) {
                ((StructHasher)ch).updateWithDigest(containerHasher.hasher().digest());
            }
        }
        scalarHasher.withHasher(currentChildHasher());
    }

    public byte[] digest() {
        if (!containerHasherStack.isEmpty()) {
            return EMPTY_BYTE_ARRAY;
        }
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

        @SuppressWarnings("deprecation")
        private SymbolHasher() {
            try {
                this.baos = new ByteArrayOutputStream();
                writer = com.amazon.ion.impl.bin._PrivateIon_HashTrampoline.newIonWriter(baos);
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

        public final void prepare() {
            if (!containerHasherStack.isEmpty() && fieldName != null) {
                beginMarker();
                updateTQandRepresentation(symbolHasher.symbolParts(fieldName));
                endMarker();
            }

            if (annotations != null && annotations.length > 0) {
                beginMarker();
                hasher.update(TQ_ANNOTATED_VALUE);
                for (SymbolToken annotation : annotations) {
                    beginMarker();
                    updateTQandRepresentation(symbolHasher.symbolParts(annotation));
                    endMarker();
                }
            }
        }

        final IonHasher hasher() {
            return hasher;
        }

        final void beginMarker() {
            hasher.update(BEGIN_MARKER);
        }

        final void endMarker() {
            hasher.update(END_MARKER);
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

        // impl assumes this method is called AFTER this object is removed from the containerHasherStack (if present)
        void finish() {
            if (annotations != null && annotations.length > 0) {
                endMarker();
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

            beginMarker();
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
            endMarker();
            super.finish();
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

        void updateWithDigest(byte[] hash) {
            hashes.add(hash);
        }

        @Override
        void finish() {
            Collections.sort(hashes, BYTE_ARRAY_COMPARATOR);
            for(byte[] hash : hashes) {
                hasher.update(escape(hash));
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
    class ScalarHasherImpl extends AbstractHasher implements ScalarHasher {
        private final IonWriter scalarWriter;
        private final ByteArrayOutputStream scalarBaos;

        @SuppressWarnings("deprecation")
        ScalarHasherImpl(IonHasher hasher) {
            super(hasher, null, null);
            try {
                this.scalarBaos = new ByteArrayOutputStream();
                this.scalarWriter = com.amazon.ion.impl.bin._PrivateIon_HashTrampoline.newIonWriter(scalarBaos);
            } catch (IOException e) {
                throw new IonHashException(e);
            }
        }

        public ScalarHasher withFieldName(SymbolToken fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public ScalarHasher withAnnotations(SymbolToken[] annotations) {
            this.annotations = annotations;
            return this;
        }

        public ScalarHasher withHasher(IonHasher hasher) {
            this.hasher = hasher;
            return this;
        }

        @Override
        void finish() {
            endMarker();
            super.finish();
        }

        public void updateBlob(byte[] value) throws IOException {
            writeScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value));
        }

        public void updateBlob(byte[] value, int start, int len) throws IOException {
            writeScalar(IonType.BLOB, () -> scalarWriter.writeBlob(value, start, len));
        }

        public void updateBool(boolean value) throws IOException {
            writeScalar(IonType.BOOL, () -> scalarWriter.writeBool(value));
        }

        public void updateClob(byte[] value) throws IOException {
            writeScalar(IonType.CLOB, () -> scalarWriter.writeClob(value));
        }

        public void updateClob(byte[] value, int start, int len) throws IOException {
            writeScalar(IonType.CLOB, () -> scalarWriter.writeClob(value, start, len));
        }

        public void updateDecimal(BigDecimal value) throws IOException {
            writeScalar(IonType.DECIMAL, () -> scalarWriter.writeDecimal(value));
        }

        public void updateFloat(double value) throws IOException {
            if (Double.valueOf(value).equals(0.0)) {
                // value is 0.0, not -0.0
                writeScalar(IonType.FLOAT, null, FLOAT_POSITIVE_ZERO_PARTS);
            } else {
                writeScalar(IonType.FLOAT, () -> scalarWriter.writeFloat(value));
            }
        }

        public void updateInt(BigInteger value) throws IOException {
            writeScalar(IonType.INT, () -> scalarWriter.writeInt(value));
        }

        public void updateNull() throws IOException {
            updateNull(IonType.NULL);
        }

        public void updateNull(IonType type) throws IOException {
            writeScalar(type, () -> scalarWriter.writeNull(type));
        }

        public void updateString(String value) throws IOException {
            writeScalar(IonType.STRING, () -> scalarWriter.writeString(value));
        }

        public void updateSymbol(String value) throws IOException {
            updateSymbolToken(Hasher.newSymbolToken(value));
        }

        public void updateSymbolToken(SymbolToken value) throws IOException {
            writeScalar(IonType.SYMBOL, null, symbolHasher.symbolParts(value));
        }

        public void updateTimestamp(Timestamp value) throws IOException {
            writeScalar(IonType.TIMESTAMP, () -> scalarWriter.writeTimestamp(value));
        }

        private void writeScalar(IonType ionType, Updatable scalarUpdater) throws IOException {
            writeScalar(ionType, scalarUpdater, null);
        }

        private void writeScalar(IonType ionType, Updatable scalarUpdater, byte[][] tqAndRepresentation) throws IOException {
            beginMarker();
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
                    ((StructHasher)containerHasher).updateWithDigest(hasher.digest());
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

    // if bytes contains one or more BEGIN_MARKER_BYTEs, END_MARKER_BYTEs, or ESCAPE_BYTEs,
    // returns a new array with such bytes preceeded by a ESCAPE_BYTE;
    // otherwise, returns the original array unchanged
    static byte[] escape(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        int cnt = 0;
        for (byte b : bytes) {
            if (b == BEGIN_MARKER_BYTE || b == END_MARKER_BYTE || b == ESCAPE_BYTE) {
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
            if (b == BEGIN_MARKER_BYTE || b == END_MARKER_BYTE || b == ESCAPE_BYTE) {
                escapedBytes[idx++] = ESCAPE_BYTE;
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
