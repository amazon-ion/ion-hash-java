package software.amazon.ionhash;

import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This IonWriter decorator calculates a hash over the Ion data model.
 * The hash of the IonValue just written or stepped out of is available via digest().
 * <p/>
 * This class is not thread-safe.
 */
class IonHashWriterImpl implements IonHashWriter {
    private static final List<SymbolToken> EMPTY_SYMBOLTOKEN_LIST = Collections.emptyList();
    private static final SymbolToken[] EMPTY_SYMBOLTOKEN_ARRAY = new SymbolToken[] {};

    private final IonWriter delegate;
    private final Hasher hasher;

    private SymbolToken fieldName = null;
    private List<SymbolToken> annotations = EMPTY_SYMBOLTOKEN_LIST;

    IonHashWriterImpl(IonWriter delegate, IonHasherProvider hasherProvider) throws IOException {
        if (delegate == null) {
            throw new NullPointerException("IonWriter must not be null");
        }
        if (hasherProvider == null) {
            throw new NullPointerException("IonHasherProvider must not be null");
        }

        this.delegate = delegate;
        this.hasher = new HasherEngagerImpl(new HasherImpl(hasherProvider));
    }

    @Override
    public byte[] digest() {
        return hasher.digest();
    }

    @Override
    public void stepIn(IonType containerType) throws IOException {
        delegate.stepIn(containerType);
        hasher.stepIn(containerType, fieldName, annotations());

        fieldName = null;
        annotations = EMPTY_SYMBOLTOKEN_LIST;
    }

    @Override
    public void stepOut() throws IOException {
        hasher.stepOut();
        delegate.stepOut();
    }

    @Override
    public void close() throws IOException {
        hasher.close();
    }

    @Override
    public void setFieldName(String name) {
        fieldName = Hasher.newSymbolToken(name);
        delegate.setFieldName(name);
    }

    @Override
    public void setFieldNameSymbol(SymbolToken name) {
        fieldName = name;
        delegate.setFieldNameSymbol(name);
    }

    @Override
    public void addTypeAnnotation(String annot) {
        if (annotations == EMPTY_SYMBOLTOKEN_LIST) {
            annotations = new ArrayList();
        }
        annotations.add(Hasher.newSymbolToken(annot));
        delegate.addTypeAnnotation(annot);
    }

    @Override
    public void setTypeAnnotations(String... annots) {
        if (annots == null || annots.length == 0) {
            annotations = EMPTY_SYMBOLTOKEN_LIST;
        } else {
            annotations = new ArrayList<>();
            for (String annot : annots) {
                annotations.add(Hasher.newSymbolToken(annot));
            }
        }
        delegate.setTypeAnnotations(annots);
    }

    @Override
    public void setTypeAnnotationSymbols(SymbolToken... annots) {
        annotations = Arrays.asList(annots);
        delegate.setTypeAnnotationSymbols(annots);
    }

    private SymbolToken[] annotations() {
        if (annotations == EMPTY_SYMBOLTOKEN_LIST) {
            return EMPTY_SYMBOLTOKEN_ARRAY;
        }
        SymbolToken[] array = new SymbolToken[annotations.size()];
        return annotations.toArray(array);
    }


    ///////// scalar value handling logic ///////////

    @Override
    public void writeBlob(byte[] value) throws IOException {
        updateScalar(() -> hasher.scalar().updateBlob(value));
        delegate.writeBlob(value);
    }

    @Override
    public void writeBlob(byte[] value, int start, int len) throws IOException {
        updateScalar(() -> hasher.scalar().updateBlob(value, start, len));
        delegate.writeBlob(value, start, len);
    }

    @Override
    public void writeBool(boolean value) throws IOException {
        updateScalar(() -> hasher.scalar().updateBool(value));
        delegate.writeBool(value);
    }

    @Override
    public void writeClob(byte[] value) throws IOException {
        updateScalar(() -> hasher.scalar().updateClob(value));
        delegate.writeClob(value);
    }

    @Override
    public void writeClob(byte[] value, int start, int len) throws IOException {
        updateScalar(() -> hasher.scalar().updateClob(value, start, len));
        delegate.writeClob(value, start, len);
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException {
        updateScalar(() -> hasher.scalar().updateDecimal(value));
        delegate.writeDecimal(value);
    }

    @Override
    public void writeFloat(double value) throws IOException {
        updateScalar(() -> hasher.scalar().updateFloat(value));
        delegate.writeFloat(value);
    }

    @Override
    public void writeInt(long value) throws IOException {
        writeInt(BigInteger.valueOf(value));
    }

    @Override
    public void writeInt(BigInteger value) throws IOException {
        updateScalar(() -> hasher.scalar().updateInt(value));
        delegate.writeInt(value);
    }

    @Override
    public void writeNull() throws IOException {
        updateScalar(() -> hasher.scalar().updateNull());
        delegate.writeNull();
    }

    @Override
    public void writeNull(IonType type) throws IOException {
        updateScalar(() -> hasher.scalar().updateNull(type));
        delegate.writeNull(type);
    }

    @Override
    public void writeString(String value) throws IOException {
        updateScalar(() -> hasher.scalar().updateString(value));
        delegate.writeString(value);
    }

    @Override
    public void writeSymbol(String content) throws IOException {
        updateScalar(() -> hasher.scalar().updateSymbol(content));
        delegate.writeSymbol(content);
    }

    @Override
    public void writeSymbolToken(SymbolToken content) throws IOException {
        updateScalar(() -> hasher.scalar().updateSymbolToken(content));
        delegate.writeSymbolToken(content);
    }

    @Override
    public void writeTimestamp(Timestamp value) throws IOException {
        updateScalar(() -> hasher.scalar().updateTimestamp(value));
        delegate.writeTimestamp(value);
    }

    private void updateScalar(Updatable scalarUpdater) throws IOException {
        hasher.scalar().withFieldName(fieldName);
        hasher.scalar().withAnnotations(annotations());
        hasher.scalar().prepare();
        scalarUpdater.update();

        this.fieldName = null;
        this.annotations = EMPTY_SYMBOLTOKEN_LIST;
    }

    ///////// /scalar value handling logic ///////////


    @Override
    public void writeValue(IonReader reader) throws IOException {
        writeValues(reader, false);
    }

    @Override
    public void writeValues(IonReader reader) throws IOException {
        writeValues(reader, true);
    }

    private void writeValues(IonReader reader, boolean iterate) throws IOException {
        IonType type = reader.getType();

        if (iterate && type == null) {
            reader.next();
            type = reader.getType();
        }

        if (type == null) {
            return;
        }

        do {
            setTypeAnnotationSymbols(reader.getTypeAnnotationSymbols());
            if (reader.isInStruct()) {
                setFieldNameSymbol(reader.getFieldNameSymbol());
            }

            if (reader.isNullValue()) {
                writeNull(type);
                continue;
            }

            if (IonType.isContainer(type)) {
                stepIn(type);
                reader.stepIn();
                writeValues(reader, true);
                reader.stepOut();
                stepOut();
            } else {
                switch (type) {
                    case BLOB:
                        writeBlob(reader.newBytes());
                        break;
                    case BOOL:
                        writeBool(reader.booleanValue());
                        break;
                    case CLOB:
                        writeClob(reader.newBytes());
                        break;
                    case DECIMAL:
                        writeDecimal(reader.decimalValue());
                        break;
                    case FLOAT:
                        writeFloat(reader.doubleValue());
                        break;
                    case INT:
                        writeInt(reader.bigIntegerValue());
                        break;
                    case STRING:
                        writeString(reader.stringValue());
                        break;
                    case SYMBOL:
                        writeSymbolToken(reader.symbolValue());
                        break;
                    case TIMESTAMP:
                        writeTimestamp(reader.timestampValue());
                        break;
                    default:
                        throw new RuntimeException("Unexpected type '" + type + "'");
                }
            }
        } while (iterate && (type = reader.next()) != null);
    }


    ///////// The remaining methods are all handled solely by the delegate ///////////

    @Override
    public void finish() throws IOException {
        delegate.finish();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public boolean isInStruct() {
        return delegate.isInStruct();
    }

    @Override
    public SymbolTable getSymbolTable() {
        return delegate.getSymbolTable();
    }
}
