package software.amazon.ionhash;

import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    // IonHashReader tests
    IonHashTestSuite.BinaryTest.class,
    IonHashTestSuite.DomTest.class,
    IonHashTestSuite.TextTest.class,
    IonHashTestSuite.BinaryInputStreamTest.class,
    IonHashTestSuite.TextNoStepInTest.class,

    // IonHashWriter tests
    IonHashTestSuite.WriterTest.class,
})
public class IonHashTestSuite {
    final static IonSystem ION = IonSystemBuilder.standard().build();

    abstract static class IonHashTester {
        IonReader getIonReader(String ionText) {
            return ION.newReader(ionText);
        }

        IonReader getIonReader(byte[] ionBinary) {
            return ION.newReader(ionBinary);
        }

        void traverse(IonReader reader, IonHasherProvider hasherProvider) throws IOException {
            IonHashReader ihr = new IonHashReaderImpl(reader, hasherProvider);
            traverse(ihr);
            ihr.close();
        }

        void traverse(IonHashReader reader) {
            IonType iType;
            while ((iType = reader.next()) != null) {
                if (!reader.isNullValue() && IonType.isContainer(iType)) {
                    reader.stepIn();
                    traverse(reader);
                    reader.stepOut();
                }
            }
        }
    }

    /**
     * verifies behavior when using a reader over binary
     */
    @RunWith(IonHashRunner.class)
    public static class BinaryTest extends IonHashTester {
        @Override
        public IonReader getIonReader(String ionText) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                IonWriter writer = ION.newBinaryWriter(baos);
                ION.singleValue(ionText).writeTo(writer);
                writer.close();
                return ION.newReader(baos.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * verifies behavior when using a reader over a DOM
     */
    @RunWith(IonHashRunner.class)
    public static class DomTest extends IonHashTester {
        @Override
        public IonReader getIonReader(String ionText) {
            return ION.newReader(ION.singleValue(ionText));
        }
    }

    /**
     * verifies behavior when using a text reader
     */
    @RunWith(IonHashRunner.class)
    public static class TextTest extends IonHashTester {
    }

    /**
     * verifies behavior when caller next()s past everything
     */
    @RunWith(IonHashRunner.class)
    public static class TextNoStepInTest extends TextTest {
        @Override
        public void traverse(IonHashReader reader) {
            while (reader.next() != null) {
            }
        }
    }

    /**
     * verify behavior when using a reader over an InputStream
     */
    @RunWith(IonHashRunner.class)
    public static class BinaryInputStreamTest extends IonHashTester {
        @Override
        public IonReader getIonReader(String ionText) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                IonWriter writer = ION.newBinaryWriter(baos);
                ION.singleValue(ionText).writeTo(writer);
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return ION.newReader(new InputStream() {
                byte[] bytes = baos.toByteArray();
                private int i = 0;

                @Override
                public int read() throws IOException {
                    if (i < bytes.length) {
                        return bytes[i++] & 0xFF;
                    }
                    return -1;
                }
            });
        }
    }


    // IonHashWriter tests

    /**
     * verify behavior of an IonHashWriter
     */
    @RunWith(IonHashRunner.class)
    public static class WriterTest extends IonHashTester {
        @Override
        void traverse(IonReader reader, IonHasherProvider hasherProvider) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IonWriter writer = ION.newTextWriter(baos);
            IonHashWriter ihw = new IonHashWriterImpl(writer, hasherProvider);
            traverse(reader, ihw);
            ihw.close();
        }

        private void traverse(IonReader reader, IonHashWriter ihw) throws IOException {
            IonType type = null;
            while ((type = reader.next()) != null) {
                ihw.setTypeAnnotationSymbols(reader.getTypeAnnotationSymbols());
                if (reader.isInStruct()) {
                    ihw.setFieldNameSymbol(reader.getFieldNameSymbol());
                }

                if (reader.isNullValue()) {
                    ihw.writeNull(type);
                    continue;
                }

                if (IonType.isContainer(type)) {
                    ihw.stepIn(type);
                    reader.stepIn();
                    traverse(reader, ihw);
                    reader.stepOut();
                    ihw.stepOut();
                } else {
                    switch (type) {
                        case BLOB:
                            ihw.writeBlob(reader.newBytes());
                            break;
                        case BOOL:
                            ihw.writeBool(reader.booleanValue());
                            break;
                        case CLOB:
                            ihw.writeClob(reader.newBytes());
                            break;
                        case DECIMAL:
                            ihw.writeDecimal(reader.decimalValue());
                            break;
                        case FLOAT:
                            ihw.writeFloat(reader.doubleValue());
                            break;
                        case INT:
                            ihw.writeInt(reader.bigIntegerValue());
                            break;
                        case STRING:
                            ihw.writeString(reader.stringValue());
                            break;
                        case SYMBOL:
                            ihw.writeSymbolToken(reader.symbolValue());
                            break;
                        case TIMESTAMP:
                            ihw.writeTimestamp(reader.timestampValue());
                            break;
                        default:
                            throw new RuntimeException("Unexpected type '" + type + "'");
                    }
                }
            }
        }
    }
}
