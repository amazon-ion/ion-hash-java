package software.amazon.ionhash;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import software.amazon.ionhash.TestIonHasherProviders.TestIonHasherProvider;
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

    // digest cache tests
    IonHashTestSuite.DigestCacheTest.class,
})
public class IonHashTestSuite {
    final static IonSystem ION = IonSystemBuilder.standard().build();

    abstract static class IonHashTester {
        TestIonHasherProvider hasherProvider;

        IonReader getIonReader(String ionText) {
            return ION.newReader(ionText);
        }

        IonReader getIonReader(byte[] ionBinary) {
            return ION.newReader(ionBinary);
        }

        void traverse(IonReader reader, TestIonHasherProvider hasherProvider) throws IOException {
            this.hasherProvider = hasherProvider;
            IonHashReader ihr = new IonHashReaderImpl(reader, hasherProvider);
            traverse(ihr);
            ihr.digest();
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

        IonSexp getHashLog() {
            return hasherProvider.getHashLog();
        }

        IonSexp filterExpectedHashLog(IonSexp expectedHashLog) {
            return expectedHashLog;
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
        void traverse(IonHashReader reader) {
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
        void traverse(IonReader reader, TestIonHasherProvider hasherProvider) throws IOException {
            this.hasherProvider = hasherProvider;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IonWriter writer = ION.newTextWriter(baos);
            IonHashWriter ihw = new IonHashWriterImpl(writer, hasherProvider);
            ihw.writeValues(reader);
            ihw.digest();
            ihw.close();
        }
    }

    // digest cache tests

    /**
     * verifies that the final digest is correct when the digest cache is enabled;
     * this is accomplished by converting the entries of the expectedHashLog into a
     * single, "final_digest" assertion
     */
    @RunWith(IonHashRunner.class)
    public static class DigestCacheTest extends WriterTest {
        @Override
        void traverse(IonReader reader, TestIonHasherProvider hasherProvider) throws IOException {
            System.setProperty("ion-hash-java.useDigestCache", "true");
            super.traverse(reader, hasherProvider);
        }

        @Override
        IonSexp filterExpectedHashLog(IonSexp expectedHashLog) {
            IonSexp finalDigestHashLog = ION.newSexp();
            IonSexp finalDigest = (IonSexp)expectedHashLog.get(expectedHashLog.size() - 1).clone();
            finalDigest.setTypeAnnotations("final_digest");
            finalDigestHashLog.add(finalDigest);

            return finalDigestHashLog;
        }
    }
}
