package software.amazon.ionhash;

import org.junit.Test;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static software.amazon.ionhash.TestUtil.assertEquals;

public class IonReaderAndWriterTest {
    private static IonSystem ION = IonSystemBuilder.standard().build();
    private static IonHasherProvider hasherProvider = TestIonHasherProviders.getInstance("identity");

    @Test
    public void test_noFieldNameInCurrentHash() throws IOException {
        assertNoFieldnameInCurrentHash("null",                 "(0x0b 0x0f 0x0e)");
        assertNoFieldnameInCurrentHash("false",                "(0x0b 0x10 0x0e)");
        assertNoFieldnameInCurrentHash("5",                    "(0x0b 0x20 0x05 0x0e)");
        assertNoFieldnameInCurrentHash("2e0",                  "(0x0b 0x40 0x40 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x0e)");
        assertNoFieldnameInCurrentHash("1234.500",             "(0x0b 0x50 0xc3 0x12 0xd6 0x44 0x0e)");
        assertNoFieldnameInCurrentHash("2017-01-01T00:00:00Z", "(0x0b 0x60 0x80 0x0f 0xe1 0x81 0x81 0x80 0x80 0x80 0x0e)");
        assertNoFieldnameInCurrentHash("hi",                   "(0x0b 0x70 0x68 0x69 0x0e)");
        assertNoFieldnameInCurrentHash("\"hi\"",               "(0x0b 0x80 0x68 0x69 0x0e)");
        assertNoFieldnameInCurrentHash("{{\"hi\"}}",           "(0x0b 0x90 0x68 0x69 0x0e)");
        assertNoFieldnameInCurrentHash("{{aGVsbG8=}}",         "(0x0b 0xa0 0x68 0x65 0x6c 0x6c 0x6f 0x0e)");
        assertNoFieldnameInCurrentHash("[1,2,3]",              "(0x0b 0xb0 0x0b 0x20 0x01 0x0e 0x0b 0x20 0x02 0x0e 0x0b 0x20 0x03 0x0e 0x0e)");
        assertNoFieldnameInCurrentHash("(1 2 3)",              "(0x0b 0xc0 0x0b 0x20 0x01 0x0e 0x0b 0x20 0x02 0x0e 0x0b 0x20 0x03 0x0e 0x0e)");
        assertNoFieldnameInCurrentHash("{a:1,b:2,c:3}",
                "(0x0b 0xd0"
              + "   0x0c 0x0b 0x0c 0x0b 0x70 0x61 0x0c 0x0e 0x0c 0x0b 0x20 0x01 0x0c 0x0e 0x0c 0x0e"
              + "   0x0c 0x0b 0x0c 0x0b 0x70 0x62 0x0c 0x0e 0x0c 0x0b 0x20 0x02 0x0c 0x0e 0x0c 0x0e"
              + "   0x0c 0x0b 0x0c 0x0b 0x70 0x63 0x0c 0x0e 0x0c 0x0b 0x20 0x03 0x0c 0x0e 0x0c 0x0e"
              + " 0x0e)");
        assertNoFieldnameInCurrentHash("hi::7",                "(0x0b 0xe0 0x0b 0x70 0x68 0x69 0x0e 0x0b 0x20 0x07 0x0e 0x0e)");
    }

    // verify that fieldname is not part of currentValue()
    private void assertNoFieldnameInCurrentHash(String val, String expectedSexpBytes) throws IOException {
        byte[] expected = TestUtil.sexpToBytes(expectedSexpBytes);

        // verify IonHashWriter behavior:
        IonReader reader = ION.newReader(val);
        reader.next();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonWriter writer = ION.newBinaryWriter(baos);
        writer.stepIn(IonType.STRUCT);
          IonHashWriter ihw = IonHashWriterBuilder.standard()
                  .withHasherProvider(hasherProvider)
                  .withWriter(writer)
                  .build();
          ihw.setFieldName("field_name");
          ihw.writeValue(reader);
          byte[] actual = ihw.digest();
          assertEquals(expected, actual);
        writer.stepOut();

        ihw.close();
        writer.close();
        baos.flush();
        byte[] bytes = baos.toByteArray();
        baos.close();
        reader.close();


        // verify IonHashReader behavior:
        reader = ION.newReader(bytes);
        reader.next();
        reader.stepIn();
          IonHashReader ihr = IonHashReaderBuilder.standard()
                  .withHasherProvider(hasherProvider)
                  .withReader(reader)
                  .build();
          ihr.next();
          ihr.next();
          actual = ihr.digest();
          assertEquals(expected, actual);
        ihr.close();
        reader.close();

        // and we've transitively asserted that currentValue of reader and writer match
    }

    @Test
    public void regression_fieldNameAsymmetry() throws IOException {
        // regression:  reader.digest() incorrectly incorporated the fieldName of a value in a struct,
        // such that if an IonHashWriter never saw the fieldName, hashes would not match
        //
        // addressed by updating reader/writer digest() behavior to not incorporate the fieldName;
        // note that upon stepping out of a struct, digest() MUST incorporate fieldNames from the fields
        //
        // I believe this test is redundant with test_noFieldNameInCurrentHash;  retaining it
        // to ensure we don't regress to customer-observed asymmetry.  --pcornell@, 2017-11-01

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        IonWriter writer = ION.newBinaryWriter(baos);
        IonHashWriter ihw = IonHashWriterBuilder.standard()
                .withHasherProvider(hasherProvider)
                .withWriter(writer)
                .build();

        // write nested struct:  {a:{b:1}}
        writer.stepIn(IonType.STRUCT);
          writer.setFieldName("a");       // ihw doesn't know about this fieldName
          ihw.stepIn(IonType.STRUCT);
            ihw.setFieldName("b");
            ihw.writeInt(1);
          ihw.stepOut();
          byte[] writeHash = ihw.digest();
          ihw.close();
        writer.stepOut();
        writer.close();

        IonValue ionValue = ION.singleValue(baos.toByteArray());

        IonReader reader = ION.newReader(((IonStruct)ionValue).get("a"));
        IonHashReader ihr = IonHashReaderBuilder.standard()
                .withReader(reader)
                .withHasherProvider(hasherProvider)
                .build();
        ihr.next();
        ihr.next();
        byte[] readHash = ihr.digest();
        ihr.close();
        reader.close();

        assertEquals(writeHash, readHash);
    }
}
