package software.amazon.ionhash;

import org.junit.Test;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static software.amazon.ionhash.TestUtil.assertEquals;

public class IonReaderAndWriterTest {
    private static IonSystem ION = IonSystemBuilder.standard().build();
    private static IonHasherProvider hasherProvider = TestIonHasherProviders.getInstance("identity");

    @Test
    public void test_noFieldNameInCurrentHash() throws IOException {
        assertNoFieldnameInCurrentHash("null",                 new byte[] {      0x0f});
        assertNoFieldnameInCurrentHash("false",                new byte[] {      0x10});
        assertNoFieldnameInCurrentHash("5",                    new byte[] {      0x20, 0x05});
        assertNoFieldnameInCurrentHash("2e0",                  new byte[] {      0x40, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
        assertNoFieldnameInCurrentHash("1234.500",             new byte[] {      0x50, (byte)0xc3, 0x12, (byte)0xd6, 0x44});
        assertNoFieldnameInCurrentHash("2017-01-01T00:00:00Z", new byte[] {      0x60, (byte)0x80, 0x0f, (byte)0xe1, (byte)0x81, (byte)0x81, (byte)0x80, (byte)0x80, (byte)0x80});
        assertNoFieldnameInCurrentHash("hi",                   new byte[] {      0x70, 0x68, 0x69});
        assertNoFieldnameInCurrentHash("\"hi\"",               new byte[] {(byte)0x80, 0x68, 0x69});
        assertNoFieldnameInCurrentHash("{{\"hi\"}}",           new byte[] {(byte)0x90, 0x68, 0x69});
        assertNoFieldnameInCurrentHash("{{aGVsbG8=}}",         new byte[] {(byte)0xa0, 0x68, 0x65, 0x6c, 0x6c, 0x6f});
        assertNoFieldnameInCurrentHash("[1,2,3]",              new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, 0x20, 0x03});
        assertNoFieldnameInCurrentHash("(1 2 3)",              new byte[] {(byte)0xc0, 0x20, 0x01, 0x20, 0x02, 0x20, 0x03});
        assertNoFieldnameInCurrentHash("{a:1,b:2,c:3}",        new byte[] {(byte)0xd0, 0x70, 0x61, 0x20, 0x01, 0x70, 0x62, 0x20, 0x02, 0x70, 0x63, 0x20, 0x03});
        assertNoFieldnameInCurrentHash("hi::7",                new byte[] {(byte)0xe0, 0x70, 0x68, 0x69, 0x20, 0x07});
    }

    // verify that fieldname is not part of currentValue()
    private void assertNoFieldnameInCurrentHash(String val, byte[] expected) throws IOException {
        // verify IonHashWriter behavior:
        IonReader reader = ION.newReader(val);
        reader.next();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonWriter writer = ION.newBinaryWriter(baos);
        IonHashWriter ihw = IonHashWriterBuilder.standard()
                .withHasherProvider(hasherProvider)
                .withWriter(writer)
                .build();

        ihw.stepIn(IonType.STRUCT);
          ihw.setFieldName("field_name");
          ihw.writeValue(reader);
          byte[] actual = ihw.digest();
          assertEquals(expected, actual);
        ihw.stepOut();

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
