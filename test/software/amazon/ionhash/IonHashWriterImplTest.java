package software.amazon.ionhash;

import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonBinaryWriterBuilder;
import software.amazon.ion.system.IonSystemBuilder;
import org.junit.Test;
import software.amazon.ion.system.IonTextWriterBuilder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IonHashWriterImplTest {
    private static IonSystem ION = IonSystemBuilder.standard().build();

    @Test
    public void testMiscMethods() throws IOException {
        // coverage for digest(), isInStruct(), setFieldName(), addTypeAnnotation()

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonHashWriter ihw = new IonHashWriterImpl(
                IonTextWriterBuilder.standard().build(baos),
                TestIonHasherProviders.getInstance("identity"));

        assertArrayEquals(new byte[] {}, ihw.digest());

        ihw.writeNull();
        assertArrayEquals(TestUtil.sexpToBytes("(0x0b 0x0f 0x0e)"), ihw.digest());

        ihw.stepIn(IonType.LIST);
          assertArrayEquals(new byte[] {}, ihw.digest());
          ihw.writeInt(5);
          assertArrayEquals(new byte[] {}, ihw.digest());
        ihw.stepOut();
        assertArrayEquals(TestUtil.sexpToBytes("(0x0b 0xb0 0x0b 0x20 0x05 0x0e 0x0e)"), ihw.digest());

        ihw.writeNull();
        assertArrayEquals(TestUtil.sexpToBytes("(0x0b 0x0f 0x0e)"), ihw.digest());

        assertFalse(ihw.isInStruct());

        ihw.stepIn(IonType.STRUCT);
        assertTrue(ihw.isInStruct());

        ihw.setFieldName("hello");
        ihw.addTypeAnnotation("ion");
        ihw.addTypeAnnotation("hash");
        ihw.writeSymbol("world");

        ihw.stepOut();
        assertFalse(ihw.isInStruct());
        assertArrayEquals(TestUtil.sexpToBytes(
                  "(0x0b 0xd0"
                + "   0x0c 0x0b 0x70 0x68 0x65 0x6c 0x6c 0x6f 0x0c 0x0e"     // hello:
                + "   0x0c 0x0b 0xe0"
                + "     0x0c 0x0b 0x70 0x69 0x6f 0x6e 0x0c 0x0e"             // ion::
                + "     0x0c 0x0b 0x70 0x68 0x61 0x73 0x68 0x0c 0x0e"        // hash::
                + "     0x0c 0x0b 0x70 0x77 0x6f 0x72 0x6c 0x64 0x0c 0x0e"   // world
                + "   0x0c 0x0e"
                + " 0x0e)"),
                ihw.digest());

        ihw.finish();
        assertEquals("null [5] null {hello:ion::hash::world}",
                new String(baos.toByteArray()));
    }

    @Test(expected = IllegalStateException.class)
    public void testExtraStepOut() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonHashWriter ihw = new IonHashWriterImpl(
                IonTextWriterBuilder.standard().build(baos),
                TestIonHasherProviders.getInstance("identity"));
        ihw.stepOut();
    }

    @Test
    public void testIonWriterContract_writeValue() throws IOException {
        File file = new File(IonHashRunner.ION_HASH_TESTS_PATH);
        byte[] expected = exerciseWriter(ION.newReader(new FileReader(file)), false, (r, w) -> { r.next(); w.writeValue(r); });
        byte[] actual   = exerciseWriter(ION.newReader(new FileReader(file)), true,  (r, w) -> { r.next(); w.writeValue(r); });
        assertTrue(expected.length > 10);
        assertTrue(actual.length   > 10);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testIonWriterContract_writeValues() throws IOException {
        File file = new File(IonHashRunner.ION_HASH_TESTS_PATH);
        byte[] expected = exerciseWriter(ION.newReader(new FileReader(file)), false, (r, w) -> w.writeValues(r));
        byte[] actual   = exerciseWriter(ION.newReader(new FileReader(file)), true,  (r, w) -> w.writeValues(r));
        assertTrue(expected.length > 1000);
        assertTrue(actual.length   > 1000);
        assertArrayEquals(expected, actual);
    }

    private byte[] exerciseWriter(IonReader reader, boolean useHashWriter, TestHelper helper) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        IonWriter writer = IonBinaryWriterBuilder.standard().build(baos);
        if (useHashWriter) {
            writer = IonHashWriterBuilder.standard()
                    .withWriter(writer)
                    .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                    .build();
        }
        helper.help(reader, writer);
        writer.finish();

        return baos.toByteArray();
    }

    interface TestHelper {
        void help(IonReader reader, IonWriter writer) throws IOException;
    }
}
