package software.amazon.ionhash;

import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.system.IonSystemBuilder;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class IonHashReaderImplTest {
    private static IonSystem ION = IonSystemBuilder.standard().build();

    @Test
    public void testEmptyString() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader(""), TestIonHasherProviders.getInstance("identity"));

        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());
        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());
    }

    @Test
    public void testTopLevelValues() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("1 2 3"), TestIonHasherProviders.getInstance("identity"));

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x01}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x02}, ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x03}, ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x03}, ihr.digest());
    }

    @Test
    public void testHashRepresentsPreviousValue() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("[1,2,{a:3,b:4},5]"), TestIonHasherProviders.getInstance("identity"));

        assertArrayEquals(new byte[] {}, ihr.digest());

        assertEquals(IonType.LIST, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());

        ihr.stepIn();
        assertArrayEquals(new byte[] {}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x01}, ihr.digest());

        assertEquals(IonType.STRUCT, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x02}, ihr.digest());

        ihr.stepIn();
        assertArrayEquals(new byte[] {}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x03}, ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x04}, ihr.digest());

        assertEquals(null, ihr.next());   // redundant next(), no change
        assertArrayEquals(new byte[] {0x20, 0x04}, ihr.digest());

        ihr.stepOut();
        assertArrayEquals(new byte[] {(byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04}, ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(new byte[] {(byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04}, ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {0x20, 0x05}, ihr.digest());

        assertEquals(null, ihr.next());   // redundant next(), no change
        assertArrayEquals(new byte[] {0x20, 0x05}, ihr.digest());

        ihr.stepOut();
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());

        assertEquals(null, ihr.next());   // redundant next(), no change
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());
    }

    @Test
    public void testConsumeRemainder_singleNext() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("[1,2,{a:3,b:4},5]"), TestIonHasherProviders.getInstance("identity"));
        assertEquals(IonType.LIST, ihr.next());
        assertEquals(null, ihr.next());
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());
    }

    @Test
    public void testConsumeRemainder_partialConsume() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("[1,2,{a:3,b:4},5]"), TestIonHasherProviders.getInstance("identity"));
        ihr.next();
        ihr.stepIn();
        ihr.next();
        ihr.next();
        ihr.next();
        ihr.stepIn();
        ihr.next();
        ihr.stepOut();   // we've only partially consumed the struct
        ihr.stepOut();   // we've only partially consumed the list
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());
    }

    @Test
    public void testConsumeRemainder_stepInStepOutNested() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("[1,2,{a:3,b:4},5]"), TestIonHasherProviders.getInstance("identity"));
        ihr.next();
        ihr.stepIn();
        ihr.next();
        ihr.next();
        ihr.next();
        ihr.stepIn();
        ihr.stepOut();   // we haven't consumed ANY of the struct
        ihr.stepOut();   // we've only partially consumed the list
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());
    }

    @Test
    public void testConsumeRemainder_stepInNextStepOut() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("[1,2,{a:3,b:4},5]"), TestIonHasherProviders.getInstance("identity"));
        ihr.next();
        ihr.stepIn();
        ihr.next();
        ihr.stepOut();   // we've partially consumed the list
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());
    }

    @Test
    public void testConsumeRemainder_stepInStepOutTopLevel() {
        IonHashReader ihr = new IonHashReaderImpl(
                ION.newReader("[1,2,{a:3,b:4},5]"), TestIonHasherProviders.getInstance("identity"));
        ihr.next();
        ihr.stepIn();
        ihr.stepOut();   // we haven't consumed ANY of the list
        assertArrayEquals(
            new byte[] {(byte)0xb0, 0x20, 0x01, 0x20, 0x02, (byte)0xd0, 0x70, 0x61, 0x20, 0x03, 0x70, 0x62, 0x20, 0x04, 0x20, 0x05},
            ihr.digest());
    }

    /*
    // pending ion-java support for SID 0

    @Test
    public void testUnresolvedSid0() {
        // special SID 0 should NOT result in an exception
        testIonTextBytes("(0xd3 0x80 0x21 0x01)");
    }

    @Test(expected = IonHashException.class)
    public void testUnresolvedSid() {
        // unresolved SIDs (except SID 0) should result in an exception
        testIonTextBytes("(0xd3 0x8a 0x21 0x01)");
    }

    void testIonTextBytes(String ionTextBytes) {
        IonContainer container = (IonContainer) ION.singleValue("(0xd3 0x80 0x21 0x01)");
        byte[] ionBinary = IonHashRunner.containerToBytes(container);
        IonHashReader reader = new IonHashReaderImpl(
                ION.newReader(ionBinary), new IdentityIonHasher());
        traverse(reader);
    }

    private void traverse(IonReader reader) {
        IonType iType;
        while ((iType = reader.next()) != null) {
            if (!reader.isNullValue() && IonType.isContainer(iType)) {
                reader.stepIn();
                traverse(reader);
                reader.stepOut();
            }
            SymbolToken fieldNameSymbol = reader.getFieldNameSymbol();
            if (fieldNameSymbol != null) {
                System.out.println("fieldNameSymbol.sid: " + fieldNameSymbol.getSid());
            }
        }
    }
    */

    /**
     * Asserts that IonHashReaderImpl's handling of the IonReader contract matches that
     * of a non-hashing IonReader.  While ReaderCompare covers much of the IonReader API,
     * it does not cover all of it.
     */
    @Test
    public void testIonReaderContract() throws IOException {
        File file = new File(IonHashRunner.ION_HASH_TESTS_PATH);

        IonReader ir = ION.newReader(new FileReader(file));

        IonHashReader ihr = IonHashReaderBuilder.standard()
                .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                .withReader(ION.newReader(new FileReader(file)))
                .build();

        ReaderCompare.compare(ir, ihr);
    }
}
