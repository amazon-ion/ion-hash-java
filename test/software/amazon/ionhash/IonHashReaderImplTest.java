package software.amazon.ionhash;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.system.IonSystemBuilder;
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
        assertArrayEquals(TestUtil.sexpToBytes("(0x0b 0x20 0x01 0x0e)"), ihr.digest());

        assertEquals(IonType.INT, ihr.next());
        assertArrayEquals(TestUtil.sexpToBytes("(0x0b 0x20 0x02 0x0e)"), ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(TestUtil.sexpToBytes("(0x0b 0x20 0x03 0x0e)"), ihr.digest());

        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());
    }

    private void consume(TestHelper consumer) {
        IonHashReader ihr = new IonHashReaderImpl(ION.newReader("[1,2,{a:3,b:4},5]"),
                TestIonHasherProviders.getInstance("identity"));
        assertArrayEquals(new byte[] {}, ihr.digest());
        consumer.traverse(ihr);
        assertArrayEquals(TestUtil.sexpToBytes(
                  "(0x0b 0xb0"
                + "   0x0b 0x20 0x01 0x0e"
                + "   0x0b 0x20 0x02 0x0e"
                + "   0x0b 0xd0"
                + "     0x0c 0x0b 0x70 0x61 0x0c 0x0e 0x0c 0x0b 0x20 0x03 0x0c 0x0e"
                + "     0x0c 0x0b 0x70 0x62 0x0c 0x0e 0x0c 0x0b 0x20 0x04 0x0c 0x0e"
                + "   0x0e"
                + "   0x0b 0x20 0x05 0x0e"
                + " 0x0e)"),
                ihr.digest());
        assertEquals(null, ihr.next());
        assertArrayEquals(new byte[] {}, ihr.digest());
    }

    @Test
    public void testConsumeRemainder_partialConsume() {
        consume((ihr) -> {
            ihr.next();
            ihr.stepIn();
              ihr.next();
              ihr.next();
              ihr.next();
              ihr.stepIn();
                ihr.next();
              ihr.stepOut();  // we've only partially consumed the struct
            ihr.stepOut();    // we've only partially consumed the list
        });
    }

    @Test
    public void testConsumeRemainder_stepInStepOutNested() {
        consume((ihr) -> {
            ihr.next();
            ihr.stepIn();
              ihr.next();
              ihr.next();
              ihr.next();
              ihr.stepIn();
              ihr.stepOut();  // we haven't consumed ANY of the struct
            ihr.stepOut();    // we've only partially consumed the list
        });
    }

    @Test
    public void testConsumeRemainder_stepInNextStepOut() {
        consume((ihr) -> {
            ihr.next();
            ihr.stepIn();
              ihr.next();
            ihr.stepOut();   // we've partially consumed the list
        });
    }

    @Test
    public void testConsumeRemainder_stepInStepOutTopLevel() {
        consume((ihr) -> {
            ihr.next();
            assertArrayEquals(new byte[] {}, ihr.digest());

            ihr.stepIn();
              assertArrayEquals(new byte[] {}, ihr.digest());
            ihr.stepOut();   // we haven't consumed ANY of the list
        });
    }

    @Test
    public void testConsumeRemainder_singleNext() {
        consume((ihr) -> {
            ihr.next();
            ihr.next();
        });
    }

    interface TestHelper {
        void traverse(IonHashReader ihr);
    }

    @Test(expected = IonHashException.class)
    public void testUnresolvedSid() {
        // unresolved SIDs (such as SID 10 here) should result in an exception
        IonContainer container = (IonContainer)ION.singleValue("(0xd3 0x8a 0x21 0x01)");
        byte[] ionBinary = IonHashRunner.containerToBytes(container);
        IonHashReader reader = new IonHashReaderImpl(
                ION.newReader(ionBinary),
                TestIonHasherProviders.getInstance("identity"));
        reader.next();
        reader.next();
    }

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
