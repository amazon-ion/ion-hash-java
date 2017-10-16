package software.amazon.ionhash;

import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class ByteArrayComparatorTest {
    private Comparator<byte[]> bac = new IonHashReaderImpl.ByteArrayComparator();

    @Test(expected = NullPointerException.class)
    public void testNPE1() {
        bac.compare(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testNPE2() {
        bac.compare(null, new byte[] {});
    }

    @Test(expected = NullPointerException.class)
    public void testNPE3() {
        bac.compare(new byte[] {}, null);
    }

    @Test
    public void testIdentity() {
        byte[] emptyByteArray = new byte[] {};
        assertEquals(0, bac.compare(emptyByteArray, emptyByteArray));

        byte[] bytes = new byte[] {0x01, 0x02, 0x03};
        assertEquals(0, bac.compare(bytes, bytes));
    }

    @Test
    public void testEquals() {
        assertEquals(0, bac.compare(new byte[] {0x01, 0x02, 0x03},
                                    new byte[] {0x01, 0x02, 0x03}));
    }

    @Test
    public void lessThan() {
        assertEquals(-1, bac.compare(new byte[] {0x01, 0x02, 0x03},
                                     new byte[] {0x01, 0x02, 0x04}));
    }

    @Test
    public void lessThanDueToLength() {
        assertEquals(-1, bac.compare(new byte[] {0x01, 0x02, 0x03},
                                     new byte[] {0x01, 0x02, 0x03, 0x04}));
    }

    @Test
    public void greaterThanDueToLength() {
        assertEquals(1, bac.compare(new byte[] {0x01, 0x02, 0x03, 0x04},
                                    new byte[] {0x01, 0x02, 0x03}));
    }

    @Test
    public void unsignedBehavior() {
        // verify signed bytes are being correctly handled as unsigned bytes
        assertEquals(-1, bac.compare(new byte[] {0x01}, new byte[] {(byte)0x7F}));
        assertEquals(-1, bac.compare(new byte[] {0x01}, new byte[] {(byte)0x80}));
        assertEquals(-1, bac.compare(new byte[] {0x01}, new byte[] {(byte)0xFF}));
        assertEquals(1, bac.compare(new byte[] {(byte)0x7F}, new byte[] {0x01}));
        assertEquals(1, bac.compare(new byte[] {(byte)0x80}, new byte[] {0x01}));
        assertEquals(1, bac.compare(new byte[] {(byte)0xFF}, new byte[] {0x01}));
    }
}
