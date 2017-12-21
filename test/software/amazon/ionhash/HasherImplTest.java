package software.amazon.ionhash;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HasherImplTest {
    @Test
    public void escape() {
        // null case
        assertNull(HasherImpl.escape(null));

        // happy cases
        byte[] empty = new byte[] {};
        assertEquals(empty, HasherImpl.escape(empty));

        byte[] bytes = new byte[] {0x00, 0x01, 0x02, 0x03};
        assertEquals(bytes, HasherImpl.escape(bytes));


        // escape cases
        assertArrayEquals(new byte[] {(byte)0xEF, (byte)0xEF},
            HasherImpl.escape(new byte[] {(byte)0xEF}));

        assertArrayEquals(new byte[] {(byte)0xEF, (byte)0xEF, (byte)0xEF, (byte)0xEF},
            HasherImpl.escape(new byte[] {(byte)0xEF, (byte)0xEF}));

        assertArrayEquals(new byte[] {(byte)0xEF, (byte)0xEF, 0x00, (byte)0xEF, (byte)0xEF, 0x01, (byte)0xEF, (byte)0xEF, 0x02, (byte)0xEF, (byte)0xEF},
                HasherImpl.escape(new byte[] {(byte)0xEF, 0x00, (byte)0xEF, 0x01, (byte)0xEF, 0x02, (byte)0xEF}));
    }
}
