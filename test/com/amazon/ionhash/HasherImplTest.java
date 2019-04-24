package com.amazon.ionhash;

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

        byte[] bytes = new byte[] {0x10, 0x11, 0x12, 0x13};
        assertEquals(bytes, HasherImpl.escape(bytes));


        // escape cases
        assertArrayEquals(new byte[] {0x0C, 0x0B}, HasherImpl.escape(new byte[] {0x0B}));
        assertArrayEquals(new byte[] {0x0C, 0x0E}, HasherImpl.escape(new byte[] {0x0E}));
        assertArrayEquals(new byte[] {0x0C, 0x0C}, HasherImpl.escape(new byte[] {0x0C}));

        assertArrayEquals(    new byte[] {0x0C, 0x0B, 0x0C, 0x0E, 0x0C, 0x0C},
            HasherImpl.escape(new byte[] {      0x0B,       0x0E,       0x0C}));

        assertArrayEquals(    new byte[] {0x0C, 0x0C, 0x0C, 0x0C},
            HasherImpl.escape(new byte[] {      0x0C,       0x0C}));

        assertArrayEquals(    new byte[] {0x0C, 0x0C, 0x10, 0x0C, 0x0C, 0x11, 0x0C, 0x0C, 0x12, 0x0C, 0x0C},
            HasherImpl.escape(new byte[] {      0x0C, 0x10,       0x0C, 0x11,       0x0C, 0x12,       0x0C}));
    }
}
