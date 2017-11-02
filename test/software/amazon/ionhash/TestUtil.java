package software.amazon.ionhash;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test utility methods.
 */
class TestUtil {
    static void assertEquals(byte[] expected, byte[] actual) {
        try {
            assertArrayEquals(expected, actual);
        } catch (AssertionError e) {
            System.out.println("expected: " + bytesToHex(expected));
            System.out.println("  actual: " + bytesToHex(actual));
            throw e;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString();
    }
}
