package software.amazon.ionhash;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test utility methods.
 */
class TestUtil {
    static void assertEquals(byte[] expected, byte[] actual) {
        assertArrayEquals(
                  "expected: " + bytesToHex(expected) + System.getProperty("line.separator")
                + "  actual: " + bytesToHex(actual),
                expected, actual);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString();
    }
}
