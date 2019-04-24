package software.amazon.ionhash;

import org.junit.Assert;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;

/**
 * Test utility methods.
 */
class TestUtil {
    private static final IonSystem ION = IonSystemBuilder.standard().build();

    static void assertEquals(byte[] expected, byte[] actual) {
        assertEquals(null, expected, actual);
    }

    static void assertEquals(String message, byte[] expected, byte[] actual) {
        Assert.assertEquals(message, bytesToHex(expected), bytesToHex(actual));
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString();
    }

    static byte[] sexpToBytes(String sexpString) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonSexp sexp = (IonSexp)ION.singleValue(sexpString);
        Iterator<IonValue> iter = sexp.iterator();
        while (iter.hasNext()) {
            IonInt i = (IonInt)iter.next();
            baos.write(i.intValue());
        }
        return baos.toByteArray();
    }
}
