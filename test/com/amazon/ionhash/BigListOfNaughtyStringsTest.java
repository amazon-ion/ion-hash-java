package com.amazon.ionhash;

import org.junit.Test;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;

public class BigListOfNaughtyStringsTest {
    private static final IonSystem ION = IonSystemBuilder.standard().build();

    /**
     * Asserts behavior when input is a "naughty string".
     * Input data is from https://github.com/minimaxir/big-list-of-naughty-strings
     */
    @Test
    public void test() throws IOException {
        try (
            BufferedReader reader = new BufferedReader(
                    new FileReader(IonHashRunner.TESTDATA_PATH + "/big-list-of-naughty-strings.txt"));
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") || line.isEmpty()) {
                    continue;
                }

                TestValue tv = new TestValue(line);

                test(tv, tv.asSymbol());
                test(tv, tv.asString());
                test(tv, tv.asLongString());
                test(tv, tv.asClob());
                test(tv, tv.asBlob());

                test(tv, tv.asSymbol() + "::" + tv.asSymbol());
                test(tv, tv.asSymbol() + "::" + tv.asString());
                test(tv, tv.asSymbol() + "::" + tv.asLongString());
                test(tv, tv.asSymbol() + "::" + tv.asClob());
                test(tv, tv.asSymbol() + "::" + tv.asBlob());

                test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asSymbol() + "}");
                test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asString() + "}");
                test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asLongString() + "}");
                test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asClob() + "}");
                test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asBlob() + "}");

                if (tv.isValidIon()) {
                    test(tv, tv.asIon());
                    test(tv, tv.asSymbol() + "::" + tv.asIon());
                    test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asIon() + "}");
                    test(tv, tv.asSymbol() + "::{" + tv.asSymbol() + ":" + tv.asSymbol() + "::" + tv.asIon() + "}");
                }

                // list
                test(tv, tv.asSymbol() + "::["
                        + tv.asSymbol() + ", "
                        + tv.asString() + ", "
                        + tv.asLongString() + ", "
                        + tv.asClob() + ", "
                        + tv.asBlob() + ", "
                        + (tv.isValidIon() ? tv.asIon() : "")
                        + "]");

                // sexp
                test(tv, tv.asSymbol() + "::("
                        + tv.asSymbol() + " "
                        + tv.asString() + " "
                        + tv.asLongString() + " "
                        + tv.asClob() + " "
                        + tv.asBlob() + " "
                        + (tv.isValidIon() ? tv.asIon() : "")
                        + ")");

                // multiple annotations
                test(tv, tv.asSymbol() + "::" + tv.asSymbol() + "::" + tv.asSymbol() + "::" + tv.asString());
            }
        }
    }

    private void test(TestValue tv, String s) throws IOException {
        IonHashWriter hashWriter = null;
        try {
            hashWriter = IonHashWriterBuilder.standard()
                    .withWriter(ION.newTextWriter(new ByteArrayOutputStream()))
                    .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                    .build();
            hashWriter.writeValues(ION.newReader(s));
        } catch (IonException e) {
            if (tv.isValidIon()) {
                throw e;
            }
        }

        IonHashReader hashReader = null;
        try {
            hashReader = IonHashReaderBuilder.standard()
                    .withReader(ION.newReader(s))
                    .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                    .build();
            hashReader.next();
            hashReader.next();
        } catch (IonException e) {
            if (tv.isValidIon()) {
                throw e;
            }
        }

        if (tv.validIon == null || tv.validIon == Boolean.TRUE) {
            TestUtil.assertEquals("Reader/writer hashes for line |" + tv.asIon() + "| as |" + s + "| don't match",
                    hashWriter.digest(),
                    hashReader.digest());
        }
    }

    private static class TestValue {
        private static final String ION = "ion::";
        private static final String INVALID_ION = "invalid_ion::";

        private String ion;
        private Boolean validIon = null;

        private TestValue(String ion) {
            this.ion = ion;

            if (this.ion.startsWith(ION)) {
                validIon = Boolean.TRUE;
                this.ion = this.ion.substring(ION.length());
            }
            if (this.ion.startsWith(INVALID_ION)) {
                validIon = Boolean.FALSE;
                this.ion = this.ion.substring(INVALID_ION.length());
            }
        }

        private String asIon() {
            return ion;
        }

        private String asSymbol() {
            String s = ion;
            s = s.replace("\\", "\\\\");
            s = s.replace("'", "\\'");
            s = "\'" + s + "\'";
            return s;
        }

        private String asString() {
            String s = ion;
            s = s.replace("\\", "\\\\");
            s = s.replace("\"", "\\\"");
            s = "\"" + s + "\"";
            return s;
        }

        private String asLongString() {
            String s = ion;
            s = s.replace("\\", "\\\\");
            s = s.replace("'", "\\'");
            return "'''" + s + "'''";
        }

        private String asClob() {
            String s = asString();
            StringBuffer sb = new StringBuffer();
            for (byte b : s.getBytes()) {
                int c = b & 0xFF;
                if (c >= 128) {
                    sb.append("\\x").append(Integer.toHexString(c));
                } else {
                    sb.append((char)c);
                }
            }
            return "{{" + sb.toString() + "}}";
        }

        private String asBlob() {
            return "{{" + new String(Base64.getEncoder().encode(ion.getBytes())) + "}}";
        }

        private boolean isValidIon() {
            return validIon != null && validIon.booleanValue();
        }
    }
}
