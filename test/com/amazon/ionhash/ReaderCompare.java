/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.ionhash;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import org.junit.Assert;

import java.math.BigDecimal;

class ReaderCompare extends Assert {
    static void compare(IonReader it1, IonReader it2) {
        while (hasNext(it1, it2)) {
            IonType t1 = it1.getType();
            IonType t2 = it2.getType();

            if ((t1 != t2) && (t1 == null || t2 == null || !t1.equals(t2))) {
                assertEquals("ion type", t1, t2);
            }
            if (t1 == null) break;

            if (it1.isInStruct()) {
                compareFieldNames(it1, it2);
            }
            compareAnnotations(it1, it2);

            boolean isNull = it1.isNullValue();
            assertEquals(isNull, it2.isNullValue());

            switch (t1) {
                case NULL:
                    assertTrue(it1.isNullValue());
                    assertTrue(it2.isNullValue());
                    break;
                case BOOL:
                case INT:
                case FLOAT:
                case DECIMAL:
                case TIMESTAMP:
                case STRING:
                case SYMBOL:
                case BLOB:
                case CLOB:
                    compareScalars(t1, isNull, it1, it2);
                    break;
                case STRUCT:
                case LIST:
                case SEXP:
                    it1.stepIn();
                    it2.stepIn();
                    compare(it1, it2);
                    it1.stepOut();
                    it2.stepOut();
                    break;

                default:
                    throw new IllegalStateException();
            }
        }
        assertFalse(hasNext(it1, it2));
    }

    private static boolean hasNext(IonReader it1, IonReader it2) {
        boolean more = (it1.next() != null);
        assertEquals("next results don't match", more, it2.next() != null);

        if (!more) {
            assertEquals(null, it1.next());
            assertEquals(null, it2.next());
        }

        return more;
    }

    private static void compareNonNullStrings(String what, String s1, String s2) {
        assertNotNull(what, s1);
        assertNotNull(what, s2);
        assertEquals(what, s1, s2);
    }

    private static void compareFieldNames(IonReader r1, IonReader r2) {
        SymbolToken tok1 = r1.getFieldNameSymbol();
        SymbolToken tok2 = r1.getFieldNameSymbol();
        String fn = tok1.getText();
        assertEquals(fn, tok2.getText());

        if (fn != null) {
            String f1 = r1.getFieldName();
            String f2 = r2.getFieldName();
            compareNonNullStrings("field name", fn, f1);
            compareNonNullStrings("field name", fn, f2);
        }
    }

    private static void compareAnnotations(IonReader it1, IonReader it2) {
        SymbolToken[] syms1 = it1.getTypeAnnotationSymbols();
        SymbolToken[] syms2 = it2.getTypeAnnotationSymbols();

        assertSymbolEquals("annotation", syms1, syms2);
    }

    private static void compareScalars(IonType t, boolean isNull,
                                      IonReader it1, IonReader it2) {
        switch (t) {
            case BOOL:
                if (!isNull) {
                    assertEquals(it1.booleanValue(), it2.booleanValue());
                }
                break;
            case INT:
                assertEquals(it1.bigIntegerValue(), it2.bigIntegerValue());
                break;
            case FLOAT: {
                if (! isNull) {
                    double v1 = it1.doubleValue();
                    double v2 = it2.doubleValue();
                    assertEquals(v1, v2, 0);
                    // The last param is a delta, and we want exact match.
                }
                break;
            }
            case DECIMAL: {
                BigDecimal dec1 = it1.bigDecimalValue();
                BigDecimal dec2 = it2.bigDecimalValue();
                assertPreciselyEquals(dec1, dec2);
                dec1 = it1.decimalValue();
                dec2 = it2.decimalValue();
                assertPreciselyEquals(dec1, dec2);
                // TODO also test double, long, int etc.
                break;
            }
            case TIMESTAMP: {
                Timestamp t1 = it1.timestampValue();
                Timestamp t2 = it2.timestampValue();
                assertEquals(t1, t2);
                break;
            }
            case STRING: {
                String s1 = it1.stringValue();
                String s2 = it2.stringValue();
                assertEquals(s1, s2);
                break;
            }
            case SYMBOL: {
                SymbolToken tok1 = it1.symbolValue();
                SymbolToken tok2 = it2.symbolValue();
                if (isNull) {
                    assertNull(tok1);
                    assertNull(tok2);
                } else if (tok1.getText() == null || tok2.getText() == null) {
                    assertEquals("sids", tok1.getSid(), tok2.getSid());
                } else {
                    String s1 = tok1.getText();
                    String s2 = tok2.getText();
                    assertEquals(s1, s2);
                }
                break;
            }
            case BLOB:
            case CLOB: {
                if (!isNull) {
                    byte[] b1 = it1.newBytes();
                    byte[] b2 = it2.newBytes();
                    assert b1 != null && b2 != null;
                    assert b1.length == b2.length;
                    for (int ii=0; ii<b1.length; ii++) {
                        byte v1 = b1[ii];
                        byte v2 = b2[ii];
                        assertEquals(v1, v2);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("iterated to a type that's not expected");
        }
    }


    /******** from IonAssert ********/

    private static void assertSymbolEquals(String path,
                                           SymbolToken[] expecteds,
                                           SymbolToken[] actuals) {

        assertEquals(path + " count", expecteds.length, actuals.length);

        for (int i = 0; i < expecteds.length; i++) {
            assertSymbolEquals(path + "[" + i + "]",
                    expecteds[i], actuals[i]);
        }
    }

    private static void assertSymbolEquals(String path,
                                           SymbolToken expected,
                                           SymbolToken actual) {

        String expectedText = expected.getText();
        String actualText   = actual.getText();
        assertEquals(path + " text", expectedText, actualText);

        if (expectedText == null) {
            assertEquals(path + " sid", expected.getSid(), actual.getSid());
        }
    }

    /**
     * Checks decimal equality, including precision and negative-zero.
     *
     * @param expected may be null
     * @param actual may be null
     */
    private static void assertPreciselyEquals(BigDecimal expected, BigDecimal actual) {
        assertEquals(expected, actual);
        if (expected != null) {
            assertEquals("value",
                    expected.unscaledValue(), actual.unscaledValue());
            assertEquals("scale",
                    expected.scale(), actual.scale());
            assertEquals("isNegativeZero",
                    Decimal.isNegativeZero(expected),
                    Decimal.isNegativeZero(actual));
        }
    }
}
