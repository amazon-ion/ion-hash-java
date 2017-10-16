package software.amazon.ionhash;

import software.amazon.ion.IonWriter;
import software.amazon.ion.impl.bin.PrivateIonHashTrampoline;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolToken;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Provides the correct bytes for IonHash purposes;  relies on the binary
 * encoding implementation in IonRawBinaryWriter when possible.
 * <p/>
 * This class is not thread-safe.
 */
final class IonHashBytes implements Closeable {
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final IonWriter writer;
    private final ByteArrayOutputStream baos;

    IonHashBytes() {
        try {
            this.baos = new ByteArrayOutputStream();
            writer = PrivateIonHashTrampoline.newIonWriter(baos);
        } catch (IOException e) {
            throw new IonHashException(e);
        }
    }

    /**
     * For the given scalar type or null value, reads the value and returns
     * the canonical bytes to be used for hashing.
     * For container types, call containerBytes().
     *
     * byte[][]:
     *   TQ, representation
     */
    byte[][] scalarOrNullParts(IonType type, IonReader reader) {
        try {
            baos.reset();
            if (reader.isNullValue()) {
                writer.writeNull(type);
            } else {
                switch (type) {
                    case BOOL:
                        writer.writeBool(reader.booleanValue());
                        break;
                    case INT:
                        writer.writeInt(reader.bigIntegerValue());
                        break;
                    case FLOAT:
                        double d = reader.doubleValue();
                        if (new Double(d).equals(0.0)) {
                            // value is 0.0, not -0.0
                            return new byte[][] {EMPTY_BYTE_ARRAY, new byte[] {0x40}, EMPTY_BYTE_ARRAY};
                        }
                        writer.writeFloat(d);
                        break;
                    case DECIMAL:
                        writer.writeDecimal(reader.decimalValue());
                        break;
                    case TIMESTAMP:
                        writer.writeTimestamp(reader.timestampValue());
                        break;
                    case SYMBOL:
                        // handled below by call to symbolParts()
                        break;
                    case STRING:
                        writer.writeString(reader.stringValue());
                        break;
                    case CLOB:
                        writer.writeClob(reader.newBytes());
                        break;
                    case BLOB:
                        writer.writeBlob(reader.newBytes());
                        break;
                    default:
                        throw new IonHashException("Unsupported IonType (" + type + ")");
                }
            }
            writer.finish();

            if (!reader.isNullValue() && type == IonType.SYMBOL) {
                return symbolParts(reader.symbolValue());
            } else {
                return scalarOrNullSplitParts(type, baos.toByteArray());
            }
        } catch (IOException e) {
            throw new IonHashException(e);
        }
    }

    // split representation and TQ parts;  also handles any special case binary cleanup
    private byte[][] scalarOrNullSplitParts(IonType type, byte[] bytes) throws IOException {
        int tl = bytes[0];
        int offset = 1 + getLengthLength(bytes);

        if (type == IonType.INT && bytes.length > offset) {
            // ignore sign byte prepended by BigInteger.toByteArray() when the magnitude
            // ends at byte boundary (the 'intLength512' test is an example of this)
            if ((bytes[offset] & 0xFF) == 0) {
                offset++;
            }
        }

        // the representation is everything after TL and length
        byte[] representation = Arrays.copyOfRange(bytes, offset, bytes.length);
        byte tq = (tl & 0x0F) == 0x0F ? (byte)tl : (byte)(tl & 0xF0);

        if (type == IonType.BOOL) {
            tq = (byte)tl;
        }

        return new byte[][] { new byte[] {tq}, representation };
    }

    // TQ, representation
    byte[][] symbolParts(SymbolToken symbol) {
        String text = symbol == null ? null : symbol.getText();
        if (text == null && (symbol == null || symbol.getSid() != 0)) {
            throw new IonHashException("Unable to resolve SID "
                    + (symbol != null ? symbol.getSid() : "null"));
        }

        byte[] symbolBytes;
        try {
            byte tq = 0x70;
            if (text == null && symbol.getSid() == 0) {
                tq |= 0x01;
            }
            baos.reset();
            writer.writeString(text);
            writer.finish();
            symbolBytes = baos.toByteArray();

            int offset = 1 + getLengthLength(symbolBytes);
            byte[] representation = Arrays.copyOfRange(symbolBytes, offset, symbolBytes.length);

            return new byte[][] {new byte[] {tq}, representation};
        } catch (IOException e) {
            throw new IonHashException(e);
        }
    }

    // returns a count of bytes in the "length" field
    private int getLengthLength(byte[] arr) {
        if ((arr[0] & 0x0F) == 0x0E) {
            // read subsequent byte(s) as the "length" field
            for (int i = 1; i < arr.length; i++) {
                if ((arr[i] & 0x80) != 0) {
                    return i;
                }
            }
            throw new IllegalStateException("Problem while reading VarUInt!");
        }
        return 0;
    }

    /**
     * For the container type and count of elements in the container,
     * returns the TQ byte.  When type == null, assumed to be an annotated value.
     * For scalar types or null values, call scalarOrNullBytes().
     */
    byte containerTQ(IonType type) {
        if (type == null) {
            return (byte)0xE0;  // annotated value
        } else {
            switch (type) {
                case LIST:
                    return (byte)0xB0;
                case SEXP:
                    return (byte)0xC0;
                case STRUCT:
                    return (byte)0xD0;
                default:
                    throw new IonHashException("Unexpected container type " + type);
            }
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
        baos.close();
    }
}
