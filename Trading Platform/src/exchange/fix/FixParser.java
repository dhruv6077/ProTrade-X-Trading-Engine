package exchange.fix;

import io.netty.buffer.ByteBuf;

public final class FixParser {
    
    private FixParser() {}

    public static int parseAsciiInt(ByteBuf buf, int index, int length) {
        int result = 0;
        for (int i = 0; i < length; i++) {
            byte b = buf.getByte(index + i);
            if (b >= '0' && b <= '9') {
                result = (result * 10) + (b - '0');
            } else if (b != '-') {
                throw new NumberFormatException("Invalid ASCII int");
            }
        }
        if (length > 0 && buf.getByte(index) == '-') {
            result = -result;
        }
        return result;
    }

    public static long parseAsciiPriceCents(ByteBuf buf, int index, int length) {
        long result = 0;
        int decimalPos = -1;
        for (int i = 0; i < length; i++) {
            byte b = buf.getByte(index + i);
            if (b >= '0' && b <= '9') {
                result = (result * 10) + (b - '0');
            } else if (b == '.') {
                decimalPos = i;
            } else {
                throw new NumberFormatException("Invalid ASCII price");
            }
        }
        
        if (decimalPos != -1) {
            int fractionDigits = length - 1 - decimalPos;
            if (fractionDigits == 0) {
                result *= 100;
            } else if (fractionDigits == 1) {
                result *= 10;
            } else if (fractionDigits == 2) {
                // already cents
            } else {
                // strip extra precision
                for (int i = 0; i < fractionDigits - 2; i++) {
                    result /= 10;
                }
            }
        } else {
            result *= 100;
        }
        return result;
    }

    public static String parseAsciiString(ByteBuf buf, int index, int length) {
        if (length <= 0) return "";
        byte[] bytes = new byte[length];
        buf.getBytes(index, bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.US_ASCII);
    }
}
