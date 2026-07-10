package exchange.ws;

import Price.Price;
import Price.PriceFactory;
import Exceptions.InvalidPriceOperation;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Fixed-layout binary order-entry frame for the WebSocket gateway.
 *
 * <p>This is intentionally small and explicit so the Netty ingress path can
 * bypass JSON/Gson object mapping. Numeric fields are primitive and encoded
 * big-endian by Netty's ByteBuf methods. Variable identifiers are length
 * prefixed ASCII slices.
 */
public final class BinaryOrderCodec {
    public static final int MAGIC = 0x56544F31; // "VTO1"
    public static final byte VERSION = 1;
    public static final byte NEW_ORDER = 1;

    private static final int FIXED_HEADER_BYTES =
            Integer.BYTES // magic
                    + 1 // version
                    + 1 // message type
                    + 1 // side
                    + 1 // order type
                    + 1 // stp mode
                    + Integer.BYTES // quantity
                    + Long.BYTES; // price cents

    private BinaryOrderCodec() {
    }

    public static BinaryNewOrder decodeNewOrder(ByteBuf buffer) {
        if (buffer.readableBytes() < FIXED_HEADER_BYTES + 3) {
            throw new IllegalArgumentException("Binary order frame is too short");
        }
        int magic = buffer.readInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid binary order magic");
        }
        byte version = buffer.readByte();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported binary order version: " + version);
        }
        byte messageType = buffer.readByte();
        if (messageType != NEW_ORDER) {
            throw new IllegalArgumentException("Unsupported binary order message type: " + messageType);
        }

        Side side = decodeSide(buffer.readByte());
        OrderType orderType = decodeOrderType(buffer.readByte());
        SelfTradePreventionMode stpMode = decodeStpMode(buffer.readByte());
        int quantity = buffer.readInt();
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        long priceCents = buffer.readLong();
        Price price = null;
        if (orderType != OrderType.MARKET) {
            try {
                price = PriceFactory.makePrice(priceCents);
            } catch (InvalidPriceOperation e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }

        String clientId = readAscii(buffer, "clientId");
        String symbol = readAscii(buffer, "symbol").toUpperCase(java.util.Locale.ROOT);
        String orderId = readAscii(buffer, "orderId");
        return new BinaryNewOrder(orderId, clientId, symbol, side, orderType, price, quantity, stpMode);
    }

    public static ByteBuffer encodeNewOrder(
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            long priceCents,
            int quantity,
            SelfTradePreventionMode stpMode) {
        byte[] orderIdBytes = ascii(orderId);
        byte[] clientIdBytes = ascii(clientId);
        byte[] symbolBytes = ascii(symbol);
        int size = FIXED_HEADER_BYTES
                + 1 + clientIdBytes.length
                + 1 + symbolBytes.length
                + 1 + orderIdBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(MAGIC);
        buffer.put(VERSION);
        buffer.put(NEW_ORDER);
        buffer.put(encodeSide(side));
        buffer.put(encodeOrderType(orderType));
        buffer.put(encodeStpMode(stpMode));
        buffer.putInt(quantity);
        buffer.putLong(priceCents);
        putAscii(buffer, clientIdBytes);
        putAscii(buffer, symbolBytes);
        putAscii(buffer, orderIdBytes);
        buffer.flip();
        return buffer;
    }

    private static String readAscii(ByteBuf buffer, String fieldName) {
        if (!buffer.isReadable()) {
            throw new IllegalArgumentException("Missing binary field: " + fieldName);
        }
        int length = buffer.readUnsignedByte();
        if (length == 0) {
            throw new IllegalArgumentException("Missing binary field: " + fieldName);
        }
        if (buffer.readableBytes() < length) {
            throw new IllegalArgumentException("Truncated binary field: " + fieldName);
        }
        String value = buffer.toString(buffer.readerIndex(), length, StandardCharsets.US_ASCII);
        buffer.skipBytes(length);
        return value.trim();
    }

    private static byte[] ascii(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Binary field cannot be blank");
        }
        byte[] bytes = value.trim().getBytes(StandardCharsets.US_ASCII);
        if (bytes.length > 255) {
            throw new IllegalArgumentException("Binary field exceeds 255 bytes");
        }
        return bytes;
    }

    private static void putAscii(ByteBuffer buffer, byte[] bytes) {
        buffer.put((byte) bytes.length);
        buffer.put(bytes);
    }

    private static byte encodeSide(Side side) {
        return switch (side) {
            case BUY -> 1;
            case SELL -> 2;
        };
    }

    private static Side decodeSide(byte side) {
        return switch (side) {
            case 1 -> Side.BUY;
            case 2 -> Side.SELL;
            default -> throw new IllegalArgumentException("Unsupported binary side: " + side);
        };
    }

    private static byte encodeOrderType(OrderType orderType) {
        return switch (orderType) {
            case LIMIT -> 1;
            case MARKET -> 2;
            case IOC -> 3;
            case FOK -> 4;
            case STOP -> 5;
            case STOP_LIMIT -> 6;
        };
    }

    private static OrderType decodeOrderType(byte orderType) {
        return switch (orderType) {
            case 1 -> OrderType.LIMIT;
            case 2 -> OrderType.MARKET;
            case 3 -> OrderType.IOC;
            case 4 -> OrderType.FOK;
            case 5 -> OrderType.STOP;
            case 6 -> OrderType.STOP_LIMIT;
            default -> throw new IllegalArgumentException("Unsupported binary order type: " + orderType);
        };
    }

    private static byte encodeStpMode(SelfTradePreventionMode stpMode) {
        return switch (stpMode) {
            case CANCEL_NEWEST -> 1;
            case CANCEL_OLDEST -> 2;
            case DECREMENT_LARGER -> 3;
        };
    }

    private static SelfTradePreventionMode decodeStpMode(byte stpMode) {
        return switch (stpMode) {
            case 1 -> SelfTradePreventionMode.CANCEL_NEWEST;
            case 2 -> SelfTradePreventionMode.CANCEL_OLDEST;
            case 3 -> SelfTradePreventionMode.DECREMENT_LARGER;
            default -> throw new IllegalArgumentException("Unsupported binary STP mode: " + stpMode);
        };
    }

    public record BinaryNewOrder(
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
    }
}
