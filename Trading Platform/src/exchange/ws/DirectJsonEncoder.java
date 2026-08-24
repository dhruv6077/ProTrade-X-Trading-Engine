package exchange.ws;

import Price.Price;
import exchange.dispatch.RingBufferEvent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public final class DirectJsonEncoder {
    
    private static final byte[] TYPE_EXECUTION_REPORT = "{\"type\":\"execution-report\",\"status\":".getBytes(StandardCharsets.US_ASCII);
    
    private static final byte[] STATUS_ACCEPTED = "\"accepted\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] STATUS_REJECTED = "\"rejected\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] STATUS_EXECUTED = "\"executed\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] STATUS_CANCELLED = "\"cancelled\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] STATUS_RESTATED = "\"restated\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] STATUS_ADMIN = "\"admin\"".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] SEQ_NUM = ",\"sequenceNumber\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ORDER_ID = ",\"orderId\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CLIENT_ID = ",\"clientId\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SYMBOL = ",\"symbol\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] EVENT_TIMESTAMP = ",\"eventTimestamp\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] DETAILS = ",\"details\":{".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] DETAILS_END = "}}".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NULL_VAL = "null".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] SIDE = "\"side\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ORDER_TYPE = ",\"orderType\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PRICE = ",\"price\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] QUANTITY = ",\"quantity\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] LEAVES_QTY = ",\"leavesQty\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CUM_QTY = ",\"cumQty\":".getBytes(StandardCharsets.US_ASCII);
    
    private static final byte[] REASON = "\"reason\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MESSAGE = ",\"message\":".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] CONTRA_ORDER_ID = ",\"contraOrderId\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FILL_PRICE = ",\"fillPrice\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FILL_QTY = ",\"fillQty\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FULL_FILL = ",\"fullFill\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] LATENCY_NANOS = ",\"latencyNanos\":".getBytes(StandardCharsets.US_ASCII);
    
    private static final byte[] TRUE_VAL = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE_VAL = "false".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] CANCELLED_QTY = "\"cancelledQty\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CANCELLED_REASON = ",\"reason\":".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] RESTATED_PRICE = "\"price\":".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] OPERATION = "\"operation\":".getBytes(StandardCharsets.US_ASCII);
    
    private static final ThreadLocal<byte[]> NUM_BUFFER = ThreadLocal.withInitial(() -> new byte[24]);

    public static ByteBuf encode(Channel channel, RingBufferEvent event) {
        ByteBuf buf = channel.alloc().buffer(256);
        buf.writeBytes(TYPE_EXECUTION_REPORT);
        
        switch (event.getEventType()) {
            case ACCEPTED -> buf.writeBytes(STATUS_ACCEPTED);
            case REJECTED -> buf.writeBytes(STATUS_REJECTED);
            case EXECUTED -> buf.writeBytes(STATUS_EXECUTED);
            case CANCELLED -> buf.writeBytes(STATUS_CANCELLED);
            case RESTATED -> buf.writeBytes(STATUS_RESTATED);
            case ADMIN -> buf.writeBytes(STATUS_ADMIN);
        }
        
        buf.writeBytes(SEQ_NUM);
        writeLong(buf, event.getSequenceNumber());
        
        buf.writeBytes(ORDER_ID);
        writeAsciiString(buf, event.getOrderId(), true);
        
        buf.writeBytes(CLIENT_ID);
        writeAsciiString(buf, event.getClientId(), true);
        
        buf.writeBytes(SYMBOL);
        writeAsciiString(buf, event.getSymbol(), true);
        
        buf.writeBytes(EVENT_TIMESTAMP);
        writeInstant(buf, event.getEventTimestamp());
        
        buf.writeBytes(DETAILS);
        appendDetails(buf, event);
        buf.writeBytes(DETAILS_END);
        
        return buf;
    }

    private static void appendDetails(ByteBuf buf, RingBufferEvent event) {
        switch (event.getEventType()) {
            case ACCEPTED -> {
                buf.writeBytes(SIDE);
                writeAsciiString(buf, event.getSide() == null ? null : event.getSide().name(), true);
                buf.writeBytes(ORDER_TYPE);
                writeAsciiString(buf, event.getOrderType() == null ? null : event.getOrderType().name(), true);
                buf.writeBytes(PRICE);
                writePrice(buf, event.getPrice());
                buf.writeBytes(QUANTITY);
                writeLong(buf, event.getQuantity());
                buf.writeBytes(LEAVES_QTY);
                writeLong(buf, event.getLeavesQty());
                buf.writeBytes(CUM_QTY);
                writeLong(buf, event.getCumQty());
            }
            case REJECTED -> {
                buf.writeBytes(REASON);
                writeAsciiString(buf, event.getRejectReason() == null ? null : event.getRejectReason().name(), true);
                buf.writeBytes(MESSAGE);
                writeAsciiString(buf, event.getMessage(), true);
            }
            case EXECUTED -> {
                buf.writeBytes(SIDE);
                writeAsciiString(buf, event.getSide() == null ? null : event.getSide().name(), true);
                buf.writeBytes(CONTRA_ORDER_ID);
                writeAsciiString(buf, event.getContraOrderId(), true);
                buf.writeBytes(FILL_PRICE);
                writePrice(buf, event.getFillPrice());
                buf.writeBytes(FILL_QTY);
                writeLong(buf, event.getFillQty());
                buf.writeBytes(LEAVES_QTY);
                writeLong(buf, event.getLeavesQty());
                buf.writeBytes(CUM_QTY);
                writeLong(buf, event.getCumQty());
                buf.writeBytes(FULL_FILL);
                buf.writeBytes(event.isFullFill() ? TRUE_VAL : FALSE_VAL);
                buf.writeBytes(LATENCY_NANOS);
                writeLong(buf, Math.max(0L, event.getEventEmittedNanos() - event.getEngineInNanos()));
            }
            case CANCELLED -> {
                buf.writeBytes(CANCELLED_QTY);
                writeLong(buf, event.getCancelledQty());
                buf.writeBytes(CANCELLED_REASON);
                writeAsciiString(buf, event.getMessage(), true);
            }
            case RESTATED -> {
                buf.writeBytes(RESTATED_PRICE);
                writePrice(buf, event.getPrice());
                buf.writeBytes(QUANTITY);
                writeLong(buf, event.getQuantity());
                buf.writeBytes(LEAVES_QTY);
                writeLong(buf, event.getLeavesQty());
                buf.writeBytes(CUM_QTY);
                writeLong(buf, event.getCumQty());
            }
            case ADMIN -> {
                buf.writeBytes(OPERATION);
                writeAsciiString(buf, event.getAdminOperation() == null ? null : event.getAdminOperation().name(), true);
                buf.writeBytes(MESSAGE);
                writeAsciiString(buf, event.getMessage(), true);
            }
        }
    }
    
    private static void writeAsciiString(ByteBuf buf, String s, boolean quote) {
        if (s == null) {
            buf.writeBytes(NULL_VAL);
            return;
        }
        if (quote) buf.writeByte('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> { buf.writeByte('\\'); buf.writeByte('"'); }
                case '\\' -> { buf.writeByte('\\'); buf.writeByte('\\'); }
                case '\b' -> { buf.writeByte('\\'); buf.writeByte('b'); }
                case '\f' -> { buf.writeByte('\\'); buf.writeByte('f'); }
                case '\n' -> { buf.writeByte('\\'); buf.writeByte('n'); }
                case '\r' -> { buf.writeByte('\\'); buf.writeByte('r'); }
                case '\t' -> { buf.writeByte('\\'); buf.writeByte('t'); }
                default -> {
                    if (c < 0x20) {
                        buf.writeByte('\\'); buf.writeByte('u');
                        String hex = Integer.toHexString(c);
                        for (int pad = hex.length(); pad < 4; pad++) {
                            buf.writeByte('0');
                        }
                        for (int j = 0; j < hex.length(); j++) {
                            buf.writeByte((byte) hex.charAt(j));
                        }
                    } else {
                        buf.writeByte((byte) c);
                    }
                }
            }
        }
        if (quote) buf.writeByte('"');
    }

    private static void writeLong(ByteBuf buf, long value) {
        if (value == 0) {
            buf.writeByte('0');
            return;
        }
        if (value == Long.MIN_VALUE) {
            buf.writeBytes("-9223372036854775808".getBytes(StandardCharsets.US_ASCII));
            return;
        }
        
        boolean negative = value < 0;
        if (negative) {
            buf.writeByte('-');
            value = -value;
        }
        
        byte[] numBuf = NUM_BUFFER.get();
        int idx = 0;
        while (value > 0) {
            numBuf[idx++] = (byte) ('0' + (value % 10));
            value /= 10;
        }
        
        while (idx > 0) {
            buf.writeByte(numBuf[--idx]);
        }
    }
    
    private static void writePrice(ByteBuf buf, Price price) {
        if (price == null) {
            buf.writeBytes(NULL_VAL);
            return;
        }
        long cents = price.getCents();
        long absCents = Math.abs(cents);
        long dollars = absCents / 100;
        long fractional = absCents % 100;
        
        buf.writeByte('"');
        if (cents < 0) {
            buf.writeByte('-');
        }
        writeLong(buf, dollars);
        buf.writeByte('.');
        if (fractional < 10) {
            buf.writeByte('0');
        }
        writeLong(buf, fractional);
        buf.writeByte('"');
    }

    private static void writeInstant(ByteBuf buf, Instant instant) {
        if (instant == null) {
            buf.writeBytes(NULL_VAL);
            return;
        }
        buf.writeByte('"');
        writeLong(buf, instant.getEpochSecond());
        buf.writeByte('.');
        int nanos = instant.getNano();
        int divisor = 100_000_000;
        while (divisor > 0) {
            buf.writeByte((byte) ('0' + (nanos / divisor) % 10));
            nanos %= divisor;
            divisor /= 10;
        }
        buf.writeByte('"');
    }
}
