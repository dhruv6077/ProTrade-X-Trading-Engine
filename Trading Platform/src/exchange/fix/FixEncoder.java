package exchange.fix;

import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class FixEncoder {
    
    private static final byte SOH = 0x01;
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private FixEncoder() {}

    public static void sendLogonReply(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer(256);
        writeHeader(buf, "A", 0); // We'll patch length later
        int bodyStart = buf.writerIndex();
        
        writeTag(buf, 98, "0"); // EncryptMethod
        writeTag(buf, 108, "30"); // HeartBtInt
        
        finishMessage(ctx, buf, bodyStart);
    }

    public static void sendExecutionReport(ChannelHandlerContext ctx, ExchangeEvent event) {
        ByteBuf buf = ctx.alloc().buffer(256);
        writeHeader(buf, "8", 0);
        int bodyStart = buf.writerIndex();

        if (event instanceof OrderAccepted acc) {
            writeTag(buf, 37, acc.orderId()); // OrderID
            writeTag(buf, 11, acc.orderId()); // ClOrdID
            writeTag(buf, 150, "0"); // ExecType = New
            writeTag(buf, 39, "0"); // OrdStatus = New
            writeTag(buf, 55, acc.symbol()); // Symbol
            writeTag(buf, 54, acc.order().side().name().equals("BUY") ? "1" : "2"); // Side
            writeTag(buf, 151, String.valueOf(acc.order().quantity())); // LeavesQty
            writeTag(buf, 14, "0"); // CumQty
        } else if (event instanceof OrderExecuted exec) {
            writeTag(buf, 37, exec.orderId()); 
            writeTag(buf, 11, exec.orderId()); 
            writeTag(buf, 150, exec.fullFill() ? "2" : "1"); // ExecType = Fill / Partial fill
            writeTag(buf, 39, exec.fullFill() ? "2" : "1"); // OrdStatus
            writeTag(buf, 55, exec.symbol());
            writeTag(buf, 54, exec.side().name().equals("BUY") ? "1" : "2");
            writeTag(buf, 31, priceString(exec.fillPrice().getCents())); // LastPx
            writeTag(buf, 32, String.valueOf(exec.fillQty())); // LastQty
            writeTag(buf, 151, String.valueOf(exec.leavesQty()));
            writeTag(buf, 14, String.valueOf(exec.cumQty()));
        } else if (event instanceof OrderCancelled cxl) {
            writeTag(buf, 37, cxl.orderId());
            writeTag(buf, 11, cxl.orderId());
            writeTag(buf, 150, "4"); // ExecType = Canceled
            writeTag(buf, 39, "4"); // OrdStatus
            writeTag(buf, 55, cxl.symbol());
            writeTag(buf, 151, "0");
            writeTag(buf, 14, "0");
        } else if (event instanceof OrderRejected rej) {
            writeTag(buf, 37, "NONE");
            writeTag(buf, 11, rej.orderId());
            writeTag(buf, 150, "8"); // ExecType = Rejected
            writeTag(buf, 39, "8"); // OrdStatus
            writeTag(buf, 55, rej.symbol());
            writeTag(buf, 58, rej.reason().name()); // Text
            writeTag(buf, 151, "0");
            writeTag(buf, 14, "0");
        } else {
            buf.release();
            return;
        }

        finishMessage(ctx, buf, bodyStart);
    }

    private static void writeHeader(ByteBuf buf, String msgType, int bodyLen) {
        buf.writeBytes("8=FIX.4.4".getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(SOH);
        
        // We leave 9= unfilled initially, or we write it later
        // A simple zero-copy hack is to append it first and overwrite
        // For simplicity, we just format it as string because this is a basic PoC
        buf.writeBytes(("9=000").getBytes(StandardCharsets.US_ASCII)); 
        buf.writeByte(SOH);
        
        buf.writeBytes("35=".getBytes(StandardCharsets.US_ASCII));
        buf.writeBytes(msgType.getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(SOH);
        
        buf.writeBytes("49=EXCHANGE".getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(SOH);
        
        buf.writeBytes("56=CLIENT".getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(SOH);
        
        buf.writeBytes("52=".getBytes(StandardCharsets.US_ASCII));
        buf.writeBytes(TIME_FORMAT.format(Instant.now()).getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(SOH);
    }

    private static void finishMessage(ChannelHandlerContext ctx, ByteBuf buf, int bodyStart) {
        int bodyEnd = buf.writerIndex();
        int bodyLength = bodyEnd - bodyStart;
        
        // Patch the body length at '9=000'
        // '8=FIX.4.4|9=' takes 10 + 2 = 12 bytes. So digits start at index 12.
        int lenIdx = 12;
        buf.setByte(lenIdx, (byte) ('0' + (bodyLength / 100) % 10));
        buf.setByte(lenIdx + 1, (byte) ('0' + (bodyLength / 10) % 10));
        buf.setByte(lenIdx + 2, (byte) ('0' + bodyLength % 10));
        
        // Checksum
        int sum = 0;
        for (int i = 0; i < buf.writerIndex(); i++) {
            sum += buf.getByte(i);
        }
        sum %= 256;
        
        buf.writeBytes("10=".getBytes(StandardCharsets.US_ASCII));
        buf.writeByte((byte) ('0' + (sum / 100) % 10));
        buf.writeByte((byte) ('0' + (sum / 10) % 10));
        buf.writeByte((byte) ('0' + sum % 10));
        buf.writeByte(SOH);
        
        ctx.writeAndFlush(buf);
    }

    private static void writeTag(ByteBuf buf, int tag, String value) {
        byte[] t = String.valueOf(tag).getBytes(StandardCharsets.US_ASCII);
        byte[] v = value == null ? new byte[0] : value.getBytes(StandardCharsets.US_ASCII);
        buf.writeBytes(t);
        buf.writeByte((byte) '=');
        buf.writeBytes(v);
        buf.writeByte(SOH);
    }

    private static String priceString(long cents) {
        long dollars = cents / 100;
        long c = cents % 100;
        return dollars + "." + (c < 10 ? "0" + c : c);
    }
}
