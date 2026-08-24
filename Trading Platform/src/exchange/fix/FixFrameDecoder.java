package exchange.fix;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ByteProcessor;

import java.util.List;

public final class FixFrameDecoder extends ByteToMessageDecoder {
    
    private static final byte SOH = 0x01;
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int readerIndex = in.readerIndex();
        int writerIndex = in.writerIndex();
        int readableBytes = writerIndex - readerIndex;

        // Search for the end of the message: "10=" followed by checksum and SOH
        // Instead of exact match, just look for SOH to delineate fields, and we know 10= is the last one.
        // Wait, multiple messages could be in the buffer.
        // The most robust way is to parse tag 9 (BodyLength).
        // 8=FIX.4.4|9=length|
        
        if (readableBytes < 15) {
            return;
        }
        
        // Find first SOH
        int firstSoh = in.indexOf(readerIndex, writerIndex, SOH);
        if (firstSoh == -1) return;
        
        // Find second SOH
        int secondSoh = in.indexOf(firstSoh + 1, writerIndex, SOH);
        if (secondSoh == -1) return;
        
        // Ensure second field is tag 9
        if (in.getByte(firstSoh + 1) != '9' || in.getByte(firstSoh + 2) != '=') {
            // Invalid FIX header, skip bytes?
            in.readByte();
            return;
        }
        
        int bodyLength = FixParser.parseAsciiInt(in, firstSoh + 3, secondSoh - (firstSoh + 3));
        
        // The full message length is: (secondSoh - readerIndex) + 1 + bodyLength + 7 (for 10=xxx|)
        int messageLength = (secondSoh - readerIndex) + 1 + bodyLength + 7;
        
        if (readableBytes < messageLength) {
            return; // Wait for more data
        }
        
        ByteBuf frame = in.readRetainedSlice(messageLength);
        out.add(frame);
    }
}
