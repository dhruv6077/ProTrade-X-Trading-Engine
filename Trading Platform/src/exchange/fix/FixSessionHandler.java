package exchange.fix;

import exchange.gateway.OrderGateway;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import Price.PriceFactory;
import Exceptions.InvalidPriceOperation;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FixSessionHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger logger = LoggerFactory.getLogger(FixSessionHandler.class);
    
    private final OrderGateway gateway;
    private String clientId;

    public FixSessionHandler(OrderGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        int readerIndex = msg.readerIndex();
        int writerIndex = msg.writerIndex();
        
        // Very basic zero-copy traversal
        int cursor = readerIndex;
        byte msgType = 0;
        String clOrdId = null;
        String symbol = null;
        Side side = null;
        int orderQty = 0;
        long priceCents = 0;
        OrderType orderType = OrderType.LIMIT;
        
        while (cursor < writerIndex) {
            int equalsIndex = msg.indexOf(cursor, writerIndex, (byte) '=');
            if (equalsIndex == -1) break;
            
            int sohIndex = msg.indexOf(equalsIndex + 1, writerIndex, (byte) 0x01);
            if (sohIndex == -1) break;
            
            int tag = FixParser.parseAsciiInt(msg, cursor, equalsIndex - cursor);
            int valLen = sohIndex - (equalsIndex + 1);
            int valStart = equalsIndex + 1;
            
            switch (tag) {
                case 35: // MsgType
                    msgType = msg.getByte(valStart);
                    break;
                case 49: // SenderCompID
                    if (clientId == null) {
                        clientId = FixParser.parseAsciiString(msg, valStart, valLen);
                    }
                    break;
                case 11: // ClOrdID
                    clOrdId = FixParser.parseAsciiString(msg, valStart, valLen);
                    break;
                case 55: // Symbol
                    symbol = FixParser.parseAsciiString(msg, valStart, valLen);
                    break;
                case 54: // Side
                    byte sideByte = msg.getByte(valStart);
                    side = (sideByte == '1') ? Side.BUY : Side.SELL;
                    break;
                case 38: // OrderQty
                    orderQty = FixParser.parseAsciiInt(msg, valStart, valLen);
                    break;
                case 44: // Price
                    priceCents = FixParser.parseAsciiPriceCents(msg, valStart, valLen);
                    break;
                case 40: // OrdType
                    byte ordTypeByte = msg.getByte(valStart);
                    orderType = (ordTypeByte == '2') ? OrderType.LIMIT : OrderType.MARKET;
                    break;
            }
            cursor = sohIndex + 1;
        }

        if (msgType == 'A') { // Logon
            FixEncoder.sendLogonReply(ctx);
        } else if (msgType == 'D') { // NewOrderSingle
            try {
                gateway.submitNewOrderAsync(
                    clOrdId,
                    clientId,
                    symbol,
                    side,
                    orderType,
                    priceCents > 0 ? PriceFactory.makePrice(priceCents) : null,
                    orderQty,
                    SelfTradePreventionMode.CANCEL_NEWEST
                );
            } catch (InvalidPriceOperation e) {
                logger.error("Invalid price", e);
            }
        } else if (msgType == 'F') { // OrderCancelRequest
            // We just submit a MutableCancelOrderCommand using gateway.submitNewOrderAsync? 
            // Wait, gateway has no submitCancelOrderAsync. I will implement a quick object creation for cancel.
            exchange.model.MutableCancelOrderCommand cancelCmd = new exchange.model.MutableCancelOrderCommand();
            cancelCmd.populate(0, java.time.Instant.now(), clOrdId, clientId, symbol, 0);
            gateway.submitAsync(cancelCmd);
        }
    }
}
