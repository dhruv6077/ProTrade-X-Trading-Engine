package Tradable;

import Exceptions.DataValidationException;
import Exceptions.InvalidPriceOperation;
import Exceptions.InvalidUserInput;
import Market.CurrentMarketTracker;
import Price.*;
import logging.AuditEvent;
import logging.AuditEventType;
import logging.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static Tradable.BookSide.*;
import User.UserManager;

public class ProductBook {
    private static final Logger logger = LoggerFactory.getLogger(ProductBook.class);
    private final AuditLogger auditLogger = AuditLogger.getInstance();
    
    private final String product;
    ProductBookSide buySide;
    ProductBookSide sellSide;
    private static final String productIdRegex = "[a-zA-Z0-9.]*";

    public ProductBook(String product) throws InvalidUserInput {
        if (product == null || product.isEmpty() || product.length() > 5 || !product.matches(productIdRegex)) {
            throw new InvalidUserInput("Invalid product input.");
        }
        this.product = product;
        this.buySide = new ProductBookSide(BUY);
        this.sellSide = new ProductBookSide(SELL);
    }

    public TradableDTO add(Tradable t) throws InvalidUserInput, DataValidationException, InvalidPriceOperation {
        if(t == null) {
            throw new InvalidUserInput("Tradable cannot be null.");
        }

        ProductBookSide side;
        if (t.getSide() == BUY) {
            logger.info("ADD: {}: {}", BUY, t);
            side = buySide;
        } else {
            logger.info("ADD: {}: {}", SELL, t);
            side = sellSide;
        }
        
        // Audit log the order placement
        AuditEvent event = new AuditEvent.Builder()
                .eventType(AuditEventType.ORDER_PLACED)
                .userId(t.getUser())
                .product(t.getProduct())
                .addData("side", t.getSide().toString())
                .addData("price", t.getPrice().toString())
                .addData("quantity", t.getOriginalVolume())
                .addData("orderId", t.getId())
                .build();
        auditLogger.logEvent(event);
        
        TradableDTO addDTO = side.add(t);
        tryTrade();
        updateMarket();
        return addDTO;

    }

    public TradableDTO[] add(Quote qte) throws InvalidUserInput, DataValidationException, InvalidPriceOperation {
        TradableDTO[] arrOfDtos = new TradableDTO[2];
        arrOfDtos[0] = buySide.add(qte.getQuoteSide(BUY));
        logger.info("ADD: {}: {}", BUY, qte.getQuoteSide(BUY));
        arrOfDtos[1] = sellSide.add(qte.getQuoteSide(SELL));
        logger.info("ADD: {}: {}", SELL, qte.getQuoteSide(SELL));
        
        // Audit log the quote submission
        AuditEvent event = new AuditEvent.Builder()
                .eventType(AuditEventType.QUOTE_SUBMITTED)
                .userId(qte.getUser())
                .product(qte.getSymbol())
                .addData("buyPrice", qte.getQuoteSide(BUY).getPrice().toString())
                .addData("buyQuantity", qte.getQuoteSide(BUY).getOriginalVolume())
                .addData("sellPrice", qte.getQuoteSide(SELL).getPrice().toString())
                .addData("sellQuantity", qte.getQuoteSide(SELL).getOriginalVolume())
                .build();
        auditLogger.logEvent(event);
        
        tryTrade();
        updateMarket();
        return arrOfDtos;
    }

    public TradableDTO cancel(BookSide side, String orderId) throws InvalidUserInput, InvalidPriceOperation {
        TradableDTO sides;
        if(side == BUY) {
            sides = buySide.cancel(orderId);
        } else {
            sides =  sellSide.cancel(orderId);
        }
        
        // Audit log the cancellation
        if (sides != null) {
            AuditEvent event = new AuditEvent.Builder()
                    .eventType(AuditEventType.ORDER_CANCELLED)
                    .userId(sides.user)
                    .product(sides.product)
                    .addData("side", side.toString())
                    .addData("orderId", orderId)
                    .addData("remainingQuantity", sides.remainingVolume)
                    .build();
            auditLogger.logEvent(event);
            logger.info("Order cancelled: {} {} {}", side, orderId, sides.product);
        }
        
        updateMarket();
        return sides;
    }

    public void tryTrade() throws InvalidUserInput, DataValidationException {
        Price topBuyPrice = buySide.topOfBookPrice();      // Get Top Buy Price
        Price topSellPrice = sellSide.topOfBookPrice();    // Get Top Sell Price

        while (topBuyPrice != null && topSellPrice != null && topBuyPrice.compareTo(topSellPrice) >= 0) {
            int topBuyVolume = buySide.topOfBookVolume();       // Get Top Buy Volume
            int topSellVolume = sellSide.topOfBookVolume();     // Get Top Sell Volume
            int volumeToTrade = Math.min(topBuyVolume, topSellVolume);      // Volume to trade is the MIN of the Buy and Sell volumes

            // Audit log the trade execution
            AuditEvent event = new AuditEvent.Builder()
                    .eventType(AuditEventType.TRADE_EXECUTED)
                    .product(product)
                    .addData("price", topBuyPrice.toString())
                    .addData("quantity", volumeToTrade)
                    .addData("buyPrice", topBuyPrice.toString())
                    .addData("sellPrice", topSellPrice.toString())
                    .build();
            auditLogger.logEvent(event);
            logger.info("Trade executed: {} @ {} qty {}", product, topBuyPrice, volumeToTrade);

            sellSide.tradeOut(topSellPrice, volumeToTrade);
            buySide.tradeOut(topBuyPrice, volumeToTrade);

            topBuyPrice = buySide.topOfBookPrice();     // Get Top Buy Price
            topSellPrice = sellSide.topOfBookPrice();   // Get Top Sell Price
        }
    }

    public TradableDTO[] removeQuotesForUser(String userName) throws InvalidUserInput, InvalidPriceOperation {
        TradableDTO[] arrayOfDTO = new TradableDTO[2];
        arrayOfDTO[0] = buySide.removeQuotesForUser(userName);
        arrayOfDTO[1] = sellSide.removeQuotesForUser(userName);

        UserManager.getInstance().getUser(userName).addTradable(arrayOfDTO[0]);
        UserManager.getInstance().getUser(userName).addTradable(arrayOfDTO[1]);
        updateMarket();


        return arrayOfDTO;
    }

    private void updateMarket() throws InvalidPriceOperation {
        int topSellVolume;
        int topBuyVolume;
        Price topBuyPrice = buySide.topOfBookPrice();
        Price topSellPrice = sellSide.topOfBookPrice();

        if(topBuyPrice == null && topSellPrice != null) {
            CurrentMarketTracker.getInstance().updateMarket(product, topBuyPrice, 0, topSellPrice, sellSide.topOfBookVolume());
        } else if (topSellPrice == null && topBuyPrice != null) {
            CurrentMarketTracker.getInstance().updateMarket(product, topBuyPrice, buySide.topOfBookVolume(), topSellPrice, 0);
        }

        if (topBuyPrice != null && topSellPrice != null) {
            topBuyVolume = buySide.topOfBookVolume();
            topSellVolume = sellSide.topOfBookVolume();

            CurrentMarketTracker.getInstance().updateMarket(product, topBuyPrice, topBuyVolume, topSellPrice, topSellVolume);
        }

        if(topBuyPrice == null && topSellPrice == null) {
            CurrentMarketTracker.getInstance().updateMarket(product, topBuyPrice, 0, topSellPrice, 0);
        }




    }


        @Override
        public String toString() {
            logger.debug("-----------------------------------");
            logger.debug("Product Book: {}", product);
            return "%s\n%s\n-----------------------------------".formatted(buySide.toString(), sellSide.toString());
        }



}
