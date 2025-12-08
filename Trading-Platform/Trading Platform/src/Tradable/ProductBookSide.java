package Tradable;

import Exceptions.DataValidationException;
import Exceptions.InvalidUserInput;
import Price.Price;
import User.UserManager;
import logging.AuditEvent;
import logging.AuditEventType;
import logging.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static Tradable.BookSide.*;

/**
 * Represents one side (BUY or SELL) of a product book.
 * Thread-safe implementation with audit logging support.
 */
public class ProductBookSide {
    private static final Logger logger = LoggerFactory.getLogger(ProductBookSide.class);
    private final AuditLogger auditLogger = AuditLogger.getInstance();

    private final BookSide side;
    private final HashMap<Price, ArrayList<Tradable>> bookEntries;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructs a ProductBookSide for the specified side.
     * 
     * @param side The side of the book (BUY or SELL)
     * @throws InvalidUserInput if side is null
     */
    public ProductBookSide(BookSide side) throws InvalidUserInput {
        if (side == null) {
            throw new InvalidUserInput("Bookside cannot be null.");
        }
        this.side = side;
        this.bookEntries = new HashMap<>();
        logger.debug("ProductBookSide created for side: {}", side);
    }

    /**
     * Adds a tradable to the book.
     * 
     * @param o The tradable to add
     * @return TradableDTO representing the added tradable
     * @throws InvalidUserInput if tradable is null
     */
    public TradableDTO add(Tradable o) throws InvalidUserInput {
        if (o == null) {
            throw new InvalidUserInput("Tradable input is null in add method.");
        }

        lock.writeLock().lock();
        try {
            Price price = o.getPrice();
            ArrayList<Tradable> tAbleArrList = bookEntries.computeIfAbsent(price, k -> new ArrayList<>());
            tAbleArrList.add(o);

            logger.info("Added tradable: {} {} @ {} qty {}", side, o.getId(), price, o.getOriginalVolume());

            return o.makeTradableDTO();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cancels a tradable by ID.
     * 
     * @param tradableId The ID of the tradable to cancel
     * @return TradableDTO of the cancelled tradable, or null if not found
     * @throws InvalidUserInput if tradableId is null
     */
    public TradableDTO cancel(String tradableId) throws InvalidUserInput {
        if (tradableId == null) {
            throw new InvalidUserInput("tradableID is null in cancel method");
        }

        lock.writeLock().lock();
        try {
            // Use iterator to safely remove while iterating
            for (Map.Entry<Price, ArrayList<Tradable>> entry : bookEntries.entrySet()) {
                ArrayList<Tradable> tradablesArrList = entry.getValue();
                Iterator<Tradable> iterator = tradablesArrList.iterator();

                while (iterator.hasNext()) {
                    Tradable eachTradable = iterator.next();

                    if (tradableId.equals(eachTradable.getId())) {
                        iterator.remove(); // Safe removal using iterator

                        // Update cancelled volume
                        int cancelledQty = eachTradable.getRemainingVolume();
                        eachTradable.setCancelledVolume(eachTradable.getCancelledVolume() + cancelledQty);
                        eachTradable.setRemainingVolume(0);

                        // Remove price level if empty
                        if (tradablesArrList.isEmpty()) {
                            bookEntries.remove(eachTradable.getPrice());
                        }

                        // Log cancellation
                        logger.info("CANCEL: {}: {} Cxl Qty: {}", side, eachTradable.getId(), cancelledQty);

                        // Audit log
                        AuditEvent event = new AuditEvent.Builder()
                                .eventType(AuditEventType.ORDER_CANCELLED)
                                .userId(eachTradable.getUser())
                                .product(eachTradable.getProduct())
                                .addData("side", side.toString())
                                .addData("orderId", tradableId)
                                .addData("cancelledQuantity", cancelledQty)
                                .build();
                        auditLogger.logEvent(event);

                        return eachTradable.makeTradableDTO();
                    }
                }
            }

            logger.warn("Tradable not found for cancellation: {}", tradableId);
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all quotes for a specific user.
     * 
     * @param userName The username whose quotes should be removed
     * @return TradableDTO of the first removed quote, or null if none found
     * @throws InvalidUserInput if userName is null
     */
    public TradableDTO removeQuotesForUser(String userName) throws InvalidUserInput {
        if (userName == null) {
            throw new InvalidUserInput("Username is null in removeQuotesForUser.");
        }

        lock.writeLock().lock();
        try {
            // Collect tradables to cancel (avoid ConcurrentModificationException)
            List<String> tradablesToCancel = new ArrayList<>();

            for (ArrayList<Tradable> tradablesArrList : bookEntries.values()) {
                for (Tradable eachTradable : tradablesArrList) {
                    if (userName.equals(eachTradable.getUser())) {
                        tradablesToCancel.add(eachTradable.getId());
                    }
                }
            }

            // Cancel collected tradables
            TradableDTO firstCancelled = null;
            for (String tradableId : tradablesToCancel) {
                TradableDTO canceledDTO = cancelInternal(tradableId);
                if (canceledDTO != null && firstCancelled == null) {
                    firstCancelled = canceledDTO;
                }
            }

            logger.info("Removed {} quotes for user: {}", tradablesToCancel.size(), userName);
            return firstCancelled;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Internal cancel method without locking (assumes lock is already held).
     */
    private TradableDTO cancelInternal(String tradableId) {
        for (Map.Entry<Price, ArrayList<Tradable>> entry : bookEntries.entrySet()) {
            ArrayList<Tradable> tradablesArrList = entry.getValue();
            Iterator<Tradable> iterator = tradablesArrList.iterator();

            while (iterator.hasNext()) {
                Tradable eachTradable = iterator.next();

                if (tradableId.equals(eachTradable.getId())) {
                    iterator.remove();

                    int cancelledQty = eachTradable.getRemainingVolume();
                    eachTradable.setCancelledVolume(eachTradable.getCancelledVolume() + cancelledQty);
                    eachTradable.setRemainingVolume(0);

                    if (tradablesArrList.isEmpty()) {
                        bookEntries.remove(eachTradable.getPrice());
                    }

                    return eachTradable.makeTradableDTO();
                }
            }
        }
        return null;
    }

    /**
     * Gets the best price on this side of the book.
     * 
     * @return The top of book price, or null if book is empty
     */
    public Price topOfBookPrice() {
        lock.readLock().lock();
        try {
            if (bookEntries.isEmpty()) {
                return null;
            }

            Price highestPrice = null;
            Price lowestPrice = null;

            for (Price price : bookEntries.keySet()) {
                if (lowestPrice == null || price.compareTo(lowestPrice) < 0) {
                    lowestPrice = price;
                }

                if (highestPrice == null || price.compareTo(highestPrice) > 0) {
                    highestPrice = price;
                }
            }

            return (side == BUY) ? highestPrice : lowestPrice;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the total volume at the top of book price.
     * 
     * @return Total volume, or 0 if book is empty
     */
    public int topOfBookVolume() {
        lock.readLock().lock();
        try {
            Price topPrice = topOfBookPrice();
            if (topPrice == null) {
                return 0;
            }

            ArrayList<Tradable> tradables = bookEntries.get(topPrice);
            if (tradables == null) {
                return 0;
            }

            int totalVol = 0;
            for (Tradable eachTradable : tradables) {
                totalVol += eachTradable.getRemainingVolume();
            }
            return totalVol;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Executes trades by removing volume from the book.
     * 
     * @param price The price level to trade at
     * @param vol   The volume to trade
     * @throws InvalidUserInput        if price is null
     * @throws DataValidationException if data validation fails
     */
    public void tradeOut(Price price, int vol) throws InvalidUserInput, DataValidationException {
        if (price == null) {
            throw new InvalidUserInput("Price is null in tradeOut.");
        }

        if (vol <= 0) {
            throw new InvalidUserInput("Volume must be positive in tradeOut.");
        }

        lock.writeLock().lock();
        try {
            ArrayList<Tradable> orderArrList = bookEntries.get(price);
            if (orderArrList == null || orderArrList.isEmpty()) {
                logger.warn("No orders found at price {} for tradeOut", price);
                return;
            }

            // Work with a copy to avoid concurrent modification
            ArrayList<Tradable> workingList = new ArrayList<>(orderArrList);
            int remainingVol = vol;

            while (remainingVol > 0 && !workingList.isEmpty()) {
                Tradable currOrder = workingList.get(0);
                int orderRemainingVol = currOrder.getRemainingVolume();

                if (orderRemainingVol <= remainingVol) {
                    // Full fill
                    workingList.remove(0);
                    currOrder.setFilledVolume(currOrder.getFilledVolume() + orderRemainingVol);
                    currOrder.setRemainingVolume(0);
                    remainingVol -= orderRemainingVol;

                    logger.info("FULL FILL: ({} {}) {}", side, orderRemainingVol, currOrder.toString());

                    TradableDTO orderDTO = currOrder.makeTradableDTO();
                    UserManager.getInstance().addToUser(currOrder.getUser(), orderDTO);

                    // Audit log
                    AuditEvent event = new AuditEvent.Builder()
                            .eventType(AuditEventType.ORDER_FILLED)
                            .userId(currOrder.getUser())
                            .product(currOrder.getProduct())
                            .addData("side", side.toString())
                            .addData("price", price.toString())
                            .addData("quantity", orderRemainingVol)
                            .addData("fillType", "FULL")
                            .build();
                    auditLogger.logEvent(event);
                } else {
                    // Partial fill
                    currOrder.setRemainingVolume(orderRemainingVol - remainingVol);
                    currOrder.setFilledVolume(currOrder.getFilledVolume() + remainingVol);

                    logger.info("PARTIAL FILL: ({} {}) {}", side, remainingVol, currOrder.toString());

                    TradableDTO orderDTO = currOrder.makeTradableDTO();
                    UserManager.getInstance().addToUser(currOrder.getUser(), orderDTO);

                    // Audit log
                    AuditEvent event = new AuditEvent.Builder()
                            .eventType(AuditEventType.ORDER_FILLED)
                            .userId(currOrder.getUser())
                            .product(currOrder.getProduct())
                            .addData("side", side.toString())
                            .addData("price", price.toString())
                            .addData("quantity", remainingVol)
                            .addData("fillType", "PARTIAL")
                            .build();
                    auditLogger.logEvent(event);

                    remainingVol = 0;
                }
            }

            // Update the book
            if (workingList.isEmpty()) {
                bookEntries.remove(price);
            } else {
                bookEntries.put(price, workingList);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            List<Map.Entry<Price, ArrayList<Tradable>>> bookEntriesList = new ArrayList<>(bookEntries.entrySet());

            bookEntriesList.sort(Comparator.comparing(Map.Entry::getKey));

            if (side == BUY) {
                Collections.reverse(bookEntriesList);
            }

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Side: %s%n", side));

            for (Map.Entry<Price, ArrayList<Tradable>> keyValue : bookEntriesList) {
                sb.append(String.format("    Price: %s%n", keyValue.getKey()));
                for (Tradable tAble : keyValue.getValue()) {
                    sb.append(String.format("        %s%n", tAble.toString()));
                }
            }

            logger.debug(sb.toString());
            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
