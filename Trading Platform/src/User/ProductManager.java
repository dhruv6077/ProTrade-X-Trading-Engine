package User;

import Exceptions.DataValidationException;
import Exceptions.InvalidPriceOperation;
import Exceptions.InvalidUserInput;
import Tradable.*;
import exchange.core.ExchangeRuntime;
import exchange.model.CancelOrderCommand;
import exchange.model.ExchangeEvent;
import exchange.model.NewOrderCommand;
import exchange.model.OrderAccepted;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import logging.AuditEvent;
import logging.AuditEventType;
import logging.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static Tradable.BookSide.*;

public final class ProductManager {
    private static final Logger logger = LoggerFactory.getLogger(ProductManager.class);
    private final AuditLogger auditLogger = AuditLogger.getInstance();

    private static ProductManager instance;
    private final ConcurrentMap<String, ProductBook> pbooks = new ConcurrentHashMap<>();
    private static final String symbolRegex = "[A-Z]{1,5}";

    private ProductManager() {
    }

    public static synchronized ProductManager getInstance() {
        if (instance == null) {
            instance = new ProductManager();
        }
        return instance;
    }

    public void addProduct(String symbol) throws InvalidUserInput, DataValidationException {
        if (symbol == null || !symbol.matches(symbolRegex)) {
            throw new DataValidationException("Symbol entered is invalid");
        }
        pbooks.computeIfAbsent(symbol, ignored -> {
            try {
                return new ProductBook(symbol);
            } catch (InvalidUserInput e) {
                throw new IllegalArgumentException(e);
            }
        });
        ExchangeRuntime.getInstance().addSymbol(symbol);

        // Audit log product addition
        AuditEvent event = new AuditEvent.Builder()
                .eventType(AuditEventType.PRODUCT_ADDED)
                .product(symbol)
                .build();
        auditLogger.logEvent(event);
        logger.info("Product added: {}", symbol);
    }

    public ProductBook getProductBook(String symbol) throws DataValidationException {
        if (symbol == null || !symbol.matches(symbolRegex)) {
            throw new DataValidationException("Symbol entered is invalid.");
        }

        ProductBook pb = pbooks.get(symbol);
        if (pb == null) {
            throw new DataValidationException("Product is not found in ProductBook.");
        }
        return pb;
    }

    public String getRandomProduct() throws DataValidationException {
        if (pbooks.isEmpty()) {
            throw new DataValidationException("ProductBook is empty.");
        }

        List<String> listOfSymbols = new ArrayList<>(pbooks.keySet());
        Collections.shuffle(listOfSymbols);
        return listOfSymbols.get(0);
    }

    public TradableDTO addTradable(Tradable o) throws DataValidationException, InvalidUserInput, InvalidPriceOperation {
        if (o == null) {
            throw new DataValidationException("Tradable entered is null");
        }

        getProductBook(o.getProduct());

        List<ExchangeEvent> events = ExchangeRuntime.getInstance().gateway().submitNewOrder(
                o.getId(),
                o.getUser(),
                o.getProduct(),
                toExchangeSide(o.getSide()),
                OrderType.LIMIT,
                o.getPrice(),
                o.getOriginalVolume(),
                SelfTradePreventionMode.CANCEL_NEWEST);
        throwIfRejected(events);
        TradableDTO trdbleDTO = dtoFromEvents(o, events);
        UserManager.getInstance().addToUser(o.getUser(), trdbleDTO);

        return trdbleDTO;
    }

    public TradableDTO[] addQuote(Quote q) throws DataValidationException, InvalidUserInput, InvalidPriceOperation {
        if (q == null) {
            throw new DataValidationException("Quote entered is null");
        }

        TradableDTO buyTradeDTO = addTradable(q.getQuoteSide(BUY));
        TradableDTO sellTradeDTO = addTradable(q.getQuoteSide(SELL));
        return new TradableDTO[] { buyTradeDTO, sellTradeDTO };
    }

    public TradableDTO cancel(TradableDTO o) throws DataValidationException, InvalidUserInput, InvalidPriceOperation {
        if (o == null) {
            throw new DataValidationException("TradableDTO entered is null");
        }

        getProductBook(o.product);

        List<ExchangeEvent> events = ExchangeRuntime.getInstance().gateway().process(
                new CancelOrderCommand(0, o.id, o.user, o.product));
        throwIfRejected(events);
        TradableDTO canceledTradableDTO = dtoAfterCancel(o, events);

        if (canceledTradableDTO == null) {
            logger.warn("Failure to cancel order: {}", o.id);
            return null;
        }

        return canceledTradableDTO;
    }

    public TradableDTO[] cancelQuote(String symbol, String user)
            throws DataValidationException, InvalidUserInput, InvalidPriceOperation {
        if (symbol == null || user == null) {
            throw new DataValidationException("Symbol or user cannot be null");
        }

        getProductBook(symbol);
        logger.warn("Bulk quote cancellation is not exposed through the gateway yet: {} {}", symbol, user);
        return new TradableDTO[2];
    }

    public ArrayList<String> getProductList() {
        return new ArrayList<>(pbooks.keySet());
    }

    private Side toExchangeSide(BookSide side) {
        return side == BUY ? Side.BUY : Side.SELL;
    }

    private void throwIfRejected(List<ExchangeEvent> events) throws DataValidationException {
        for (ExchangeEvent event : events) {
            if (event instanceof OrderRejected rejected) {
                throw new DataValidationException(rejected.reason() + ": " + rejected.message());
            }
        }
    }

    private TradableDTO dtoFromEvents(Tradable source, List<ExchangeEvent> events) {
        int leavesQty = source.getOriginalVolume();
        int cumQty = 0;

        for (ExchangeEvent event : events) {
            if (event instanceof OrderAccepted accepted && accepted.orderId().equals(source.getId())) {
                leavesQty = accepted.order().leavesQty();
                cumQty = accepted.order().cumQty();
            } else if (event instanceof OrderExecuted executed && executed.orderId().equals(source.getId())) {
                leavesQty = executed.leavesQty();
                cumQty = executed.cumQty();
            }
        }

        return new TradableDTO(source.getId(), source.getUser(), source.getProduct(), source.getPrice(),
                source.getSide(), source.getOriginalVolume(), leavesQty, 0, cumQty);
    }

    private TradableDTO dtoAfterCancel(TradableDTO source, List<ExchangeEvent> events) {
        int cancelledQty = source.remainingVolume;
        for (ExchangeEvent event : events) {
            if (event instanceof exchange.model.OrderCancelled cancelled && cancelled.orderId().equals(source.id)) {
                cancelledQty = cancelled.cancelledQty();
            }
        }
        return new TradableDTO(source.id, source.user, source.product, source.price, source.side,
                source.originalVolume, 0, cancelledQty, source.filledVolume);
    }

    @Override
    public String toString() {
        for (ProductBook pb : pbooks.values()) {
            logger.debug(pb.toString());
        }

        return "";
    }
}
