package exchange;

import exchange.clearing.ClearingService;
import exchange.core.DeterministicMatchingEngine;
import exchange.core.Sequencer;
import exchange.dispatch.InMemoryEventDispatcher;
import exchange.gateway.GatewayValidator;
import exchange.gateway.OrderGateway;
import exchange.journal.InMemoryCommandJournal;
import exchange.marketdata.MarketDataEngine;
import exchange.model.NewOrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.risk.InMemoryRiskEngine;
import exchange.risk.RiskProfile;

import Price.Price;

import java.time.Clock;
import java.util.Set;

final class ExchangeTestSupport {
    private ExchangeTestSupport() {
    }

    static TestExchange newExchange(Set<String> symbols) {
        return newExchange(symbols, new Sequencer());
    }

    static TestExchange newExchange(Set<String> symbols, Sequencer sequencer) {
        return newExchange(symbols, sequencer, new InMemoryCommandJournal(4_096));
    }

    static TestExchange newExchange(Set<String> symbols, Sequencer sequencer, exchange.journal.CommandJournal customJournal) {
        InMemoryEventDispatcher dispatcher = new InMemoryEventDispatcher();
        InMemoryRiskEngine riskEngine = new InMemoryRiskEngine(new RiskProfile(10_000_000_000L, 10_000_000,
                10_000_000_000L, false));
        MarketDataEngine marketDataEngine = new MarketDataEngine();
        dispatcher.addListener(marketDataEngine);
        OrderGateway gateway = new OrderGateway(
                new GatewayValidator(symbols, 1, 1, 1_000_000),
                riskEngine,
                sequencer,
                customJournal,
                new DeterministicMatchingEngine(symbols),
                dispatcher);
        return new TestExchange(gateway, customJournal, dispatcher, riskEngine, marketDataEngine);
    }

    static TestExchange newExchange(Set<String> symbols, Clock clock) {
        return newExchange(symbols, new Sequencer(1, clock));
    }

    static NewOrderCommand limit(String orderId, String clientId, String symbol, Side side, Price price, int quantity) {
        return new NewOrderCommand(0, orderId, clientId, symbol, side, OrderType.LIMIT, price, quantity,
                SelfTradePreventionMode.CANCEL_NEWEST);
    }

    static NewOrderCommand market(String orderId, String clientId, String symbol, Side side, int quantity) {
        return new NewOrderCommand(0, orderId, clientId, symbol, side, OrderType.MARKET, null, quantity,
                SelfTradePreventionMode.CANCEL_NEWEST);
    }

    record TestExchange(
            OrderGateway gateway,
            exchange.journal.CommandJournal journal,
            InMemoryEventDispatcher dispatcher,
            InMemoryRiskEngine riskEngine,
            MarketDataEngine marketDataEngine) implements AutoCloseable {
        @Override
        public void close() {
            gateway.close();
            dispatcher.close();
            marketDataEngine.close();
            if (journal instanceof AutoCloseable c) {
                try {
                    c.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
