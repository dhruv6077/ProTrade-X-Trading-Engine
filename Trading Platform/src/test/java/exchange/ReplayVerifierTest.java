package exchange;

import exchange.clearing.ClearingService;
import exchange.core.DeterministicMatchingEngine;
import exchange.core.Sequencer;
import exchange.journal.CommandJournal;
import exchange.model.ExchangeEvent;
import exchange.model.OrderCommand;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.risk.InMemoryRiskEngine;
import exchange.risk.RiskProfile;
import exchange.ExchangeTestSupport.TestExchange;
import Price.Price;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

public class ReplayVerifierTest {

    @Test
    public void testReplayMatchesSourceExactly() {
        List<OrderCommand> journalReplay = new ArrayList<>();
        CommandJournal sharedJournal = new CommandJournal() {
            @Override
            public void append(OrderCommand command) {
                if (command instanceof NewOrderCommand no) {
                    journalReplay.add(no);
                } else if (command instanceof MutableOrderCommand mo) {
                    journalReplay.add(new NewOrderCommand(mo.sequenceNumber(), mo.orderId(), mo.clientId(), mo.symbol(), mo.side(), mo.orderType(), mo.price(), mo.quantity(), mo.stpMode()));
                }
            }
            @Override public List<OrderCommand> replay() { return journalReplay; }
            @Override public long size() { return journalReplay.size(); }
            @Override public long totalAppended() { return journalReplay.size(); }
            @Override public void close() {}
        };
        
        TestExchange sourceExchange = ExchangeTestSupport.newExchange(Set.of("AAPL"), new Sequencer(1, Clock.systemUTC()), sharedJournal);

        sourceExchange.riskEngine().setPosition("SELLER1", "AAPL", 1000);
        sourceExchange.riskEngine().setProfile("BUYER1", new RiskProfile(10_000_000_000L, 10_000_000, 10_000_000_000L, false));
        sourceExchange.riskEngine().setAvailableCash("BUYER1", 10_000_000_000L);
        sourceExchange.riskEngine().setShortSellingEnabled("SELLER1", true);

        int numOrders = 1000;
        for (int i = 0; i < numOrders; i++) {
            sourceExchange.gateway().submit(ExchangeTestSupport.limit(
                    "SELL" + i, "SELLER1", "AAPL", exchange.model.Side.SELL,
                    new Price(100), 10));
            sourceExchange.gateway().submit(ExchangeTestSupport.limit(
                    "BUY" + i, "BUYER1", "AAPL", exchange.model.Side.BUY,
                    new Price(100 + (i % 10)), 10));
        }
        
        sourceExchange.close();

        System.out.println("SOURCE CASH: " + sourceExchange.riskEngine().account("BUYER1").availableCashCents());

        DeterministicMatchingEngine replayEngine = new DeterministicMatchingEngine(Set.of("AAPL"));
        InMemoryRiskEngine replayRisk = new InMemoryRiskEngine(new RiskProfile(10_000_000_000L, 10_000_000, 10_000_000_000L, false));
        ClearingService replayClearing = new ClearingService(replayRisk, true);

        replayRisk.setPosition("SELLER1", "AAPL", 1000);
        replayRisk.setProfile("BUYER1", new RiskProfile(10_000_000_000L, 10_000_000, 10_000_000_000L, false));
        replayRisk.setAvailableCash("BUYER1", 10_000_000_000L);
        replayRisk.setShortSellingEnabled("SELLER1", true);

        for (OrderCommand cmd : journalReplay) {
            List<ExchangeEvent> events = replayEngine.process(cmd);
            replayClearing.onEvents(events);
        }

        assertEquals(sourceExchange.riskEngine().account("BUYER1").availableCashCents(), replayRisk.account("BUYER1").availableCashCents(), "BUYER1 availableCash mismatch after replay");
        assertEquals(sourceExchange.riskEngine().account("SELLER1").availableCashCents(), replayRisk.account("SELLER1").availableCashCents(), "SELLER1 availableCash mismatch after replay");
        
        assertEquals(sourceExchange.riskEngine().account("BUYER1").reservedCashCents(), replayRisk.account("BUYER1").reservedCashCents(), "BUYER1 reservedCash mismatch after replay");
    }
}
