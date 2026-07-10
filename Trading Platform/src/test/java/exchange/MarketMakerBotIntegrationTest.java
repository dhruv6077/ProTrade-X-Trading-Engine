package exchange;

import exchange.marketdata.L2Snapshot;
import exchange.ws.NettyWebSocketServer;
import org.junit.jupiter.api.Test;
import sim.MarketMakerBot;
import sim.MarketMakerConfig;

import java.time.Duration;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MarketMakerBotIntegrationTest {

    @Test
    void marketMakerKeepsTwoSidedBookOverWebSocket() throws Exception {
        try (ExchangeTestSupport.TestExchange exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"))) {
            exchange.riskEngine().setAvailableCash("MM_TST", 100_000_000L);
            exchange.riskEngine().setPosition("MM_TST", "AAPL", 1_000_000L);

            NettyWebSocketServer server = new NettyWebSocketServer(
                    exchange.gateway(),
                    exchange.dispatcher(),
                    exchange.marketDataEngine(),
                    0);
            server.start();

            MarketMakerBot bot = new MarketMakerBot(MarketMakerConfig.builder()
                    .clientId("MM_TST")
                    .symbols(java.util.List.of("AAPL"))
                    .halfSpreadCents(5L)
                    .orderSize(25)
                    .defaultMidPriceCents(10_000L)
                    .webSocketPort(server.port())
                    .build());
            try {
                bot.start();

                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                    L2Snapshot snapshot = exchange.marketDataEngine().l2Snapshot("AAPL", 1);
                    assertFalse(snapshot.bids().isEmpty(), "expected bid liquidity from market maker");
                    assertFalse(snapshot.asks().isEmpty(), "expected ask liquidity from market maker");
                });
            } finally {
                bot.close();
                server.close();
            }
        }
    }
}
