package web;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import sim.SimulationManager;
import User.ProductManager;
import Market.CurrentMarketObserver;
import Market.CurrentMarketPublisher;
import Market.CurrentMarketSide;
import com.google.gson.Gson;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WebDashboard {
    private static Javalin app;
    private static final Gson gson = new Gson();
    private static final Queue<String> tradeLogs = new ConcurrentLinkedQueue<>();
    private static final int MAX_LOGS = 100;
    private static final Map<String, Map<String, String>> latestMarketData = new ConcurrentHashMap<>();

    private static final CurrentMarketObserver marketObserver = new CurrentMarketObserver() {
        @Override
        public void updateCurrentMarket(String symbol, CurrentMarketSide buySide, CurrentMarketSide sellSide) {
            Map<String, String> data = new HashMap<>();
            data.put("symbol", symbol);
            data.put("bid", buySide.getPrice().toString());
            data.put("bidVol", String.valueOf(buySide.getVolume()));
            data.put("ask", sellSide.getPrice().toString());
            data.put("askVol", String.valueOf(sellSide.getVolume()));
            data.put("last", "N/A");
            latestMarketData.put(symbol, data);
        }
    };

    public static void start() {
        if (app != null) {
            return;
        }

        // Initialize simulation (loads products and bots)
        SimulationManager.getInstance().initialize();

        app = Javalin.create(config -> {
            config.staticFiles.add("/public", Location.CLASSPATH);
        }).start(8080);

        // Subscribe to market updates
        try {
            ArrayList<String> products = ProductManager.getInstance().getProductList();
            for (String symbol : products) {
                CurrentMarketPublisher.getInstance().subscribeCurrentMarket(symbol, marketObserver);
                // Initialize with placeholders
                if (!latestMarketData.containsKey(symbol)) {
                    Map<String, String> initialData = new HashMap<>();
                    initialData.put("symbol", symbol);
                    initialData.put("bid", "N/A");
                    initialData.put("bidVol", "0");
                    initialData.put("ask", "N/A");
                    initialData.put("askVol", "0");
                    initialData.put("last", "N/A");
                    latestMarketData.put(symbol, initialData);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // API Endpoints
        app.get("/api/status", ctx -> {
            Map<String, Object> status = new HashMap<>();
            status.put("running", true);
            status.put("message", "System Operational");
            status.put("timestamp", System.currentTimeMillis());
            ctx.contentType("application/json").result(gson.toJson(status));
        });

        app.get("/api/market-data", ctx -> {
            ctx.contentType("application/json").result(gson.toJson(latestMarketData.values()));
        });

        app.get("/api/trade-log", ctx -> {
            ctx.contentType("application/json").result(gson.toJson(new ArrayList<>(tradeLogs)));
        });

        app.post("/api/simulation/start", ctx -> {
            SimulationManager.getInstance().start();
            addLog("System: Simulation started");
            ctx.contentType("application/json").result(gson.toJson(Map.of("status", "started")));
        });

        app.post("/api/simulation/stop", ctx -> {
            SimulationManager.getInstance().stop();
            addLog("System: Simulation stopped");
            ctx.contentType("application/json").result(gson.toJson(Map.of("status", "stopped")));
        });

        System.out.println("Web Dashboard started at http://localhost:8080");
    }

    public static void addLog(String message) {
        tradeLogs.add(message);
        if (tradeLogs.size() > MAX_LOGS) {
            tradeLogs.poll();
        }
    }

    public static void stop() {
        if (app != null) {
            app.stop();
            app = null;

            try {
                ArrayList<String> products = ProductManager.getInstance().getProductList();
                for (String symbol : products) {
                    CurrentMarketPublisher.getInstance().unSubscribeCurrentMarket(symbol, marketObserver);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
