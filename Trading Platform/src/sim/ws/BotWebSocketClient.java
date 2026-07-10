package sim.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Lightweight WebSocket client for algorithmic bots connecting to the Netty ECN API.
 */
public final class BotWebSocketClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BotWebSocketClient.class);

    private final URI uri;
    private final Consumer<String> messageHandler;
    private final HttpClient httpClient;
    private volatile WebSocket webSocket;
    private volatile boolean open;
    private volatile boolean closing;

    public BotWebSocketClient(URI uri, Consumer<String> messageHandler) {
        this.uri = Objects.requireNonNull(uri, "uri");
        this.messageHandler = Objects.requireNonNull(messageHandler, "messageHandler");
        this.httpClient = HttpClient.newHttpClient();
    }

    public void connect(Duration timeout) throws InterruptedException {
        closing = false;
        CompletableFuture<WebSocket> future = httpClient.newWebSocketBuilder()
                .connectTimeout(timeout)
                .buildAsync(uri, new Listener());
        webSocket = future.join();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!open && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        if (!open) {
            throw new IllegalStateException("WebSocket did not open in time: " + uri);
        }
    }

    public void send(String payload) {
        WebSocket socket = webSocket;
        if (socket == null || !open) {
            throw new IllegalStateException("WebSocket is not connected: " + uri);
        }
        socket.sendText(payload, true);
    }

    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        closing = true;
        open = false;
        WebSocket socket = webSocket;
        if (socket != null) {
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "bot-shutdown").join();
            } catch (RuntimeException e) {
                logger.debug("WebSocket already closed for {}", uri, e);
            } finally {
                webSocket = null;
            }
        }
    }

    private final class Listener implements WebSocket.Listener {
        private final StringBuilder textBuffer = new StringBuilder(4096);

        @Override
        public void onOpen(WebSocket socket) {
            closing = false;
            open = true;
            socket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            textBuffer.append(data);
            if (last) {
                String message = textBuffer.toString();
                textBuffer.setLength(0);
                try {
                    if (!message.isBlank()) {
                        messageHandler.accept(message);
                    }
                } catch (Exception e) {
                    logger.warn("Bot message handler failed for {}", uri, e);
                }
            }
            socket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
            open = false;
            closing = false;
            textBuffer.setLength(0);
            return null;
        }

        @Override
        public void onError(WebSocket socket, Throwable error) {
            open = false;
            textBuffer.setLength(0);
            if (closing) {
                logger.debug("WebSocket closed during shutdown for {}", uri, error);
            } else {
                logger.warn("WebSocket error on {}", uri, error);
            }
        }
    }
}
