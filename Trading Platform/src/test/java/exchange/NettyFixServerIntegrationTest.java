package exchange;

import exchange.fix.NettyFixServer;
import exchange.ExchangeTestSupport.TestExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyFixServerIntegrationTest {

    private TestExchange exchange;
    private NettyFixServer fixServer;

    @BeforeEach
    void setUp() {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        fixServer = new NettyFixServer(exchange.gateway(), 64481);
        exchange.dispatcher().addListener(fixServer);
    }

    @AfterEach
    void tearDown() {
        if (fixServer != null) {
            fixServer.close();
        }
        if (exchange != null) {
            exchange.close();
        }
    }

    private String formatFixMessage(String msgType, String bodyStr) {
        String body = "35=" + msgType + "\u0001" + bodyStr;
        int bodyLength = body.length();
        String header = "8=FIX.4.4\u00019=" + bodyLength + "\u0001";
        
        String withoutChecksum = header + body;
        int sum = 0;
        for (int i = 0; i < withoutChecksum.length(); i++) {
            sum += withoutChecksum.charAt(i);
        }
        sum %= 256;
        String checksum = String.format("10=%03d\u0001", sum);
        return withoutChecksum + checksum;
    }

    @Test
    void testLogonAndNewOrder() throws Exception {
        try (Socket socket = new Socket("localhost", 64481)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            String logon = formatFixMessage("A", "49=TESTCLIENT\u0001");
            out.write(logon.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            assertTrue(bytesRead > 0);
            String logonReply = new String(buffer, 0, bytesRead, StandardCharsets.US_ASCII);
            assertTrue(logonReply.contains("35=A"));

            exchange.riskEngine().setProfile("TESTCLIENT", new exchange.risk.RiskProfile(100000000L, 1000, 100000000L, false));
            exchange.riskEngine().setAvailableCash("TESTCLIENT", 100000000L);

            String newOrder = formatFixMessage("D", "11=ORDER1\u000149=TESTCLIENT\u000155=AAPL\u000154=1\u000138=10\u000144=150.00\u000140=2\u0001");
            out.write(newOrder.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            bytesRead = in.read(buffer);
            assertTrue(bytesRead > 0);
            String execReport = new String(buffer, 0, bytesRead, StandardCharsets.US_ASCII);
            System.out.println("RECEIVED: " + execReport);
            
            assertTrue(execReport.contains("35=8"));
            assertTrue(execReport.contains("39=0") || execReport.contains("39=1") || execReport.contains("39=2"));
            assertTrue(execReport.contains("11=ORDER1"));
        }
    }
}
