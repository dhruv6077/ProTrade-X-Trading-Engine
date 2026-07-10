document.addEventListener("DOMContentLoaded", () => {
	  const MAX_TRADE_ROWS = 60;
	  const MAX_LATENCY_POINTS = 120;
	  const SUBMIT_COOLDOWN_MS = 750;
	  const state = {
	    symbol: "",
	    side: "BUY",
	    clientId: "WEB_TRADER",
	    sources: new Map(),
	    trades: [],
	    latencyMicros: [],
	    submittingOrder: false,
	    lastSubmitAt: 0,
	  };

  const els = {
    connectionDot: document.getElementById("connection-dot"),
    connectionText: document.getElementById("connection-text"),
    startBtn: document.getElementById("start-btn"),
    stopBtn: document.getElementById("stop-btn"),
    symbolSelect: document.getElementById("symbol-select"),
    clientId: document.getElementById("client-id"),
    eventCount: document.getElementById("event-count"),
    journalSize: document.getElementById("journal-size"),
    bidLadder: document.getElementById("bid-ladder"),
    askLadder: document.getElementById("ask-ladder"),
    l2Asof: document.getElementById("l2-asof"),
    tradeTape: document.getElementById("trade-tape"),
    accountLabel: document.getElementById("account-id-label"),
    availableCash: document.getElementById("available-cash"),
    reservedCash: document.getElementById("reserved-cash"),
    positions: document.getElementById("positions"),
    orderForm: document.getElementById("order-form"),
    orderType: document.getElementById("order-type"),
    priceField: document.getElementById("price-field"),
    orderPrice: document.getElementById("order-price"),
	    orderQty: document.getElementById("order-qty"),
	    submitOrder: document.querySelector(".submit-order"),
	    formMessage: document.getElementById("form-message"),
    latencyChart: document.getElementById("latency-chart"),
    latencyValue: document.getElementById("latency-value"),
    throughput: document.getElementById("throughput"),
    shards: document.getElementById("shards"),
  };

  boot();

  async function boot() {
    wireControls();
    await loadSymbols();
    connectDiagnostics();
    connectAccount();
    connectMarketStreams();
    refreshStatus();
    setInterval(refreshStatus, 2500);
  }

  function wireControls() {
    els.orderForm.classList.add("buy");
    updateSideState();
    els.startBtn.addEventListener("click", () => postJson("/api/simulation/start", {}));
    els.stopBtn.addEventListener("click", () => postJson("/api/simulation/stop", {}));
    els.symbolSelect.addEventListener("change", () => {
      state.symbol = els.symbolSelect.value;
      connectMarketStreams();
    });
    els.clientId.addEventListener("change", () => {
      state.clientId = cleanClientId();
      connectAccount();
    });
    document.querySelectorAll(".side-button").forEach((button) => {
      button.addEventListener("click", () => {
        state.side = button.dataset.side;
        document.querySelectorAll(".side-button").forEach((item) => item.classList.remove("active"));
        button.classList.add("active");
        updateSideState();
      });
    });
    document.querySelectorAll("[data-step-target]").forEach((button) => {
      button.addEventListener("click", () => stepFinancialInput(button));
    });
    els.orderType.addEventListener("change", updatePriceField);
    els.orderPrice.addEventListener("input", validatePriceTick);
    els.orderForm.addEventListener("submit", submitOrder);
  }

  async function loadSymbols() {
    const symbols = await fetchJson("/api/symbols").catch(() => []);
    els.symbolSelect.innerHTML = symbols
      .map((symbol) => `<option value="${escapeHtml(symbol)}">${escapeHtml(symbol)}</option>`)
      .join("");
    state.symbol = symbols[0] || "AAPL";
    els.symbolSelect.value = state.symbol;
  }

  function connectMarketStreams() {
    if (!state.symbol) return;
    replaceSource("l2", `/api/market-data/l2/${encodeURIComponent(state.symbol)}`, {
      snapshot: (payload) => renderL2(payload),
    });
    replaceSource("trades", `/api/market-data/trades/${encodeURIComponent(state.symbol)}`, {
      trades: (payload) => {
        state.trades = payload.trades.slice(-MAX_TRADE_ROWS);
        renderTrades();
      },
    });
  }

  function connectAccount() {
    state.clientId = cleanClientId();
    els.accountLabel.textContent = state.clientId;
    replaceSource("account", `/api/accounts/${encodeURIComponent(state.clientId)}`, {
      account: renderAccount,
    });
  }

  function connectDiagnostics() {
    replaceSource("diagnostics", "/api/diagnostics/stream", {
      metrics: (payload) => {
        state.latencyMicros = payload.latencyMicros.slice(-MAX_LATENCY_POINTS);
        renderDiagnostics(payload);
      },
    });
  }

  function replaceSource(key, url, handlers) {
    const existing = state.sources.get(key);
    if (existing) existing.close();

    const source = new EventSource(url);
    Object.entries(handlers).forEach(([eventName, handler]) => {
      source.addEventListener(eventName, (event) => handler(JSON.parse(event.data)));
    });
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    state.sources.set(key, source);
  }

  function setConnected(connected) {
    els.connectionDot.classList.toggle("online", connected);
    els.connectionText.textContent = connected ? "Streaming" : "Reconnecting";
  }

  async function refreshStatus() {
    const status = await fetchJson("/api/status").catch(() => null);
    if (!status) {
      setConnected(false);
      return;
    }
    els.eventCount.textContent = status.eventCount ?? 0;
    els.journalSize.textContent = status.journalSize ?? 0;
  }

  function renderL2(snapshot) {
    els.l2Asof.textContent = `${snapshot.symbol} as of ${formatTime(snapshot.asOf)}`;
    els.bidLadder.innerHTML = renderLevels(snapshot.bids, "bid");
    els.askLadder.innerHTML = renderLevels(snapshot.asks, "ask");
  }

  function renderLevels(levels, side) {
    if (!levels.length) {
      return `<div class="empty-row">No ${side} liquidity</div>`;
    }
    return levels
      .map((level) => {
        return `
          <div class="ladder-row ${side}">
            <span class="price">${escapeHtml(level.price)}</span>
            <span class="qty">${level.quantity.toLocaleString()}</span>
            <span class="cum">${level.cumulativeQuantity.toLocaleString()}</span>
          </div>
        `;
      })
      .join("");
  }

  function renderTrades() {
    els.tradeTape.innerHTML = state.trades
      .slice()
      .reverse()
      .map((trade) => {
        const sideClass = trade.takerSide === "BUY" ? "bid-text" : "ask-text";
        return `
          <tr>
            <td>${formatTime(trade.timestamp)}</td>
            <td class="${sideClass}">${trade.takerSide}</td>
            <td>${escapeHtml(trade.price)}</td>
            <td>${trade.quantity.toLocaleString()}</td>
          </tr>
        `;
      })
      .join("");
  }

	  function renderAccount(account) {
	    els.availableCash.textContent = account.availableCash;
	    els.reservedCash.textContent = account.reservedCash;
	    const positions = Object.entries(account.positions || {});
	    els.positions.innerHTML = positions.length
	      ? positions
	          .map(([symbol, qty]) => {
	            const reserved = Number((account.reservedPositions || {})[symbol] || 0);
	            const available = Number((account.availablePositions || {})[symbol] ?? Math.max(0, qty - reserved));
	            return `
	              <div class="position-row">
	                <span>${escapeHtml(symbol)}</span>
	                <strong>${Number(qty).toLocaleString()} total</strong>
	                <small>${available.toLocaleString()} available · ${reserved.toLocaleString()} reserved</small>
	              </div>
	            `;
	          })
	          .join("")
	      : `<div><span>No positions</span><strong>0</strong></div>`;
	  }

  function renderDiagnostics(payload) {
    const latest = state.latencyMicros[state.latencyMicros.length - 1] || 0;
    els.latencyValue.textContent = `${latest.toLocaleString()} us`;
    drawLatencyChart();

    const throughput = Object.entries(payload.ordersPerSecondBySymbol || {});
    els.throughput.innerHTML = throughput.length
      ? throughput
          .map(([symbol, value]) => `<div><span>${escapeHtml(symbol)}</span><strong>${value}/s</strong></div>`)
          .join("")
      : `<div><span>No flow yet</span><strong>0/s</strong></div>`;

    els.shards.innerHTML = payload.shards
      .map(
        (shard) => `
          <div class="shard-row">
            <span class="shard-light ${shard.running ? "online" : ""}"></span>
            <span>${escapeHtml(shard.symbol)}</span>
            <strong>${shard.queuedCommands} queued</strong>
          </div>
        `
      )
      .join("");
  }

  function drawLatencyChart() {
    const canvas = els.latencyChart;
    const ctx = canvas.getContext("2d");
    const width = canvas.width;
    const height = canvas.height;
    ctx.clearRect(0, 0, width, height);
    ctx.strokeStyle = "rgba(255,255,255,0.08)";
    ctx.beginPath();
    for (let y = 20; y < height; y += 30) {
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
    }
    ctx.stroke();

    if (state.latencyMicros.length < 2) return;
    const max = Math.max(1, ...state.latencyMicros);
    ctx.beginPath();
    ctx.strokeStyle = "#38bdf8";
    ctx.lineWidth = 2;
    state.latencyMicros.forEach((value, index) => {
      const x = (index / (MAX_LATENCY_POINTS - 1)) * width;
      const y = height - 10 - (value / max) * (height - 20);
      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }

	  async function submitOrder(event) {
	    event.preventDefault();
	    const now = Date.now();
	    if (state.submittingOrder || now - state.lastSubmitAt < SUBMIT_COOLDOWN_MS) {
	      showMessage("Order is already being submitted. Please wait a moment before sending another order.", false);
	      return;
	    }
	    const validation = validateForm();
	    if (!validation.ok) {
	      showMessage(validation.message, false);
	      return;
	    }
	    state.submittingOrder = true;
	    state.lastSubmitAt = now;
	    if (els.submitOrder) els.submitOrder.disabled = true;

	    const orderType = els.orderType.value;
	    const payload = {
      clientId: cleanClientId(),
      symbol: state.symbol,
      side: state.side,
      orderType,
      price: orderType === "MARKET" ? null : els.orderPrice.value.trim(),
      quantity: Number.parseInt(els.orderQty.value, 10),
    };

	    const result = await postJson("/api/orders", payload).catch((error) => ({
	      accepted: false,
	      message: error.message,
	    }));
	    showMessage(result.message, result.accepted);
	    window.setTimeout(() => {
	      state.submittingOrder = false;
	      if (els.submitOrder) els.submitOrder.disabled = false;
	    }, SUBMIT_COOLDOWN_MS);
	  }

  function validateForm() {
    const quantity = Number.parseInt(els.orderQty.value, 10);
    if (!Number.isInteger(quantity) || quantity <= 0) {
      return { ok: false, message: "Quantity must be a positive whole number." };
    }
    if (els.orderType.value !== "MARKET") {
      return validatePriceTick();
    }
    return { ok: true, message: "" };
  }

  function validatePriceTick() {
    if (els.orderType.value === "MARKET") {
      return { ok: true, message: "" };
    }
    const raw = els.orderPrice.value.trim();
    if (!/^\d+(\.\d{1,2})?$/.test(raw)) {
      return { ok: false, message: "Price must use a one-cent tick, e.g. 100.25." };
    }
    return { ok: true, message: "" };
  }

  function updatePriceField() {
    const isMarket = els.orderType.value === "MARKET";
    els.priceField.classList.toggle("disabled", isMarket);
    els.orderPrice.disabled = isMarket;
    if (isMarket) els.orderPrice.value = "";
  }

  function updateSideState() {
    const isBuy = state.side === "BUY";
    els.orderForm.classList.toggle("buy", isBuy);
    els.orderForm.classList.toggle("sell", !isBuy);
    const label = isBuy ? "Buy" : "Sell";
    els.orderForm.querySelector(".submit-order").textContent = `Submit ${label} to Gateway`;
  }

  function stepFinancialInput(button) {
    const input = document.getElementById(button.dataset.stepTarget);
    if (!input || input.disabled) return;
    const step = Number.parseFloat(button.dataset.step);
    const current = Number.parseFloat(input.value || "0");
    const next = Math.max(0, current + step);
    input.value = input.id === "order-price" ? next.toFixed(2) : String(Math.max(1, Math.round(next)));
    input.dispatchEvent(new Event("input", { bubbles: true }));
  }

  function showMessage(message, accepted) {
    els.formMessage.textContent = message;
    els.formMessage.className = `form-message ${accepted ? "success" : "error"}`;
  }

  async function fetchJson(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return response.json();
  }

  async function postJson(url, body) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await response.json();
    if (!response.ok && !data.message) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return data;
  }

  function cleanClientId() {
    const value = els.clientId.value.trim() || "WEB_TRADER";
    els.clientId.value = value;
    return value;
  }

  function formatTime(value) {
    if (!value) return "--:--:--";
    return new Date(value).toLocaleTimeString([], {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }
});
