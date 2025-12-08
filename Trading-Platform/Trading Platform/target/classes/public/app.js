document.addEventListener('DOMContentLoaded', () => {
    const startBtn = document.getElementById('start-btn');
    const stopBtn = document.getElementById('stop-btn');
    const statusDot = document.getElementById('status-dot');
    const statusText = document.getElementById('system-status');
    const logContainer = document.getElementById('log-container');
    const marketContainer = document.getElementById('market-container');
    const logFilter = document.getElementById('log-filter');

    // State
    let priceHistory = {}; // { symbol: [price1, price2, ...] }
    let previousPrices = {}; // { symbol: price }
    let allLogs = [];

    // Event Listeners
    startBtn.addEventListener('click', () => {
        fetch('/api/simulation/start', { method: 'POST' })
            .then(res => res.json())
            .then(() => updateStatus(true))
            .catch(err => console.error('Error starting:', err));
    });

    stopBtn.addEventListener('click', () => {
        fetch('/api/simulation/stop', { method: 'POST' })
            .then(res => res.json())
            .then(() => updateStatus(false))
            .catch(err => console.error('Error stopping:', err));
    });

    logFilter.addEventListener('input', (e) => {
        renderLogs(e.target.value);
    });

    // Polling
    setInterval(checkStatus, 2000);
    setInterval(loadMarketData, 1000);
    setInterval(loadTradeLogs, 1000);

    // Initial Load
    loadMarketData();
    loadTradeLogs();
    checkStatus();

    function updateStatus(isRunning) {
        if (isRunning) {
            statusText.textContent = 'System Online';
            statusDot.classList.add('online');
        } else {
            statusText.textContent = 'System Offline';
            statusDot.classList.remove('online');
        }
    }

    function checkStatus() {
        fetch('/api/status')
            .then(res => res.json())
            .then(data => updateStatus(data.running))
            .catch(() => {
                statusText.textContent = 'Connection Lost';
                statusDot.classList.remove('online');
            });
    }

    function loadMarketData() {
        fetch('/api/market-data')
            .then(res => res.json())
            .then(data => {
                if (data && data.length > 0) {
                    renderMarketCards(data);
                }
            })
            .catch(console.error);
    }

    function renderMarketCards(data) {
        // Sort by symbol
        data.sort((a, b) => a.symbol.localeCompare(b.symbol));

        // Create cards if they don't exist, update if they do
        data.forEach(item => {
            let card = document.getElementById(`card-${item.symbol}`);
            
            // Parse price
            const currentPrice = parseFloat(item.bid.replace('$', '')) || 0;
            const prevPrice = previousPrices[item.symbol] || currentPrice;
            
            // Update history for sparkline
            if (!priceHistory[item.symbol]) priceHistory[item.symbol] = [];
            if (currentPrice > 0) {
                priceHistory[item.symbol].push(currentPrice);
                if (priceHistory[item.symbol].length > 20) priceHistory[item.symbol].shift();
            }

            // Calculate change
            const change = currentPrice - prevPrice;
            const changeClass = change >= 0 ? 'flash-up' : 'flash-down';
            
            if (!card) {
                card = createCardElement(item);
                marketContainer.appendChild(card);
            }

            // Update values
            const priceEl = card.querySelector('.current-price');
            if (currentPrice !== prevPrice) {
                priceEl.textContent = item.bid;
                priceEl.classList.remove('flash-up', 'flash-down');
                void priceEl.offsetWidth; // trigger reflow
                priceEl.classList.add(changeClass);
            }

            // Update Bid/Ask
            card.querySelector('.bid-val').textContent = item.bid;
            card.querySelector('.bid-vol').textContent = item.bidVol;
            card.querySelector('.ask-val').textContent = item.ask;
            card.querySelector('.ask-vol').textContent = item.askVol;

            // Draw Sparkline
            drawSparkline(card.querySelector('canvas'), priceHistory[item.symbol]);

            previousPrices[item.symbol] = currentPrice;
        });

        // Remove loading state if present
        const loading = marketContainer.querySelector('.loading-state');
        if (loading) loading.remove();
    }

    function createCardElement(item) {
        const div = document.createElement('div');
        div.className = 'market-card';
        div.id = `card-${item.symbol}`;
        div.innerHTML = `
            <div class="card-header">
                <div class="symbol-info">
                    <h3>${item.symbol}</h3>
                    <div class="company-name">Nasdaq Listed</div>
                </div>
                <div class="price-info">
                    <div class="current-price">${item.bid}</div>
                    <div class="price-change">
                        <span>+0.00%</span>
                    </div>
                </div>
            </div>
            <div class="sparkline-container">
                <canvas class="sparkline" width="240" height="60"></canvas>
            </div>
            <div class="market-depth">
                <div class="depth-col">
                    <div class="depth-label">Bid</div>
                    <div class="depth-value bid bid-val">${item.bid}</div>
                    <div class="depth-vol">Vol: <span class="bid-vol">${item.bidVol}</span></div>
                </div>
                <div class="depth-col ask">
                    <div class="depth-label">Ask</div>
                    <div class="depth-value ask ask-val">${item.ask}</div>
                    <div class="depth-vol">Vol: <span class="ask-vol">${item.askVol}</span></div>
                </div>
            </div>
        `;
        return div;
    }

    function drawSparkline(canvas, data) {
        const ctx = canvas.getContext('2d');
        const width = canvas.width;
        const height = canvas.height;
        
        ctx.clearRect(0, 0, width, height);
        
        if (data.length < 2) return;

        const min = Math.min(...data);
        const max = Math.max(...data);
        const range = max - min || 1;

        ctx.beginPath();
        ctx.strokeStyle = data[data.length - 1] >= data[0] ? '#00e676' : '#ff5252';
        ctx.lineWidth = 2;
        ctx.lineJoin = 'round';

        data.forEach((val, i) => {
            const x = (i / (data.length - 1)) * width;
            const y = height - ((val - min) / range) * (height - 10) - 5;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });

        ctx.stroke();
        
        // Gradient fill
        const gradient = ctx.createLinearGradient(0, 0, 0, height);
        gradient.addColorStop(0, ctx.strokeStyle + '40'); // 40 hex = 25% opacity
        gradient.addColorStop(1, ctx.strokeStyle + '00'); // 0% opacity
        
        ctx.lineTo(width, height);
        ctx.lineTo(0, height);
        ctx.fillStyle = gradient;
        ctx.fill();
    }

    function loadTradeLogs() {
        fetch('/api/trade-log')
            .then(res => res.json())
            .then(logs => {
                allLogs = logs;
                renderLogs(logFilter.value);
            })
            .catch(console.error);
    }

    function renderLogs(filterText) {
        const filter = filterText.toLowerCase();
        const filteredLogs = allLogs.filter(log => log.toLowerCase().includes(filter));
        
        // Take last 50 logs and reverse
        const logsToShow = filteredLogs.slice(-50).reverse();

        logContainer.innerHTML = logsToShow.map(log => {
            const timeMatch = log.match(/\[(.*?)\]/);
            const time = timeMatch ? timeMatch[1] : new Date().toLocaleTimeString();
            const cleanLog = log.replace(/\[.*?\]/, '').trim();
            
            let type = 'INFO';
            let className = 'system';
            
            if (cleanLog.includes('BUY')) { type = 'BUY'; className = 'buy'; }
            else if (cleanLog.includes('SELL')) { type = 'SELL'; className = 'sell'; }
            else if (cleanLog.includes('Simulation')) { type = 'SYSTEM'; className = 'system'; }

            return `
                <div class="log-entry ${className}">
                    <span class="log-time">${time}</span>
                    <span class="log-type">${type}</span>
                    <span class="log-msg">${cleanLog}</span>
                </div>
            `;
        }).join('');
    }
});
