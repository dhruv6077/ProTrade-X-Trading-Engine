# ProTrade-X-Trading-Engine
High-performance, enterprise-grade trading platform engine in Java. Features multithreaded order matching, audit logging, dual persistence, and real-time market simulation.

> _My personal explanation of the Trading Platform I built._

## 🚀 What is this project?

My Trading Platform is a **simulation of a real-world stock exchange** like NASDAQ or the NYSE.

I designed it so automated bots can **buy** and **sell** stocks, and the system handles the core responsibilities of an actual exchange:

1. **Receive Orders** — for example: “Buy 10 shares of Apple at $150.”
2. **Match Orders** — find a seller whose price matches the buyer.
3. **Execute Trades** — complete the transaction securely.
4. **Record Everything** — store all actions in a tamper-evident audit log.

---

## 🔑 Key Concepts & Examples

### 1. 🧠 The Matching Engine (The “Brain”)

The matching engine is the heart of my system. It decides which buyer and seller should be matched.

**Analogy:**  
It’s like a bulletin board where people post what they want to buy or sell.

- Buyer: “Buying iPhone for $500.”
- Seller A: “Selling for $550.” → No deal.
- Seller B: “Selling for $500.” → **Match!**

**In my code:**  
I maintain two lists inside the `ProductBook`:

- **Bids** (buyers)
- **Asks** (sellers)

The engine constantly looks for a matching pair and executes the trade instantly.

---

### 2. ⚡ Multithreading (Handling Many Tasks at Once)

A real exchange handles millions of trades per second, so I built my system to process multiple operations concurrently.

**Analogy:**  
One checkout lane vs. ten checkout lanes.

- **Single-threaded:** slow, everything waits.
- **Multithreaded:** fast, lots happening at once.

**In my code:**  
I use Java threads and **ReentrantReadWriteLock** to prevent race conditions.  
This ensures two bots can't modify the same order at the same time.

---

### 3. 🛡️ Non-Repudiation Audit Logging (The “Black Box”)

Every trade must be recorded in a way that **cannot be altered**.

**Analogy:**  
A blockchain-style log where each entry contains the hash of the previous one.  
If someone changes one entry, the entire chain breaks.

**In my code:**

- Every log entry gets a **cryptographic hash**.
- That hash is combined with the previous log's hash.
- This forms a linked chain of fingerprints.

If any log is modified, the hashes no longer match, and tampering is obvious.

---

### 4. 🗄️ Database & Persistence

I save all logs and trades so they are never lost.

- I use **PostgreSQL** for durable storage.
- If the database is down, the system automatically switches to local file logging (failover).

This ensures I never lose critical financial data.

---

## 🛠️ Tech Stack Summary

- **Language:** Java
- **Database:** PostgreSQL
- **Concurrency:** ExecutorService, ReentrantReadWriteLock
- **Design Patterns:**
  - Singleton (Managers)
  - Observer (Market Updates)
  - Factory (Object Creation)

---

## ▶️ How I Run the Trading Platform

I run the system either with automated scripts.

```bash
./install_dependencies.sh
./run_pro.sh
```

---

## 🎯 Why I Built This

I created this project to learn how real exchanges work internally—
including order matching, concurrency, audit trails, and system design.
This project helped me deepen my understanding of:

- High-performance systems
- Multithreading
- Financial market structure
- Cryptographic hash chains
- Backend architecture

