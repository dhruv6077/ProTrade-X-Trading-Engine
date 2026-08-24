import { chromium } from "/Users/dhruvpatel/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs";
import { pathToFileURL } from "node:url";
import path from "node:path";
import fs from "node:fs/promises";

const root = path.resolve(import.meta.dirname, "..");
const html = path.join(root, "docs/assets/vision-trader-architecture-linkedin.html");
const png = path.join(root, "docs/assets/vision-trader-architecture-linkedin.png");
const videoDir = path.join(root, "target/linkedin-architecture-video");

await fs.mkdir(videoDir, { recursive: true });

const browser = await chromium.launch({
  headless: true,
  executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
});
const staticContext = await browser.newContext({
  viewport: { width: 1200, height: 1500 },
  deviceScaleFactor: 1
});
const staticPage = await staticContext.newPage();
await staticPage.goto(pathToFileURL(html).href);
await staticPage.evaluate(() => document.fonts.ready);
await staticPage.waitForTimeout(300);
await staticPage.screenshot({ path: png, fullPage: false });
await staticContext.close();

const videoContext = await browser.newContext({
  viewport: { width: 1200, height: 1500 },
  deviceScaleFactor: 1,
  recordVideo: { dir: videoDir, size: { width: 1200, height: 1500 } }
});
const page = await videoContext.newPage();
await page.goto(pathToFileURL(html).href);
await page.evaluate(() => document.fonts.ready);
await page.waitForTimeout(8000);
const video = page.video();
await page.close();
const webm = await video.path();
await videoContext.close();
await browser.close();

console.log(JSON.stringify({ png, webm }));
