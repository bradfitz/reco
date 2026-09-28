// Optional real-browser/process regression. WEBDEMO_BIN points to a built
// cmd/webdemo binary. This test owns two child processes on temporary ports.
const {test} = require("node:test");
const assert = require("node:assert/strict");
const {spawn} = require("node:child_process");
const net = require("node:net");
const {chromium} = require("playwright");

async function unusedPort() {
  const s = net.createServer();
  await new Promise((resolve, reject) => { s.once("error", reject); s.listen(0, "127.0.0.1", resolve); });
  const port = s.address().port;
  await new Promise((resolve) => s.close(resolve));
  return port;
}

test("two processes: authority, deltas, Tx, partition, and restart", {timeout: 60000}, async () => {
  assert.ok(process.env.WEBDEMO_BIN, "set WEBDEMO_BIN to a built webdemo binary");
  const pa = await unusedPort(), pb = await unusedPort();
  const urls = {a: `http://127.0.0.1:${pa}`, b: `http://127.0.0.1:${pb}`};
  const children = new Set();
  async function start(id) {
    const child = spawn(process.env.WEBDEMO_BIN, ["-listen", `127.0.0.1:${id === "a" ? pa : pb}`, "-peer-id", id, "-peer-url", urls[id === "a" ? "b" : "a"]]);
    children.add(child);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("server startup timed out")), 5000);
      child.once("error", reject);
      child.once("exit", (code) => { clearTimeout(timer); reject(new Error(`early server exit ${code}`)); });
      child.stderr.on("data", (data) => { if (String(data).includes("reco playground")) { clearTimeout(timer); resolve(); } });
    });
    return child;
  }
  async function stop(child) {
    if (child.exitCode !== null || child.signalCode !== null) { children.delete(child); return; }
    await new Promise((resolve) => { child.once("exit", resolve); child.kill("SIGTERM"); });
    children.delete(child);
  }
  let browser;
  try {
    await start("a");
    browser = await chromium.launch({executablePath: process.env.CHROMIUM_PATH || undefined, headless: true, args: ["--no-sandbox"]});
    const context = await browser.newContext({viewport: {width: 1440, height: 1000}});
    const a = await context.newPage(), b = await context.newPage();
    const errors = [], frames = [];
    for (const page of [a, b]) {
      page.on("pageerror", (error) => errors.push(error.message));
      page.on("websocket", (ws) => ws.on("framereceived", ({payload}) => {
        const m = JSON.parse(String(payload));
        if (m.type === "peer-wire") frames.push(JSON.parse(m.wire.data));
      }));
    }
    const summary = async (page, text) => page.waitForFunction((text) => document.getElementById("value-summary").textContent === text, text);
    const live = async (page) => page.waitForFunction(() => document.getElementById("status").textContent === "Live");
    await a.goto(urls.a); await live(a);
    assert.equal(await a.locator(".intro, .experiment, #reset").count(), 0);
    assert.match(await a.locator("#value-summary").textContent(), /Waiting for remote/);
    assert.equal(await a.locator("#peer-stale").isVisible(), true);
    await a.locator("#label").fill("Two gardens"); await a.locator("#label").press("Enter");
    let serverB = await start("b");
    await b.goto(urls.b); await live(b);
    assert.equal(await b.locator(".intro, .experiment, #reset").count(), 0);
    for (const page of [a, b]) await summary(page, "Two gardens · 19 runes · 57 points");
    assert.equal(await a.locator('[data-add="b"] input').isDisabled(), true);
    assert.equal(await b.locator('[data-add="a"] input').isDisabled(), true);
    assert.equal(await a.locator("#weight").isDisabled(), true);
    assert.equal(await b.locator("#label").isDisabled(), true);
    assert.equal(await b.getByRole("button", {name: "Remove amber from set A"}).isDisabled(), true);

    const input = a.getByRole("textbox", {name: "New word for set A"});
    await a.locator("#tx-start").click();
    await input.fill("🌿é"); await input.press("Enter");
    assert.equal(await a.locator("#value-totalRunes").textContent(), "19");
    await a.locator("#tx-commit").click();
    for (const page of [a, b]) await summary(page, "Two gardens · 21 runes · 63 points");
    assert.ok(frames.some((f) => f.type === "value" && !f.full && f.value?.add?.length === 1 && f.value.add[0] === "🌿é"));
    await b.locator("#weight").fill("5"); await b.locator("#weight").press("Enter");
    for (const page of [a, b]) await summary(page, "Two gardens · 21 runes · 105 points");
    await b.getByRole("textbox", {name: "New word for set B"}).fill("willow");
    await b.getByRole("textbox", {name: "New word for set B"}).press("Enter");
    await summary(a, "Two gardens · 27 runes · 135 points");

    await stop(serverB);
    await a.waitForFunction(() => !document.getElementById("peer-stale").hidden);
    assert.equal(await input.isEnabled(), true);
    await input.fill("oak"); await input.press("Enter");
    await summary(a, "Two gardens · 30 runes · 150 points"); // stale weight and willow retained
    serverB = await start("b");
    for (const page of [a, b]) await summary(page, "Two gardens · 24 runes · 72 points");
    await a.waitForFunction(() => document.getElementById("peer-stale").hidden);
    assert.equal(await a.getByRole("button", {name: "Remove willow from set B"}).count(), 0);

    await a.setViewportSize({width: 390, height: 844});
    assert.equal(await a.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.deepEqual(errors, []);
  } finally {
    if (browser) await browser.close();
    await Promise.all([...children].map(stop));
  }
});
