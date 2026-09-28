// Optional real-browser regression test. Requires Playwright and an isolated
// demo server: this test edits and resets its shared graph. See README.md.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");

// Reset test state through the demo protocol; there is no reset control in the UI.
async function resetFixture(page) {
  const before = await page.locator("#revision").textContent();
  assert.equal(await page.evaluate(() => send({op: "reset"})), true);
  await page.waitForFunction((before) => document.getElementById("revision").textContent !== before, before);
}

test("browser edits, deltas, cross-tab watches, and reconnect", async () => {
  assert.ok(process.env.WEBDEMO_URL, "set WEBDEMO_URL to an isolated test server");
  const browser = await chromium.launch({
    executablePath: process.env.CHROMIUM_PATH || undefined,
    headless: true,
    args: ["--no-sandbox"],
  });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    const a = await context.newPage(), b = await context.newPage();
    const errors = [];
    for (const page of [a, b]) {
      page.on("pageerror", (error) => errors.push(error.message));
      page.on("console", (msg) => { if (msg.type() === "error") errors.push(msg.text()); });
      await page.goto(process.env.WEBDEMO_URL);
      await page.waitForFunction(() => document.getElementById("status").textContent === "Live");
      assert.equal(await page.locator(".intro, .experiment, #reset").count(), 0);
    }
    await resetFixture(a);
    await a.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "19");

    // Same number of unique words, different rune total and score.
    await a.locator('[data-replace="a"]').click();
    await a.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "17");
    assert.equal(await a.locator("#value-union .chip").count(), 4);
    assert.equal(await a.locator("#value-score").textContent(), "51");
    await resetFixture(a);
    await a.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "19");

    // Regression: form.elements.item is a method, not the input named "item".
    const input = a.getByRole("textbox", { name: "New word for set A" });
    await input.fill(" "); await input.press("Enter");
    await a.locator("#error").waitFor({state: "visible"});
    assert.equal(await a.locator("#status").textContent(), "Live");
    // The server counts Unicode code points, not bytes or UTF-16 units.
    await input.fill("🌿é"); await input.press("Enter");
    for (const page of [a, b]) {
      await page.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "21");
      assert.equal(await page.locator("#value-score").textContent(), "63");
    }
    const unicodeWire = JSON.parse(await a.locator("#wire-message").textContent());
    assert.equal(unicodeWire.nodes.find((n) => n.id === "details").map.put["🌿é"].runes, 2);
    await a.getByRole("button", {name: "Remove 🌿é from set A"}).click();
    await a.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "19");

    await input.fill("willow");
    await input.press("Enter");
    for (const page of [a, b]) {
      await page.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "25");
      assert.equal(await page.locator("#error").isVisible(), false);
      assert.match(await page.locator("#value-details").textContent(), /WILLOW/);
    }
    let wire = JSON.parse(await a.locator("#wire-message").textContent());
    assert.equal(wire.type, "update");
    const map = wire.nodes.find((node) => node.id === "details");
    assert.equal(Object.hasOwn(map, "value"), false);
    assert.deepEqual(Object.keys(map.map.put), ["willow"]);
    assert.equal(map.map.put.willow.runes, 6);

    await a.getByRole("button", { name: "Remove cedar from set A" }).click();
    await a.waitForFunction(() => !document.querySelector('[aria-label="Remove cedar from set A"]'));
    assert.equal(await a.locator("#value-totalRunes").textContent(), "25");
    wire = JSON.parse(await a.locator("#wire-message").textContent());
    assert.deepEqual(wire.nodes.map((node) => node.id), ["a"]);

    await a.locator("#connection-toggle").click();
    assert.equal(await a.locator("#stale").isVisible(), true);
    assert.equal(await input.isDisabled(), true);
    const otherInput = b.getByRole("textbox", { name: "New word for set B" });
    await otherInput.fill("juniper");
    await b.locator('[data-add="b"] button').click();
    await b.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "32");
    assert.equal(await a.locator("#value-totalRunes").textContent(), "25");
    await a.locator("#connection-toggle").click();
    await a.waitForFunction(() => document.getElementById("status").textContent === "Live" && document.getElementById("value-totalRunes").textContent === "32");
    assert.equal(JSON.parse(await a.locator("#wire-message").textContent()).type, "snapshot");

    // Stage locally: previews change, but computed values and the other tab do not.
    const baseRevision = await a.locator("#revision").textContent();
    await a.locator("#tx-start").click();
    await input.fill("spruce"); await input.press("Enter");
    await a.locator("#weight").fill("9"); await a.locator("#weight-form button").click();
    assert.match(await a.locator("#value-a").textContent(), /spruce/);
    assert.equal(await a.locator("#node-a").evaluate((el) => el.classList.contains("draft")), true);
    assert.equal(await a.locator("#value-totalRunes").textContent(), "32");
    assert.equal(await a.locator("#value-score").textContent(), "96");
    assert.equal(await a.locator("#revision").textContent(), baseRevision);
    assert.doesNotMatch(await b.locator("#value-a").textContent(), /spruce/);
    await a.locator("#tx-rollback").click();
    assert.doesNotMatch(await a.locator("#value-a").textContent(), /spruce/);
    assert.equal(await a.locator("#weight").inputValue(), "3");

    // A server-side validation failure rejects EVERY edit and keeps the draft
    // available to roll back, rather than clearing it as if commit succeeded.
    await a.locator("#tx-start").click();
    await input.fill("rejected"); await input.press("Enter");
    await a.locator("#label").fill("é".repeat(50));
    await a.locator("#label-form button").click();
    await a.locator("#tx-commit").click();
    await a.locator("#error").waitFor({state: "visible"});
    assert.equal(await a.locator("#revision").textContent(), baseRevision);
    assert.doesNotMatch(await b.locator("#value-a").textContent(), /rejected/);
    assert.equal(await a.locator("#tx-rollback").isEnabled(), true);
    await a.locator("#tx-rollback").click();

    await a.locator("#tx-start").click();
    await input.fill("spruce"); await input.press("Enter");
    await a.locator("#weight").fill("9"); await a.locator("#weight-form button").click();
    await a.locator("#tx-commit").click();
    for (const page of [a,b]) {
      await page.waitForFunction(() => document.getElementById("value-score").textContent === "342");
    }
    await a.waitForFunction(() => !document.getElementById("tx-start").disabled);
    assert.equal(Number((await a.locator("#revision").textContent()).split(" ")[1]), Number(baseRevision.split(" ")[1]) + 1);
    assert.equal(await a.locator("#node-a").evaluate((el) => el.classList.contains("draft")), false);
    // Inspect the broadcast on the other tab (the originating tab also gets an ack).
    wire = JSON.parse(await b.locator("#wire-message").textContent());
    assert.deepEqual(Object.keys(wire.nodes.find((n) => n.id === "details").map.put), ["spruce"]);
    await a.getByRole("button", {name: "Remove spruce from set A"}).click();
    await a.waitForFunction(() => document.getElementById("value-totalRunes").textContent === "32");

    // Hover and keyboard focus identify exactly the selected dependency edge.
    const dep = a.locator('#node-summary .dep-link[data-from="label"]');
    await dep.hover();
    assert.equal(await a.locator("#node-label.dep-highlight").count(), 1);
    assert.equal(await a.locator('#edges path.dep-highlight[data-from="label"][data-to="summary"]').count(), 1);
    assert.equal(await a.locator("#edges path.dep-highlight").count(), 1);
    await a.locator(".brand").hover();
    assert.equal(await a.locator(".dep-highlight").count(), 0);

    // A disconnected draft is discarded, never silently replayed on reconnect.
    await a.locator("#tx-start").click();
    await input.fill("never-sent"); await input.press("Enter");
    await a.locator("#connection-toggle").click();
    assert.doesNotMatch(await a.locator("#value-a").textContent(), /never-sent/);
    await a.locator("#connection-toggle").click();
    await a.waitForFunction(() => document.getElementById("status").textContent === "Live");
    assert.doesNotMatch(await a.locator("#value-a").textContent(), /never-sent/);
    assert.equal(await a.locator("#tx-start").isEnabled(), true);
    const totalRunesDep = a.locator('#node-summary .dep-link[data-from="totalRunes"]');
    await totalRunesDep.focus();
    assert.equal(await a.locator("#node-totalRunes.dep-highlight").count(), 1);
    assert.equal(await a.locator('#edges path.dep-highlight[data-from="totalRunes"][data-to="summary"]').count(), 1);
    await a.locator("#weight").focus();
    assert.equal(await a.locator(".dep-highlight").count(), 0);

    await a.locator("#weight").fill("7");
    await a.locator("#weight-form button").click();
    await a.waitForFunction(() => document.getElementById("value-score").textContent === "224");
    await a.locator("#label").fill("Live test");
    await a.locator("#label-form button").click();
    await a.waitForFunction(() => document.getElementById("value-summary").textContent.startsWith("Live test"));
    await a.locator('[data-replace="a"]').click();
    await a.waitForFunction(() => document.getElementById("value-a").textContent.includes("fern") && !document.getElementById("value-a").textContent.includes("willow"));
    await a.locator('[data-clear="a"]').click();
    await a.waitForFunction(() => document.getElementById("value-a").textContent.includes("empty set"));

    // Arbitrary keys are text, not HTML or JavaScript object properties.
    await input.fill("<b>safe</b>");
    await input.press("Enter");
    await a.waitForFunction(() => document.getElementById("value-a").textContent.includes("<b>safe</b>"));
    assert.equal(await a.locator("#value-a b").count(), 0);
    await input.fill("__proto__");
    await input.press("Enter");
    await a.waitForFunction(() => document.getElementById("value-details").textContent.includes("__proto__"));

    await a.setViewportSize({ width: 390, height: 844 });
    assert.equal(await a.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    assert.deepEqual(errors, []);
    await resetFixture(a);
    await a.waitForFunction(() => document.getElementById("value-summary").textContent === "Word garden · 19 runes · 57 points");
  } finally {
    await browser.close();
  }
});
