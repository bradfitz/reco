# Webdemo

Run from the repository root:

```sh
go run ./cmd/webdemo -listen :8081
```

Open `http://localhost:8081/` (or this machine's hostname). Assets are embedded;
there is no JS build step. The default without `-listen` is `127.0.0.1:8080`.
There is no authentication: only expose the demo to a trusted network.

Try removing `cedar` from A: the union keeps it until you also remove it from B.
Add words, clear or atomically replace sets, and edit the scalar leaves. All
computed values come from real `reco` nodes on the server. Open two tabs to see
the same shared graph. Disconnect one, edit the other, and reconnect to resync.

The computed branch is `union → details → totalRunes → score → summary`.
Each map entry contains an uppercase word and its rune count (Unicode code
points, not bytes). `totalRunes` incrementally sums those counts; `score` is that
total times "Points per rune". The starting four unique words contain 19 runes
and score 57 points at weight 3. Try replacing A with `{fern, moss}`: there are
still four unique words, but only 17 runes, so the score becomes 51. Counts are
added/subtracted from map deltas, without scanning the whole map on point edits.

By default, every edit auto-commits. **Start Tx** stages multiple leaf edits;
draft leaves are marked, while derived values stay committed. **Rollback Tx**
discards the draft; **Commit Tx** applies the whole batch atomically. Invalid
batches do not partially apply. Other tabs' edits remain live; commit applies
your staged operations to the latest server state. Disconnect discards drafts,
and an uncertain in-flight commit is never replayed automatically.

Hover or keyboard-focus a dependency name in a node footer to highlight its
source box and connecting line. Clicking it scrolls to the source box.

The wire inspector shows full snapshots on connect/reconnect and touched-key
collection deltas afterward. Updates to all affected nodes arrive in one batch,
after the DAG settles. The UI highlights changed values, not every computation.
Slow clients are disconnected instead of silently losing patches. Offline edits
are disabled and never replayed; restart resets all in-memory data.

The demo JSON protocol is private and provisional. It is not the future peer
protocol. Executor/delivery/render rate controls, coalescing visualization,
durability, Locators, and multi-process demos remain future work in `DESIGN.md`.

Tests: `go test -race ./...`.

Optional browser regression test (requires the `playwright` npm package on
Node's module path and a Playwright-installed browser, or `CHROMIUM_PATH`):

```sh
# Run a separate server; the browser test changes and resets its graph.
go run ./cmd/webdemo -listen 127.0.0.1:8082
# In another terminal:
WEBDEMO_URL=http://127.0.0.1:8082 node --test cmd/webdemo/browser_test.cjs
```
