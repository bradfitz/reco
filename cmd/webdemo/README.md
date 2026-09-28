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

## Two processes, one metagraph

From this directory, run both processes with:

```sh
make multiproc
```

This builds one temporary binary and runs the peers on ports 8031 and 8032.
Ctrl-C stops both and removes the temporary binary; if either peer exits, the
other is stopped too. The ports must be free (stop any existing demo services
first), or choose alternatives with `make multiproc PORT_A=8041 PORT_B=8042`.
From the repository root, use `make -C cmd/webdemo multiproc`.

To manage the processes separately, run these in separate terminals from the
repository root:

```sh
go run ./cmd/webdemo -listen :8031 -peer-id a -peer-url http://localhost:8032
go run ./cmd/webdemo -listen :8032 -peer-id b -peer-url http://localhost:8031
```

Open both ports side by side. Process A owns set A and the garden name; process B
owns set B and points per rune. Remote leaves are read-only mirrors. Both graphs
compute every intermediate and the final summary locally. Try adding a word on
A, then changing the weight on B: both summaries converge. Use peer URLs
reachable by both the process and your browser if you aren't opening the browser
on localhost.

Only A dials, but the same full-duplex WebSocket carries both directions' watches.
The **Between processes** inspector shows initial snapshots and subsequent scalar
replacements or touched-key collection deltas. `-metagraph` sets the logical
graph name (default `word-garden`); both processes must agree. All this is
experimental and unauthenticated: use only a trusted network, not the internet.

Stop B, edit A, and start B again with the same flags. A keeps B's cached leaves
but visibly marks remote state stale. B's owned data resets because it is only
in memory. Reconnect takes fresh authoritative snapshots, including removals,
then resumes deltas. No missing remote leaf is invented before its first sync.
You can also restart A: it retries until B is available. Browser disconnect and
peer disconnect are separate states with separate indicators.

Transactions are atomic in the owning local graph; peer delivery is atomic per
leaf, **not across leaves**. During propagation or a partition the summaries can
differ. After edits stop and messages arrive, they agree; there is no global
fixed-point/transaction guarantee. Each Import is an explicit shared watch with
a closeable lifetime, rather than automatic Locator/demand resolution yet.

The browser and peer JSON protocols are separate, private, and provisional.
Executor/delivery/render rate controls, coalescing, durability, generic Locators,
and automatic remote-demand integration remain future work in `DESIGN.md`.

Tests: `go test -race ./...`.

Optional browser regression test (requires the `playwright` npm package on
Node's module path and a Playwright-installed browser, or `CHROMIUM_PATH`):

```sh
# Run a separate server; the browser test changes and resets its graph.
go run ./cmd/webdemo -listen 127.0.0.1:8082
# In another terminal:
WEBDEMO_URL=http://127.0.0.1:8082 node --test cmd/webdemo/browser_test.cjs
```

The multi-process browser test starts and stops its own isolated servers,
including a peer restart while the other process accepts offline edits:

```sh
go build -o /tmp/reco-webdemo ./cmd/webdemo
WEBDEMO_BIN=/tmp/reco-webdemo node --test cmd/webdemo/peer_browser_test.cjs
```
