# Tailscale map protocol

The Tailscale map response protocol supplies a client with its network configuration as an initial snapshot followed by updates to that snapshot. **The first non-keepalive response establishes the state of a new map stream; subsequent responses usually omit unchanged information.** Correctly interpreting an update requires retaining earlier state and applying each field's specific rules. There is no universal recursive JSON merge operation, and there is no explicit “this is a delta” flag.

This report describes the public [tailscale/tailscale repository](https://github.com/tailscale/tailscale) at commit `d229a06f9a4b2340f0749288df0ac4f6de223acd`, whose `CurrentCapabilityVersion` is **148**. It is based on the `tailcfg` definitions, `control/controlclient` implementation and tests, and the downstream mutation conversion in `types/netmap/nodemut.go`. Where comments and implementation differ, those differences are identified explicitly. Source links are pinned to that revision.

The distinction between the initial response and later responses is:

| Property | First non-keepalive response of a fresh stream | Later non-keepalive responses |
| --- | --- | --- |
| Purpose | Establish a complete starting configuration, including defaults for omitted optional fields. | Change selected parts of the accumulated configuration. |
| Self `Node` | Required by this client's read loop. Its absence ends the poll with `initial MapResponse lacked node`. | Omitted or `null` means retain the previous self node. A supplied node replaces it. |
| Peers | Normally the complete visible peer set in `Peers`, sorted by `Node.ID`. No peers is valid. | Usually `PeersChanged`, `PeersRemoved`, or smaller patch fields. A nonempty `Peers` can still replace the entire peer set. |
| DNS, filters, policy, other state | Supply the applicable starting values. “Complete” does not require every JSON property to be present. | Retain omitted state, subject to the field-specific rules below. |
| Keepalives | May arrive before the initial configuration without satisfying the initial-node requirement. | Maintain liveness without applying ordinary network-map fields. |
| Client notification | The required `Node` makes the first response take the full-network-map path. | May produce either downstream mutations or a reconstructed full network map. |

The schema describes the initial response as complete, but the read loop's explicit completeness check only requires `Node`. It does not validate that every other necessary configuration field was supplied. A fresh session starts with no peers, an empty DNS configuration, no filter rules, no SSH policy, no TKA information, empty domain strings, and service collection disabled. Thus a server must supply the starting configuration it intends the client to use; missing fields cannot inherit values from a previous poll's `mapSession`. See the [MapResponse contract](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L1985), [session constructor](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L112), and [initial-response check](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L1352).

The request and response travel through `POST /machine/map`. A `MapRequest` is JSON and advertises the client's capability version, node and discovery keys, host information, endpoints, and request mode. The current `Direct` client uses the ts2021 HTTP/2-over-Noise transport, requests `Compress: "zstd"`, and sets `KeepAlive: true`. The URL in the request is not a description of a plain HTTPS-only transport: the request is issued through the Noise client. See [MapRequest](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L1427), [request construction](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L1092), and [ts2021 transport](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/ts2021/client.go#L170).

| Request mode | Meaning |
| --- | --- |
| `Stream: true` | Receive an initial map and then further messages in one long-lived HTTP response: a “map poll.” For capability version 68 and later, the server should treat this as read-only and ignore host/endpoint updates carried in the request. |
| `Stream: false`, fetching a map | Receive one framed map response. `FetchNetMapForTest` uses this mode. |
| `Stream: false, OmitPeers: true, ReadOnly: false` | Upload the client's changed state without interrupting its existing map poll. The server may omit the response body; the client requires HTTP 200 and discards any body. `SendUpdate` uses this mode. |

`Auto` maintains separate routines for receiving maps and sending local changes. Updating endpoints or host information therefore does not inherently require restarting the map stream. The older `ReadOnly` request field is deprecated and remains false in this client. See [updateRoutine](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/auto.go#L59), [mapRoutine](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/auto.go#L580), and [SendUpdate](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L971).

The successful response body is a sequence of application-level frames:

```text
uint32 little-endian payload length | payload bytes
uint32 little-endian payload length | payload bytes
...

For the current client's Compress: "zstd" request:
payload bytes = zstd-compressed JSON for one MapResponse
```

The length counts the compressed payload, excluding the four-byte prefix. Each payload is decoded separately. The body is neither a JSON array nor newline-delimited JSON, and HTTP/2 frame boundaries do not determine map-message boundaries. The schema also permits requesting no compression, but this implementation always requests zstd and its response decoder always decompresses zstd. It creates a fresh `MapResponse` value for each JSON decode, then explicitly applies that message to session state.

The current implementation limits each wire payload to 256 MiB and decoded JSON to 1 GiB, while allowing an unbounded number of messages in the HTTP response. A 120-second watchdog covers waiting for the response and subsequent network reads; it is stopped during message processing. Read/decompression/JSON errors end the poll. `Auto.mapRoutine` retries after the poll ends, with backoff and special handling for rate-limited responses. These sizes and timers describe this implementation, not immutable wire-format constants. See [framing and decoding](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L1454) and [read loop](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L1266).

**The retained map state belongs to one poll in this client.** `sendMapRequest` constructs a new `mapSession` for each response stream and closes it when that poll ends. Other parts of the client can retain operational configuration while reconnecting, but the next poll's map accumulator starts fresh.

The schema does define an optional resumable-session mechanism: the server can return a `MapSessionHandle` on the first message and `Seq` values on state-changing messages. A client can request reattachment with that handle and the last processed sequence number in `MapRequest.MapSessionSeq`; an accepting server returns later sequence numbers. Matching the returned handle identifies a resumed session. The server may decline and start a fresh session, and keepalives or other non-state-changing messages may omit `Seq`.

**That mechanism is not implemented by the `control/controlclient` code examined here.** The request builder does not set the resume fields, and the receive path does not retain or use the response handle or sequence number. Consequently, a control server interoperating with this client must send a fresh initial configuration after each new map poll rather than assume deltas can continue across reconnects. See [resume-field definitions](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L1475) and [per-poll session creation](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L1228).

Peers are indexed by numeric `Node.ID`, not by their WireGuard key, name, or `StableID`. Three forms of peer updates serve different purposes:

| Field | Interpretation |
| --- | --- |
| Nonempty `Peers` | Replace the entire peer set with these complete nodes. Peers absent from the list disappear. |
| `PeersChanged` | Insert new peers or replace existing peers by ID with complete `Node` values. Other peer IDs remain. This is not a sparse patch to each node's fields. |
| `PeersRemoved` | Delete the listed IDs. Unknown IDs have no effect. |
| `PeersChangedPatch` | Apply supported field-level changes to existing peers. A patch cannot introduce a previously unknown node. |
| `PeerSeenChange` | For known IDs, `true` sets `LastSeen` to the client's current time; `false` clears `LastSeen` to nil. Neither removes the peer. |
| `OnlineChange` | For known IDs, explicitly set `Node.Online` to true or false. Neither removes the peer. |

The server promises `Peers` and `PeersChanged` sorted by `Node.ID`; the client also sorts its accumulated peers when building a full network map. A peer being offline is distinct from that peer being removed. Likewise, a `PeersChanged` response does not imply that unmentioned peers disappeared.

An especially consequential detail is that the peer replacement test is **`len(resp.Peers) > 0`**, not `resp.Peers != nil`. Therefore `"Peers": []`, `"Peers": null`, and an omitted `Peers` all leave existing peers alone unless other delta fields change them. An empty peer list works initially because the session is already empty. To remove the last peers during an established stream, send their IDs in `PeersRemoved`; sending `Peers: []` will not do it. A later nonempty `Peers` refresh replaces only peer state, without resetting other retained configuration.

The state accumulator applies peer operations in this order:

1. If `Peers` is nonempty, replace the peer set and return immediately from peer processing.
2. Otherwise apply `PeersRemoved`.
3. Apply full-node upserts in `PeersChanged`.
4. Apply `PeerSeenChange`.
5. Apply `OnlineChange`.
6. Apply `PeersChangedPatch` in slice order.

This is the order in [updatePeersStateFromResponse](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L550), after the normalization and patch conversion discussed below. The early return in step 1 ignores **all** peer delta fields, including patches and online/seen updates. The `PeersChangedPatch` comment says patches apply after the other peer fields, but that does not describe the nonempty-`Peers` branch in this implementation. The schema itself recommends sending patches without the other `Peers*` fields. A server should avoid overlapping representations for the same node in one message.

Each [PeerChange](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L3031) identifies a peer with `NodeID` and supports these updates:

| Patch field | Update condition and meaning |
| --- | --- |
| `DERPRegion` | Nonzero replaces `Node.HomeDERP`. Zero means no change, so it cannot clear the home region. |
| `Cap` | Nonzero replaces the peer's capability version. Zero means no change. |
| `Endpoints` | A non-nil slice replaces endpoints. The code accepts an explicit empty array to clear them, despite the comment saying “if non-empty.” |
| `CapMap` | A non-nil map replaces the entire capability map; `{}` clears it. This is not a patch to individual capability keys. |
| `Key`, `DiscoKey` | Non-nil pointers replace the respective keys. |
| `KeySignature` | A non-nil byte slice replaces the signature. Its normal JSON representation is a base64 string. |
| `Online` | A non-nil pointer sets online status; explicit `false` is an update. |
| `LastSeen` | A non-nil timestamp replaces `LastSeen`; null means no update, so this field cannot clear it to nil. |
| `KeyExpiry` | A non-nil timestamp replaces the expiry, including an explicitly supplied zero time. |

Unknown patch IDs are ignored by the session accumulator. Changes to fields outside this structure, such as a name, addresses, routes, tags, or host information, require a complete node in `PeersChanged` or a full peer-list refresh. A supplied complete node carries its own field defaults: omitted fields inside it do not inherit arbitrary fields from the previous node. One defined normalization is that nil `AllowedIPs` means the node's `Addresses`; it does not mean the node's previous allowed routes.

The same retain-versus-replace distinction applies to the non-peer fields. [updateStateFromResponse](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L368) maintains the following state:

| Field | Omitted/null/empty behavior | Supplied value |
| --- | --- | --- |
| `Node` | Nil retains the self node after the initial response. | Replaces the self node and recomputes its capabilities. |
| `DNSConfig` | Nil retains the prior configuration. | Replaces the whole DNS configuration. `{}` resets it to empty; fields within the object are not merged with old DNS fields. |
| `DERPMap` | Nil retains the previous map. | Has its own nested retention rules, described below. |
| `Domain`, `DomainDataPlaneAuditLogID` | Empty string retains the previous value. | Nonempty string replaces it. An empty string cannot clear it within a stream. |
| `CollectServices` | Unset or null retains the previous value. | Explicit JSON `true` or `false` updates it. The Go `opt.Bool` representation is a string internally, but its wire encoding is a JSON boolean. |
| `PacketFilter` | Nil retains the prior legacy filter chunk. | Replaces the named `base` chunk; `[]` clears that chunk's rules. |
| `PacketFilters` | Nil or `{}` leaves named chunks unchanged. | Patches chunks by name, with deletion and clear-all operations. |
| `UserProfiles` | Omitted, null, or `[]` does not erase known profiles. | Upserts complete profiles by `UserProfile.ID`. There is no corresponding profile-removal field. |
| `Health` | Nil retains the previous list. | Replaces the list; `[]` clears reported problems. |
| `DisplayMessages` | Nil or `{}` means no change. | Patches messages by ID, with deletion and clear-all operations. |
| `SSHPolicy` | Nil retains the previous policy. | Replaces the whole policy. Clearing its rules requires an explicit policy object. |
| `TKAInfo` | Initially nil means no TKA enabled by the map; later nil means unchanged. | Replaces the TKA information. `Disabled: true` explicitly signals disablement; null cannot signal that transition. |

A `DERPMap` is itself partly incremental. If `Regions` is nil, both the old `Regions` and old `OmitDefaultRegions` are retained. A non-nil `Regions` map replaces the entire region map, and then the accompanying `OmitDefaultRegions` value takes effect. It is not merged by region ID. An explicit empty regions object therefore replaces the stored region set with an empty set. Nil region definitions are discarded defensively.

If `HomeParams` is nil it is retained; if a supplied `HomeParams.RegionScore` is nil, the previous scores are retained. A non-nil `RegionScore` replaces the score map, and `{}` resets scores to the implicit factor of 1.0. Thus `DERPMap: {}` generally leaves an existing DERP map unchanged, whereas `DNSConfig: {}` clears DNS configuration. See [DERPMap and DERPHomeParams](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/derpmap.go#L19) and [DERP delta tests](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map_test.go#L635).

Packet filters use a persistent map of named rule slices. The exact merge order is:

1. If singular `PacketFilter` is non-nil, install it as chunk `"base"`.
2. If `PacketFilters` contains `"*": null`, clear every stored chunk, including `base` just installed in step 1.
3. For the remaining `PacketFilters` entries, a non-null array replaces that chunk and null deletes it. The `"*"` key is reserved and skipped as an ordinary chunk.
4. Concatenate all stored chunks in sorted key order and parse the resulting rules.

Sorting provides deterministic output; allow-rule semantics do not depend on chunk order. Both singular and plural fields can appear in one response or alternate throughout a session. **`PacketFilter: []` does not clear other named chunks.** With only the legacy `base` chunk, an empty filter means no allow rules, described in the schema as blocking everything. When using named chunks, `{"PacketFilters":{"*":null}}` clears all of them. `{"PacketFilters":{"acl":[]}}` leaves an empty chunk called `acl`, while `{"PacketFilters":{"acl":null}}` removes that chunk. See [filter field definitions](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L2100) and [merge tests](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map_test.go#L541).

`DisplayMessages` uses a similar keyed patch: a non-null value inserts or replaces one whole message, null deletes its ID, and `"*": null` clears all prior messages before other entries are applied. `Health` is the older whole-list representation. The schema says to use one representation or the other, not both in a response. The implementation stores them separately and prefers nonempty structured messages when constructing a network map; if there are none, it converts retained legacy health strings into display messages. A server switching representations should account for retained legacy health rather than assume clearing structured messages clears that old list too. See [health-message definitions](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L2143) and [network-map construction](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L1007).

Presence matters throughout these rules. For pointer, slice, and map fields decoded into a fresh Go value, omission and JSON null commonly both become nil. Explicit `[]` and `{}` can instead request replacement with empty state, depending on the field. The significance of null also depends on its position: top-level `PacketFilters: null` means unchanged, but `PacketFilters: {"acl": null}` means delete one chunk.

The `MapResponse` type has historical `omitempty` tags that make it unsuitable as a general server-side encoder for every valid operation. For example, marshaling `MapResponse{Health: []string{}}` omits `Health`, losing the intended clear operation. Similar issues affect singular `PacketFilter`, and `PeerChange` has the same concern for empty endpoints and capability maps. A server needs a representation that preserves explicit empty arrays/objects, such as dedicated encoding types or raw JSON fields. The schema's introductory comment explicitly says the Tailscale control plane uses a separate encoding type for this reason. Never infer an update's meaning solely from Go zero-value terminology or JSON “truthiness”: explicit false can matter, and an empty object can either clear or preserve state depending on the field.

Some response fields represent control events or configuration outside the network-map accumulator. `Direct.sendMapRequest` handles these before handing a non-keepalive response to `mapSession`:

| Field | Handling in this client |
| --- | --- |
| `PingRequest` | Answers a deduplicated liveness/diagnostic request asynchronously. It can accompany a keepalive or a map update. |
| `PopBrowserURL` | Requests opening a nonempty URL. The session suppresses a URL equal to the last one it handled. |
| `ControlTime` | Logs and publishes a supplied nonzero server timestamp. |
| `ClientVersion` | Publishes supplied version information. |
| `ControlDialPlan` | Stores a supplied plan when enabled and a destination store is available. Nil does not trigger a store update. |
| `DefaultAutoUpdate` | Publishes the deprecated optional boolean; the modern self-node capability form is also handled. The schema says the initial stored default is used and later control changes are ignored by its consumer. |
| `Debug` | For non-keepalive messages, invokes the debug handler before applying the map update. |

`KeepAlive: true` bypasses normal node, peer, DNS, filter, and policy processing, and bypasses the first-node check. The documented exceptions are `PingRequest`, `ControlTime`, and `PopBrowserURL`. **The actual read loop also handles `ClientVersion` and `ControlDialPlan` before its keepalive early return**, so these have effects on keepalive messages in this revision. Servers targeting the documented contract should use non-keepalive messages for those extra fields. The schema describes an initially nil dial plan as equivalent to an empty plan; the actual store update here occurs only for a non-nil plan. See [out-of-band handling and keepalive branch](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L1299).

Within `map.go`, the processing path is:

1. Handle `Debug`, then normalize incoming self/peer nodes. Legacy `DERP: "127.3.3.40:N"` is converted to `HomeDERP`; nil `AllowedIPs` becomes `Addresses`. Nodes marked `UnsignedPeerAPIOnly` have allowed routes restricted to their own addresses. Legacy self capabilities are incorporated into `CapMap`, control knobs are updated, and display names are initialized.
2. Run `patchifyPeersChanged`, comparing full changed nodes against the session's existing peers. When it can express a change as a `PeerChange`, it moves it to `PeersChangedPatch`. Identical nodes can be dropped. New nodes or replacements needing other fields remain in `PeersChanged`.
3. Apply the response to **all retained session state**, including peers, users, filters, DNS, and policy.
4. Attempt `tryHandleIncrementally` to notify the downstream consumer with narrower updates.
5. If that cannot handle the response, construct a complete `netmap.NetworkMap` from the already-updated session, run the self-node callback when needed, and call `UpdateFullNetmap`.

Updating session state before trying the optimization is essential: a later response or full rebuild still needs an accurate accumulated configuration even when earlier messages were delivered downstream as mutations. The `DisableDeltaUpdates` control knob disables this notification optimization; it does not disable understanding deltas from the server. See [HandleNonKeepAliveMapResponse](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L167), [patchifyPeersChanged](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L696), and [tryHandleIncrementally](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L277).

The downstream fast path requires `NetmapDeltaUpdater`. In this revision it can deliver full-node additions/replacements and removals as `NodeMutationUpsert` and `NodeMutationRemove`, in addition to field mutations. A route-changing complete `PeersChanged` node therefore need not force a complete network-map rebuild. Upsert and remove consumers must handle those mutation types explicitly; their `Apply(*Node)` methods are no-ops.

Field patches currently have narrower fast-path support than the wire schema: `DERPRegion`, `Endpoints`, `Online`, and `LastSeen` convert to field mutations. Other supported wire patch fields, such as `CapMap`, keys, or expiry, still update session state but make the client fall back to a full network-map notification. Self-node updates, DNS, DERP-map changes, policies, health, and the other fields listed in `mapResponseContainsNonPatchFields` likewise force fallback. Even an explicitly empty, non-nil `Peers` slice triggers that fallback check, although it does not clear peers.

Filter changes can stay on the fast path if the consumer also implements and accepts `PacketFilterUpdater`. User-profile changes require `UserProfileUpdater`. Filters and profiles are delivered before peer mutations. The profile update also replays cached user and sharer profiles needed by upserted peers, because the server only resends changed profiles and downstream full maps include only currently referenced users. If an interface is absent, a callback declines, or mutation conversion fails, the client rebuilds a full map. See [updater interfaces](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/direct.go#L226), [mutation conversion](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/types/netmap/nodemut.go#L112), and [profile replay test](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map_test.go#L977).

There are additional implementation edge cases worth distinguishing from the intended wire semantics:

- The session accumulator clears `LastSeen` for `PeerSeenChange[id] = false`, but `MutationsFromMapResponse` emits a last-seen mutation only for true. If this response succeeds on the fast path, downstream consumers do not receive that clearing operation.
- The mutation converter emits patches before `OnlineChange` and `PeerSeenChange`, then stably sorts mutations by node ID. The session accumulator applies those legacy maps before patches. Conflicting updates for the same node and field can therefore produce different results on the two paths. Avoid sending such conflicts.
- `peerChangeDiff` is not lossless for every zero/nil transition in this revision. For example, a full replacement changing `HomeDERP` to zero can become a patch with `DERPRegion: 0`, which the accumulator treats as unchanged. Its `Online` and `LastSeen` comparison only emits a change when both old and new pointers are non-nil, and some nil slice transitions are also not expressible by the generated patch. Servers relying on clearing these fields should verify the exact transition against the client rather than assume every full-node replacement survives this optimization unchanged. A nonempty full `Peers` refresh bypasses this conversion for those nodes.

These observations follow directly from [peerChangeDiff](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L758), [peer-state application](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/control/controlclient/map.go#L550), and [MutationsFromMapResponse](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/types/netmap/nodemut.go#L149). They are implementation caveats, not additional protocol guarantees.

For a concrete stream, the following decoded JSON illustrates the initial snapshot. Node keys and other operational node details are omitted here for readability; an actual server must supply complete applicable node information.

```json
{
  "Node": {
    "ID": 1,
    "Name": "self.example.ts.net.",
    "Addresses": ["100.64.0.1/32"]
  },
  "Peers": [
    {
      "ID": 2,
      "Name": "peer.example.ts.net.",
      "Addresses": ["100.64.0.2/32"],
      "HomeDERP": 1,
      "Endpoints": ["192.0.2.2:41641"]
    }
  ],
  "DNSConfig": {"Domains": ["example.ts.net"], "Proxied": true},
  "Domain": "example.com",
  "PacketFilter": []
}
```

The peer set now contains node 2; its implicit `AllowedIPs` equal its `Addresses`. The empty filter intentionally supplies no allow rules. The next message can change just that peer's endpoints:

```json
{"PeersChangedPatch":[{"NodeID":2,"Endpoints":["198.51.100.2:41641"]}]}
```

The self node, peer name, addresses, home DERP, DNS, domain, and filter remain unchanged. Then a keepalive changes none of that state:

```json
{"KeepAlive":true}
```

Finally, this message removes the last peer and resets DNS to an empty configuration while retaining the self node, domain, and filter:

```json
{"PeersRemoved":[2],"DNSConfig":{}}
```

Replacing that final message with `{"Peers":[],"DNSConfig":null}` would retain both the peer and DNS configuration. This contrast is why a decoder must preserve field presence and use the appropriate field rules. Each illustrated object travels as its own length-prefixed, compressed payload for the current client.

Compatibility is negotiated through `MapRequest.Version`, historically called the map version and now a general capability version. The most relevant milestones in the [capability history](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tailcfg/tailcfg.go#L51) are:

| Capability version | Relevant change |
| --- | --- |
| 5 | Incremental peers and user profiles. |
| 6 | Nil `PacketFilter` means unchanged. |
| 10 | `PeerSeenChange`. |
| 15 | Nil `DNSConfig` means unchanged. |
| 16 | `Node.Online` and `OnlineChange`. |
| 17 | Empty `Domain` means unchanged. |
| 18 | Nil self `Node` means unchanged. |
| 33 | `PeersChangedPatch`, initially DERP and endpoint changes. |
| 36, 40 | Additional peer patch fields and key signatures. |
| 68 | Dedicated upload routine; streaming requests are read-only. |
| 81 | Named `PacketFilters`. |
| 111 | Numeric `Node.HomeDERP`. |
| 112 | Nil `Node.AllowedIPs` may mean `Addresses`. |
| 117 | Structured `DisplayMessages`. |

A server supporting older clients must honor the advertised capabilities rather than assume this checkout's full schema is understood. For a server targeting this client, the practical requirements are to send a fresh complete initial configuration on each poll, preserve explicit clear operations during JSON encoding, send complete nodes for `PeersChanged`, keep patch representations unambiguous, and retain per-client stream state so each emitted delta describes a transition from what that client has already received.

Validation included running the existing focused tests in `./control/controlclient` and `./types/netmap` for peer-state application, retained configuration, named packet filters, DERP-map deltas, peer diff/patch conversion, incremental full-node replacements, user-profile replay, display-message patches, framing/decoded-size limits, and downstream mutation classification/conversion. Both packages passed. The implementation caveats above were identified by source inspection; those passing tests do not establish coverage of every zero/nil transition or mixed-field conflict.
