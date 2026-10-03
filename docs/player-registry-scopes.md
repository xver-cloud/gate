# Player registry scopes

Embedding applications can host independent player registries in one Java Proxy.
This is opt-in; ordinary Gate installations remain in the empty scope.
It is session bookkeeping, not authentication or a complete tenant boundary.

In a synchronous `PreLoginEvent` subscriber, resolve trusted route ownership and
call `e.SetRegistryScope(scope)`. The scope is an exact opaque string. The first
assignment wins, including an explicit empty string; further calls return false.
Gate seals the event immediately after synchronous dispatch, before login-plugin
continuations, authentication or player registration. Calls after sealing return
false. The captured value survives virtual-host changes and backend transfers.
An integration requiring isolation must deny unknown routes, missing scope, and
failed assignments; Gate cannot know which route claims an application trusts.
Never accept an arbitrary client-provided scope as ownership evidence.

`PlayerRegistryScope(player)` retrieves the immutable value. It does not add a
method requirement to the existing `Player` interface; wrappers can preserve the
value by implementing `RegistryScope() string`. UUID, username, game profile,
Mojang/Floodgate identity and backend-forwarded identity remain unchanged. Scope
must not be encoded into those identities or derived from mutable display names.

## Lookup and duplicate behavior

| API | Scope |
| --- | --- |
| `PlayerInScope(scope, id)` | Exact scope, UUID |
| `PlayerByNameInScope(scope, name)` | Exact scope, case-insensitive name |
| `PlayersInScope(scope)` | Snapshot of one scope |
| Existing `Player(id)` / `PlayerByName(name)` | Empty scope only; never an arbitrary scoped match |
| `Players()` / `PlayerCount()` / `DisconnectAll()` | All scopes; trusted operator-wide APIs |

Same UUID/name may be present independently in different scopes. Within a
nonempty scope, both username and UUID must be unique. With
`OnlineModeKickExistingPlayers`, only a connection whose actual `OnlineMode()` is
true may replace that scope's matching UUID. Global online mode does not grant
that privilege to a forced-offline connection. An authenticated same-name,
different-UUID collision is rejected. The empty scope retains Gate's existing
global-mode duplicate policy for compatibility.

Replacement publication is atomic under the registry lock. The old connection
is disconnected outside it; pointer-checked cleanup cannot erase the replacement,
even if teardown is delayed. Duplicate disconnect events retain conflicting-login
status. Backend `RegisteredServer.Players()` lists track connection identity, so
a shared backend does not overwrite equal UUIDs from different scopes or lose a
replacement when an old connection leaves. Enumeration uses snapshots; callbacks
can mutate membership without holding the collection lock.

## Integration boundaries

Registry scopes do **not** scope the server catalog, permissions, command manager,
plugin subscriptions, default fallback list, broadcasts or proxy API access.
Default `/send`, `/glist`, `/server` and their completions use global server/player
views. A hosted-service integration must disable `BuiltinCommands`,
`AnnounceProxyCommands` and `BungeePluginChannelEnabled`, or replace them with
explicitly authorized handlers. Disabling command announcements alone is not
authorization.

The Bungee responder's player lookup/list/count/broadcast and UUID-based connect
lookup are bounded to the initiating player's scope, including server player
views. Its server catalog and server addresses remain global, and it can still
request a transfer to another registered backend. Therefore these scoped player
views do not make the plugin channel suitable for untrusted tenants on their own.

Authorize **every** `ServerPreConnectEvent` against immutable route ownership and
auth policy, including initial selection, plugins, fallback and programmatic
transfers. Use backend registrations owned by the intended workload and prevent
network bypass. Scope should normally be stable tenant+workload identity, shared
by that workload's endpoints; configuration generation is separate. Otherwise a
config edit can create a second registry for the same server. Those concepts and
policies belong to the embedding application, not Gate.

## Verification and limits

`registry_scope_test.go` covers exact-scope lookups, online/offline duplicate
policy, delayed old cleanup, concurrent same-scope admission, cross-scope
registration/enumeration, immutable sealing, backend membership and Bungee player
views. A real Minecraft protocol login through `HandleConn` keeps the same
offline account connected in two scopes and confirms untouched profile identity
after virtual-host rewriting. It holds at PostLogin without a backend: it does
not claim gameplay, forwarding, inventory or real Microsoft/Xbox authentication.
Those require the embedding application's real-backend/account matrix.

Run `go test -race -timeout 120s ./...`. No scope is inferred from a client
hostname automatically, and no configuration flag enables a safe hosted service
without application ownership checks.
