# Wasl, Explained From Zero

*A plain-language guide for someone who joined the project today and asked: "What is this
thing, how does it actually work, and what's currently wrong with it?"*

This document assumes you know nothing about Wasl — not its purpose, not its vocabulary, not
its code. Every Wasl-specific term is explained in plain words the first time it appears. It
builds up in order: what the system is, its core ideas, how the old-and-stable part works, what
it can and can't do, then the newest feature, and finally — the reason this document is worth
reading — the real, not-yet-fixed bugs in that newest feature.

---

## 1. What is Wasl, in one paragraph

AI coding assistants that run in a terminal (things like Claude Code, Codex, Gemini CLI,
opencode) are normally solo: one human, one assistant, one machine. **Wasl is a small server
that lets these assistants talk to *each other* — and lets a human approve every message before
it's sent.** One assistant can ask another a question, hand it a task, drop it a note, or send
it a file — whether the other assistant is running on the same computer or on a different one
across the network. The catch, and the whole point, is that **a human gets a chance to say
"yes, send that" or "no, block it" before anything actually leaves.** Wasl is fully
self-contained: you don't need any other software to run it, though a bigger dashboard *can*
plug into it later if someone wants one. (The name "Wasl", وصل, is Arabic for "to connect".)

---

## 2. The core concepts, one at a time

### 2.1 An "agent"

An **agent** is just one running AI coding-assistant CLI — for example, one instance of Claude
Code running in a terminal on your laptop. Every agent has an id (a name Wasl uses to refer to
it). Wasl's job is to be the switchboard *between* agents.

Concretely: imagine Alice is running Claude Code on `laptop-1` and Bob is running opencode on
`server-2`. To Wasl, "Alice" and "Bob" are two agents, and Wasl is the wiring that lets Alice's
assistant reach Bob's assistant at all.

### 2.2 The four things one agent can do to another: consult, command, message, transfer

Wasl exposes exactly four actions (verified from the specs — this really is the vocabulary):

- **consult** — "ask a question and wait for the answer." *Example:* Alice's agent consults
  Bob's agent: "What's the current schema version of the billing table?" and gets a text reply
  back.
- **command** — "tell it to do something." *Example:* Alice commands Bob: "Run the migration
  script on staging." The reply matters, but the important part is the *action* it triggers on
  Bob's side.
- **message** — "drop a note; no answer expected." *Example:* Alice messages Bob: "Heads up, I
  merged the auth refactor." Wasl just confirms it was delivered.
- **transfer** — "send a file directly." *Example:* Alice transfers a 200 MB dataset to Bob.
  This one is special: the file goes *directly* from Alice's machine to Bob's machine (using
  ordinary tools like `scp` or `rsync`), never routed through Wasl in the middle.

The first three (consult/command/message) can also fan out to **many** recipients at once — a
"multicast" or "broadcast." *Example:* Alice broadcasts a `message` to Bob, Carol, and Dave in
one go.

> The word **delivery** shows up a lot in the code. A delivery is simply "one of these four
> actions, packaged up with who it's from, what kind it is, and its text/data" — the unit of
> work that arrives at the receiving side.

### 2.3 The three-tier permission gate, and why it exists

Wasl's founding rule is **permission-before-send**: a human should be able to approve or deny
*every* outgoing action before it goes out. But different assistant CLIs support different ways
of asking a human. So Wasl has a **gate** — the component that runs the approval check — with
three "tiers", meaning three different ways to ask, tried in priority order:

1. **In-band prompt (elicitation).** Some CLIs (Claude Code, Codex) can pop up a question to the
   human *through the same channel Wasl is already talking to them on*. This is the cleanest
   path: Wasl asks "Approve sending this consult to Bob?" and the human clicks yes/no right
   there. (The technical name is "MCP elicitation" — MCP is the protocol these CLIs speak; more
   on that in §3.)

2. **Wasl's own two-step handshake (prepare → commit).** Some CLIs (e.g. Gemini) *can't* be
   prompted in-band. For those — and for any case where you need a *guaranteed* real human
   sign-off — Wasl runs its own handshake: it first records the pending request ("prepare"),
   hands out a one-time token, and only actually sends once someone comes back and approves that
   token ("commit"). Think of it like a coat-check ticket: you get a stub now, and the coat
   (the send) is released only when the matching stub is presented with an approval.

3. **Hand it to an external boss (external authority).** If a bigger supervisor/dashboard is
   attached, Wasl can surface the approval decision to *it* instead, and let it decide.

The gate always picks the strongest available tier. Crucially, **a request that isn't approved
is never sent.** If a client can't be prompted at all and no other tier applies, the safe
default is to *not* send.

*Why it exists:* these agents can take real actions (run migrations, move files, trigger other
agents). Letting them message each other freely, unsupervised, is exactly the kind of thing you
want a human in the loop for. The gate is that loop.

### 2.4 The audit log, and why "durability-first" matters

Every decision the gate makes — approved, denied, dismissed — and every interaction is written
to an **audit log**: an append-only record, in plain human-readable form, of who did what to
whom and whether it was allowed.

"Append-only" means you only ever add lines; you never edit or delete history. That makes it a
trustworthy record of what actually happened.

**"Durability-first"** is a specific, important rule about *ordering*: when a decision is made,
Wasl writes it to durable storage **first**, and only *then* proceeds. If writing to the log
fails, the whole operation fails — Wasl would rather refuse to act than act without a durable
record of it. Put plainly: **Wasl never does something it couldn't afterwards prove it did.** A
decision that isn't safely written down is treated as a decision that didn't happen.

The audit log is also the same mechanism an outside dashboard would use to *observe* everything
Wasl does — the log is written through a generic "sink" (a place-to-write-records) that anyone
can plug into. So "the audit log" and "the observation feed for an external dashboard" are the
same thing.

### 2.5 A2A (agent-to-agent), and what it replaced

For agents to reach each other *across machines*, Wasl needs a network protocol. It uses **A2A**
("agent-to-agent") — a standard-shaped protocol running over ordinary HTTP with JSON messages
(plus a streaming channel for live updates). Every Wasl instance runs a small A2A **server** so
other instances can reach it, and uses an A2A **client** to reach out to others.

A2A **replaced** an earlier, home-grown networking layer. Wasl originally had its own bespoke
"socket mesh" and separate daemon/mailbox components for moving messages around. Those were
**retired and deleted** in favor of A2A over HTTP, because a standard HTTP+JSON protocol is far
simpler to reason about, test, and interoperate with than a custom socket protocol. (You'll
still see mentions of the old `wasl-daemon`/`wasl-mailbox`/`wasl-mesh` in historical spec notes
marked "SUPERSEDED" — those parts are gone.)

---

## 3. How the system works today (before the newest feature)

Wasl is built as a set of small Rust libraries ("crates"), each one a self-contained building
block with its own tests. Rather than list them all, here's what the important ones *do* and how
a real request flows through them.

### 3.1 The building blocks, in plain terms

- **A node.** A **node** is one running Wasl instance on one machine. Every machine that
  participates runs a node. A node is *two things at once*: it's an **A2A server** (the receive
  side — how other nodes reach it) and it's an **MCP server** (the send side — how the local
  assistant CLI attaches to it). "MCP" is the Model Context Protocol — the standard way these AI
  CLIs talk to tools. So the local Claude Code connects to the node *as an MCP client*, and sees
  Wasl's four actions (consult/command/message/transfer) as tools it can call.

- **The MCP server (`wasl-mcpd`).** This is the actual runnable program — the node you launch.
  It reads a config file to learn about its peers, picks which assistant CLI it drives, exposes
  the four tools over MCP, runs the gate, writes the audit log, and dispatches over A2A. It's
  deliberately a thin shell: it doesn't invent any logic of its own; it wires together the
  tested building blocks below it.

- **The gate + audit "spine."** The piece that, for every outgoing action, runs the three-tier
  gate (§2.3) and records the decision to the audit log (§2.4). This is the heart of "permission
  before send."

- **The registry.** A node's list of the peers it knows about — each peer's id and its A2A
  address (where to reach it on the network).

- **Dispatch.** The send-side fan-out: once the gate approves, this resolves each target peer to
  its network address, builds the A2A message (tagging it with the verb — consult/command/etc —
  and the sender's id), sends to all recipients concurrently, and collects their replies.

- **The adapter (the "last inch").** Here's a subtlety. The AI CLIs are MCP *clients*, not
  servers — they can't receive an incoming A2A request on their own. So on the *receiving* side,
  the node needs a piece that takes an incoming request and actually feeds it into the local CLI,
  then returns the CLI's reply. That piece is the **adapter** — the "last inch" between the
  network and the real assistant. There's a concrete adapter per CLI (one for Claude, one for
  Codex, one for antigravity, one for opencode). Today, each adapter works by **spawning a
  brand-new headless CLI process for each incoming request** — remember this, it's the key to
  understanding the newest feature in §5.

### 3.2 One request, end to end

Let's walk a single realistic consult from Alice's agent to Bob's agent, all the way through:

1. **Alice's assistant calls the tool.** Claude Code on Alice's machine decides it needs
   something from Bob, so it calls the `consult` tool that Alice's node exposes over MCP:
   "consult Bob: what's the billing schema version?"

2. **The gate runs.** Alice's node builds a secret-free summary of the request ("consult 1
   peer", plus a preview of the question) and runs the three-tier gate. Say Alice's CLI supports
   in-band prompts, so tier 1 fires: Alice the human sees "Approve consulting Bob?" and clicks
   yes.

3. **The decision is written to the audit log — first.** Before anything is sent, Alice's node
   durably records "Alice consulted Bob → Approved." (If that write had failed, the whole
   consult would fail here, and nothing would be sent — durability-first.)

4. **Dispatch sends it over A2A.** Only now, because it was approved, does Alice's node look up
   Bob in its registry, find Bob's A2A address, and send an A2A message across the network
   carrying the question, tagged "kind = consult, from = Alice."

5. **Bob's node receives it.** Bob's node is running an A2A server. It accepts the incoming
   request and hands it to Bob's **adapter** — the last inch.

6. **The adapter feeds it to Bob's real CLI.** Bob's adapter spawns a fresh headless opencode
   process, gives it the question, waits for the answer, and returns the reply text.

7. **The reply travels back.** The reply goes back over A2A to Alice's node, which returns it to
   Alice's assistant as the result of its `consult` tool call. Alice's assistant now has Bob's
   answer.

That's the full loop: **tool call → gate → audit → A2A send → receive → feed to CLI → reply →
back.** "Local" and "cross-host" are the *same code path* — a local send is just A2A over the
loopback address; a cross-host send is A2A over the real network. A broadcast is the same thing
fanned out to many peers at once.

---

## 4. What Wasl supports today — and what it explicitly does NOT

Pulled from the specs' own stated scope and non-goals, not guessed:

**Supported today:**

- The four actions (consult, command, message, transfer) between agents, on one host and across
  hosts.
- Multicast / broadcast (one sender → many peers).
- Direct host-to-host file transfer (not routed through Wasl).
- The full three-tier permission gate, with the guarantee that an unapproved action never sends.
- A durable, human-readable audit log of every decision, with the durability-first ordering
  guarantee.
- An optional generic seam so an external supervisor/dashboard can approve gates and observe
  everything — without Wasl depending on any specific one.

**Explicitly NOT supported (documented non-goals, honest limitations):**

- **Wasl is fully standalone by design — it names and requires no external dashboard.** Support
  for an external supervisor exists only as an *optional* generic plug-in seam.
- **The four concrete CLI adapters do not, in the base system, hold a live conversation.** Each
  request spawns a fresh headless process; continuity is faked by "resuming" a stored session,
  not by keeping a real conversation alive. (This is precisely the gap the newest feature fills —
  §5.)
- **A big class of things is honestly marked "UNTESTED HERE"**: real cross-host over SSH/WAN and
  real `scp`/`rsync` between two genuinely different machines. All cross-host testing uses
  loopback (two ports on one machine standing in for two hosts — same code path), and file
  transfer is tested with stand-in commands. The project is deliberately careful not to claim
  "it works end-to-end" on the strength of fakes alone.

---

## 5. The newest feature: "live" / managed-runtime sessions

Everything above is the stable, shipped system. Now the new part.

### 5.1 The problem it solves

Recall from §3.1 that today, when a request arrives, the adapter **spawns a brand-new headless
CLI process, uses it for that one request, and throws it away.** Every consult/command/message
is answered by a *fresh, disposable, invisible* conversation. There's no memory except what's
manually re-fed via a "resume" trick each time, no way to watch what's happening live, no notion
of the assistant being "busy," no queueing, no ability to interrupt.

Picture asking a colleague a question by, each time, hiring a brand-new temp who reads a summary
of the previous conversation, answers your one question, and then quits. It works, but it's not a
*conversation* — it's a series of amnesiac one-shots.

**The new "live" (or "managed-runtime") feature keeps ONE real assistant process alive across
many turns** — a genuine, persistent, multi-turn conversation that Wasl owns and holds open. It's
the difference between hiring a series of temps and having one colleague who stays in the room
and remembers everything. On top of that, a human can **watch** the live session (`attach`) and
**inject** their own input into it (`send`), all mediated safely by Wasl.

Key vocabulary for this feature:

- A **turn** is one back-and-forth: one prompt in, one reply out. A live session serves many
  turns over its lifetime.
- A **target** is one live managed session — one persistent assistant process Wasl is holding
  open, that turns get delivered into.
- **Model A** (the locked design choice): *Wasl owns the session.* The human observes and injects
  *through Wasl*. The human never types directly into the raw process. The rejected alternative,
  **Model B** (a human personally driving the assistant's terminal while Wasl also writes to it),
  is deliberately foreclosed — see the "single-writer" rule next.

### 5.2 The actor / single-writer idea (the "one worker, two inboxes" model)

The safety cornerstone of the whole feature is the **single-writer invariant**: for any live
session, **exactly one piece of code is ever allowed to write to it.** That one piece is called
the **actor** — one dedicated worker task per live target.

Why does this matter? If two things could write to the same live assistant's input at the same
time — say, an incoming request from another agent *and* a human typing — their inputs could
interleave and corrupt each other, like two people talking into the same microphone. The
single-writer rule makes that impossible.

Here's the "one worker with two inboxes" picture. The actor is a single worker. It has:

- an **inbox of commands** (a "mailbox"): submit-this-turn, cancel-this-turn, shut-down,
  answer-a-permission-prompt, and so on — including the human's `send` (which arrives as just
  another turn to submit), and
- a **feed from the live assistant** (the "transport"): the stream of stuff the assistant emits
  — "I'm ready," "here's some output," "I finished the turn," "the process died."

The worker loops: it either handles a command from its mailbox, or it reads the next event from
the assistant's feed, and it's the *only* one holding the pen. Two logical sources of input
(another agent, and the human) get *serialized* — lined up one behind the other — by this single
worker. That's what makes concurrent agent + human input on one live session safe.

The actor also owns a bunch of bookkeeping: the current turn plus a FIFO queue of waiting turns
(so if the session is busy, new turns line up in order); a **journal** of everything that
happened (numbered events, so a late-joining observer can replay from where they left off with no
gaps and no duplicates); a dedupe map (so an accidentally-resent identical turn doesn't run
twice); and the current status.

One more honest limitation, stated loudly in the design: **all of this live state is in memory
only. If the Wasl node restarts, live sessions are abandoned** — the child process is gone with
its parent, and there's no rebuilding it. This is the *first* part of Wasl whose source of truth
is *not* the durable audit log. There's exactly one safety carve-out: for a **command**
specifically (which might cause a real side effect), a durable "this command was about to run"
record is written to the audit log *before* it's injected — so after a crash, Wasl can tell that
a command *might* have run and refuse to blindly re-run it.

### 5.3 What a "driver" is, and the three that exist

The actor is **generic** — it knows nothing about any specific assistant CLI. It only speaks in
normalized, abstract events ("a turn was accepted," "here's an output chunk," "the turn
completed"). But real CLIs each have their own wire format: Claude speaks one JSON dialect,
opencode speaks another.

A **driver** is the translator: the code that knows how to actually talk to *one specific real
CLI's* input and output format, and maps that CLI's native wire onto the abstract events the
actor understands. The driver is the *only* place CLI-specific knowledge is allowed to live.

Three drivers exist:

1. **The Claude driver** (`ClaudeStreamProtocol`) — drives the real `claude` CLI over its
   streaming-JSON interface.
2. **The opencode driver** (`OpenCodeAcpProtocol`) — drives the real `opencode` CLI over its
   ACP (a JSON-RPC) interface. (This one is the "primary" real driver because it genuinely
   streams partial output; Claude's interface only sends whole blocks.)
3. **A fake/test-only driver** (`FakeLineProtocol`) — *not* a real CLI. It's an in-repo fake
   "assistant" that speaks a trivial line-based protocol, used to test the entire generic
   machinery deterministically (no real AI, no network flakiness, no cost). It can be told to
   remember facts across turns, to hold a turn open, to crash mid-turn, to raise a permission
   prompt — everything needed to prove the actor behaves correctly.

**Why genericity was a deliberate goal:** the point of the abstract-event design is that *the
same actor core works for every driver, unchanged.* Adding a new CLI should mean writing a new
driver (a translator) and nothing else — the queueing, journaling, single-writer safety, cancel
logic, etc. never need to change per CLI. The project even tested this on purpose by building two
*structurally different* real drivers (Claude's streaming-JSON vs opencode's JSON-RPC): if the
abstraction had leaked, the second driver would have forced changes to the "generic" core. It
didn't — which is the evidence the abstraction is real, not just Claude concepts renamed.

To let a human actually use a live session standalone, there's also a small command-line tool
(`wasl session ...`) with commands like `start`, `list`, `attach` (watch a session), `send`
(inject a turn as a human), `interrupt`, `stop`, and `gate` (answer a permission prompt). Because
the human's tool and the running session may be in different processes, they talk over a small
local control API. Every one of those commands routes through the single actor — the human never
gets a raw back-channel to the process.

---

## 6. The current, real, not-yet-fixed problems

This is the heart of the document. The two real drivers were, for the first time, run
end-to-end against the *real* `claude` and `opencode` CLIs by a human outside the sandbox. That
shook out one bug that's already fixed (an event-loss race) and a set of others that are
**identified but not yet fixed**, captured in the hardening plan. Below, each is translated into
plain language: what's broken, why a normal person would care, and roughly how bad it is. I've
kept the plan's grouping (Issue A, Issue B, the actor-core bugs, the driver-supervision bugs).

A note on confidence: I checked the plan's claims against the actual source. They hold up — for
example, the shutdown bug really is present (the shutdown code genuinely never touches the live
process), and the mailbox-race bug really is there (the actor really does wait on the assistant's
feed as a standalone step, not raced against its command inbox).

### Issue A — the drivers don't recognize tool calls or permission prompts

**What's broken:** When a real assistant, mid-turn, decides to *use a tool* (run a shell
command, read a file) or *ask permission* to do something, it emits a specific message on its
wire. Neither real driver knows how to recognize those messages yet. The translation tables in
the Claude and opencode drivers simply don't have entries for "the CLI is calling a tool" or
"the CLI is asking permission."

**Why it matters:** A turn that makes the CLI invoke a tool or hit a permission prompt is
effectively *silently dropped* on the floor. Worse, when a CLI asks for permission over a
non-interactive pipe and *nobody ever answers* (because Wasl doesn't even see the question), the
CLI can **hang** or misbehave. To a user this looks like: "I asked the agent to do something real,
and it just... froze, or gave nothing back, with no explanation."

**How bad:** Significant, and it's the *largest* item — but it's not a quick code fix. It needs a
real capture of what these messages actually look like on the wire (the original feasibility
spike used prompts that never triggered a tool or a gate, so this shape was never recorded), and
then wiring that shape into the drivers' translation tables. It's scheduled last precisely
because it requires new field research, not just editing known code. It's also the reason a
couple of the actor-core issues below "don't bite yet" — the machinery to track tools can't be
exercised by a real CLI until real tool calls can even be *seen*.

### Issue B — a live session can't be interrupted or shut down while it's waiting

**What's broken:** Remember the actor is "one worker with two inboxes" — a command mailbox and the
assistant's event feed (§5.2). The bug: in its main loop, the worker waits on the assistant's
feed as a **standalone step** — it does *not* wait on the feed and the command mailbox *at the
same time*. (Confirmed in the source: the loop does `next_event().await` on its own line, not
raced against the mailbox.)

Now here's the trap. Both real drivers correctly wait up to a long "hang watchdog" budget
(around 60 seconds) for the assistant to produce its next event. During that whole wait, the
worker is *stuck on the feed* and literally cannot look at its command mailbox.

**Why it matters:** Any command the human or system sends during that wait — **cancel this turn**,
**shut this session down**, or a future "answer the permission prompt" — just sits in the mailbox
unserviced until the assistant happens to emit an event or the 60-second watchdog expires. To a
user: "I hit interrupt (or stop), and nothing happened for up to a minute." The very controls
that are supposed to let you rein in a live session don't respond while the session is thinking.

**How bad:** High-impact for usability, and it's genuinely tricky to fix safely. The fix is to
have the worker wait on *both* inboxes at once (so a command can preempt a blocked feed-read).
The subtlety — and why it isn't trivial — is a pre-existing invariant: "once you start reading an
event, never abandon it half-read." Racing the two inboxes means one side gets cancelled when the
other wins; the implementer has to prove that cancelling the feed-read only ever happens at a safe
point (between whole events, never mid-event) — and must verify that *concretely against each real
driver*, not just argue it in theory. There's a telling data point in the plan: naively applying
this fix to the fake test driver broke four tests that answer a permission/tool prompt mid-turn —
evidence that the interaction is delicate.

### The actor-core bugs (these affect *every* driver, because they live in the shared actor)

#### C1 — a plain emoji or accented character can crash the whole session

**What's broken:** The actor caps how big a chunk of assistant output it keeps, by trimming the
text to a maximum number of *bytes*. But it does this with a raw byte-length cut on a text string.
In Rust, a text string can't be cut in the middle of a multi-byte character — doing so
**panics** (crashes the task). Many perfectly ordinary characters take multiple bytes: accented
letters (é, ñ), Chinese/Japanese/Korean text, emoji. (Confirmed present in the source.)

**Why it matters:** If assistant output happens to land a multi-byte character *straddling the
byte cap*, the actor task crashes — and that kills the entire live session. This isn't exotic
input; it's completely normal model output. To a user: "The session randomly died in the middle
of a response, and the only pattern is that it involved non-English text or an emoji."

**How bad:** High priority, but a trivial fix — trim at the nearest safe character boundary
instead of a blind byte cut. It's slated to be fixed first because it's cheap and it's a genuine
crash.

#### C2 — "shutting down" a session doesn't actually stop the assistant process

**What's broken:** When you shut a target down, the shutdown code marks it stopped, closes the
event stream, and reports "Stopped" — but it **never touches the live assistant process at all.**
(Confirmed directly in the source: the stop routine has no reference to the transport/process.)
After that, the worker just parks forever, still holding the live child process alive, until —
maybe — the last handle to it gets dropped somewhere else, which might never happen promptly.

**Why it matters:** "Stop" is a lie. The UI says the session is stopped, but the real CLI process
(and everything *it* spawned) keeps running, holding memory and possibly doing work. To a user or
operator: "I stopped ten sessions and my machine is still running ten `claude` processes." This is
a real resource leak dressed up as a clean shutdown.

**How bad:** High operational impact. The fix is to make the process handle something the shutdown
can actually drop/kill, and to decide that an *immediate* shutdown should end the worker task
promptly rather than parking it forever. Both drivers already correctly kill and reap their child
when their handle is dropped — the problem is purely that shutdown never drops it.

#### C3 — cancelling a turn moves on before the cancel actually takes effect

**What's broken:** When you cancel the current turn, the actor asks the assistant to interrupt and
then *immediately* clears the turn and starts the next queued one — without waiting for the
cancelled turn to actually finish. But "interrupt" is explicitly best-effort, not instant: the
old turn may still emit a final "I'm done" event a moment later.

**Why it matters:** That late "done" event from the *cancelled* turn can get **misattributed to
the newly-started turn** — so the new turn looks like it completed when it never even ran, or its
result gets mixed up. To a user: "I cancelled turn 1 and started turn 2; turn 2's result is
garbage / claims it finished instantly." Correctness bug in a corner case, but a nasty one because
it silently mislabels results.

**How bad:** Medium-high. The fix is to add an explicit "cancelling" waiting state that only moves
on to the next turn once the cancelled turn's *own* terminal event (or a timeout) is seen — the
same disciplined pattern the actor already uses for waiting on a permission gate.

#### C4 — answering a permission prompt reports success even if the answer failed to send

**What's broken:** When the actor answers a native permission prompt, it sends the answer to the
assistant and then reports "gate resolved" **regardless of whether that send actually worked.**

**Why it matters:** If the write to the assistant genuinely failed (broken pipe, dead process),
Wasl still tells everyone the prompt was answered and the session should proceed — but the
assistant never got the answer and is still stuck. To a user: "It says my approval went through,
but the agent is still frozen waiting for permission." A silent lie about a safety-relevant
action.

**How bad:** Medium. The fix is to check whether the send succeeded and, if not, keep the session
in its waiting state (or fault it) instead of falsely claiming resolution. (There's a related
concern — whether the answer is even in the right wire format — but that's really part of Issue A,
since a driver that can't recognize a gate yet has no gate-answer format to get wrong.)

#### C5 — the actor tracks only one running tool at a time

**What's broken:** The actor remembers a running tool call in a **single slot** (one optional
name). If two tool calls are active at once, or two same-named calls happen in sequence, the
second silently overwrites the record of the first. (Confirmed: it's literally one optional-string
field, keyed by name only, with no per-call id.)

**Why it matters:** With no way to tell two concurrent tool calls apart, their start/finish events
get confused. Practically, this **doesn't bite the real CLIs yet** — because of Issue A, they
can't even emit tool events today — but it already affects the fake driver's tool tests, and it
will matter the instant Issue A lands.

**How bad:** Lower priority, and scope-gated by Issue A (no point hardening tool tracking before
tools can appear). The fix is to track pending tool calls by a real per-call id (a map) instead of
one shared slot.

### The driver-supervision bugs (in the real drivers that babysit the child process)

These are all in the two real drivers' "process supervision" code — the part that spawns, watches,
and eventually kills the child CLI process. The recurring theme is **"every wait must be
bounded"**: a supervisor that can wait *forever* on a misbehaving child is a hang waiting to
happen.

#### D1 — waiting on a half-dead child forever (stdout closed but process alive)

If the child closes its output stream but doesn't actually exit — a real category of misbehavior
(a hung cleanup handler, a wedged process) — the supervisor waits for it to exit with **no time
limit and no way to force a kill.** That hangs supervision permanently. To a user: a session
that's dead but that Wasl can never let go of. **High priority.** Fix: treat an unexpected output
close as a failure and route it through the same bounded "escalate to kill" ladder used elsewhere,
instead of an unbounded wait.

#### D2 — the very last "kill" wait is also unbounded

After sending the final SIGKILL, the "last resort" wait for the process to actually die has no
time limit. In practice SIGKILL essentially always works, so this almost never matters — but for
consistency with the "every wait is bounded" rule, it should get a final deadline too, with an
error surfaced if the OS somehow still hasn't reported the exit. **Lower priority** (same root
cause as D1).

#### D3 — writing to a stuck child's input can hang forever (and it compounds Issue B)

When the actor sends a prompt to the child, the actual write to the child's input pipe has **no
timeout.** If the child's input buffer is full and it has stopped reading (a genuinely stuck
child), that write blocks forever. And because of Issue B (the worker isn't watching its command
mailbox during blocking operations), a stuck write here would **freeze the entire worker with no
watchdog able to intervene** — the hang watchdog only guards waiting for *events*, not waiting on
a *write*. To a user: a totally frozen, uncancellable session. **High priority.** Fix: put a time
limit on the write and surface a transport error if it's exceeded.

#### D4 — unbounded buffers let a runaway child eat all memory

The channel the driver uses to hand events to the actor is *unbounded*, and the reader that splits
the child's output into lines has *no cap on line length*. A verbose or malfunctioning child — not
even necessarily malicious; a real bug in the CLI, or a runaway tool loop spewing output — can
grow Wasl's memory without limit. The actor's own size caps only kick in *after* the fact, too
late to help here. To an operator: "one bad session ballooned memory and took down the node."
**Medium** (real, but lower likelihood). Fix: bound the channel and cap the per-line size, failing
the turn / killing the child on a pathologically huge line instead of buffering it all.

#### D5 — opencode's reply accumulator grows unbounded before any cap applies

The opencode driver has to accumulate reply text itself (because opencode's response doesn't carry
the reply the way you'd expect). That accumulator has no size cap of its own — the actor's cap only
trims the *published* event afterward, so the accumulator can balloon first. **Medium.** Fix: cap
the accumulator at the driver level so it stops growing early. (Same family of problem as D4.)

#### D6 — opencode's startup handshake breaks on any out-of-order message

When the opencode driver first connects, it does a handshake and *assumes the very next line it
reads is the reply to its request.* But this protocol legitimately allows the server to send
notifications at any time, including in the middle of a handshake. If one arrives first, the
handshake mis-reads it and **the whole session fails to start.** To a user: "opencode sessions
randomly fail to launch." **Medium-high.** Fix: loop reading until the actually-expected reply
shows up, ignoring interleaved notifications along the way.

#### D7 — opencode's error output isn't drained until too late

The opencode driver only starts draining the child's error-output stream *after* the handshake
finishes. If the child writes enough to that stream during startup to fill the OS pipe buffer, the
child **blocks on its own error write**, which stalls the handshake — and it surfaces as a
misleading "handshake timeout" instead of the real cause. **Lower-medium.** Fix: start draining
error output immediately, before the handshake.

#### D8 — replies are matched to requests by assumption, not by checking

The opencode driver doesn't verify that a response's id matches the request it's waiting for; it
relies on "there's only ever one request outstanding at a time," which is true *today* by
construction. It's correct now but **silently fragile** — a future change that broke that
assumption would misroute responses with no warning. **Lower priority, defense-in-depth.** Fix:
actually check the id.

#### D9 — kill signals hit only the main process, not its children

When escalating to a kill, the driver signals only the single main child process. But there's
direct evidence that these CLIs spawn *their own* subprocesses (Claude, for instance, launches
its own helper servers). Signalling only the leader can **orphan those children** — leaving
zombie helper processes behind. To an operator: "I killed the session but there are still stray
helper processes running." **Medium** (real evidence exists). Fix: put the child in its own
process group and signal the whole group.

#### D10 — the kill uses the `kill` command found on `$PATH`

To send signals, the driver shells out to the external `kill` program looked up via the system
`PATH`, rather than calling the kill system call directly. One reviewer flagged this as a
security risk (a `PATH` hijack); the plan honestly recalibrates it — exploiting it would require
an attacker who *already* controls your `PATH`, i.e. someone who's already substantially in. Still,
calling the system call directly is simply more robust (no extra process, no `PATH` dependency).
**Lower priority, hardening not urgency.**

### How the plan sequences the fixes

The hardening plan front-loads the cheapest, highest-confidence, highest-impact fixes and defers
the one requiring new field research (Issue A) to the end:

1. Actor-core safety (C1 the UTF-8 crash first as it's trivial, then C2 shutdown-doesn't-stop, C3
   cancel confirmation, C4 gate-resolve error handling).
2. Issue B (the mailbox race), plus bringing the fake driver in line afterward.
3. The three genuinely-unbounded-hang driver bugs together (D1, D2, D3).
4. The unbounded-memory bugs (D4, D5).
5. opencode handshake robustness (D6, D7, D8).
6. Process-group signalling and the direct-kill hardening (D9, D10).
7. Last, and largest: Issue A (capture and map the tool/gate wire shapes) together with C5
   (per-call tool tracking), since these need new real-CLI research, not just code fixes.

---

## 7. Closing: how sound is this, really?

My honest read: **the newest feature is fundamentally sound in its design, with a cluster of very
real but fixable rough edges — most of which are exactly the kind of thing that only surfaces the
first time real, unpredictable software is put in front of it.**

The core architecture is the right one and it's holding up: the single-writer actor gives a clean
safety story; the generic-core-plus-per-driver-translator split was validated by building two
structurally different real drivers without the core leaking; the honesty discipline (marking
capabilities `false` until *proven* true, refusing to claim `streaming` or `interruptible` on
unverified evidence) is genuinely disciplined engineering, not hand-waving.

But the bug list is not cosmetic. Several items are true crashes or hangs that a normal user *will*
hit: an emoji crashing a session (C1), "stop" not actually stopping anything (C2), interrupt/stop
not responding for up to a minute (Issue B), and — most fundamentally — the drivers not yet
recognizing tool use or permission prompts at all (Issue A), which means a large and common class
of real agent behavior currently just... doesn't work. That last one is the biggest gap between
"demoed successfully" and "actually usable for real work," because most useful agent turns involve
running tools.

The reassuring part is *how these were found and framed*: they came from a real end-to-end run,
they're written down with specific source-line evidence, one reviewer claim that was actually
wrong was caught and excluded, and they're sequenced sensibly (cheap crash-fixes first, the
research-heavy item last). That's the profile of a feature that's structurally healthy and honestly
managed — not one that's rotten underneath. The work remaining is hardening and filling in the
tool/gate story, not rethinking the design. If I had to summarize in one line: **the skeleton is
solid; the feature is not yet safe to leave unattended in production, and the plan to get it there
is credible.**
