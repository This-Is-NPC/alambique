---------------------------- MODULE Pipeline ----------------------------
(***************************************************************************)
(* Pipelined commits of one cell: leases, the fence, out-of-order puts,   *)
(* the chain a restore computes, and in-order acknowledgement.            *)
(* How to run and how the actions map to code: docs/how-to-check-the-     *)
(* pipeline-model.md.                                                     *)
(*                                                                         *)
(* Simplifications, all on the safe side or irrelevant to the properties: *)
(* - The store is linearizable: a LIST or GET sees every completed PUT.   *)
(* - A restore is one atomic read of the store.                           *)
(* - Every unreachable segment after the chain is a tail (the window of   *)
(*   16 files and the level-0 rule, which keep real history loss loud,    *)
(*   are not modelled).                                                    *)
(* - A stale owner keeps encoding and acknowledging until it is fenced;   *)
(*   the real cell also stops at its lease deadline.                      *)
(* - Callers are not modelled, only the kind of answer: `refused` holds   *)
(*   the batches that were answered with a definite error.                *)
(* - An upload failure or a fenced cell stops the cell, so a failure is a *)
(*   Crash: the volatile state is gone and requests in flight stay.       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Nodes,      \* the nodes that can own the cell
  MaxEpoch,   \* how many leases are granted in total
  MaxTx,      \* number of TXIDs in the store (fences take one too)
  Depth,      \* pipeline depth: batches encoded beyond the acked prefix
  Bug         \* "none", or a mutation that the invariants must catch

Bugs == {"none", "AckOutOfOrder", "OverwriteAny", "NoFence", "ChainNoEpoch",
         "ChainNoPre", "ChainNoEpochNoPre", "RefuseSent"}
ASSUME Bug \in Bugs /\ Depth \in 1..MaxTx /\ MaxEpoch \in Nat /\ MaxTx \in Nat


\* A segment. `pre` is the checksum the writer chained from, `Post` the one
\* it leaves. A fence changes no page, so it leaves the checksum it found
\* (`neu`). So can a batch that rewrites pages with the same bytes, or
\* changes and reverts them: then a stale owner's batch can carry the
\* checksum the new owner's chain ends with, and only the epoch rule of the
\* chain rejects it.
Genesis == [t |-> 0, ep |-> 0, by |-> "none", kind |-> "genesis", neu |-> TRUE, pre |-> <<0, 0>>]
NoSeg == [t |-> 0, ep |-> 0, by |-> "none", kind |-> "none", neu |-> TRUE, pre |-> <<0, 0>>]
Post(s) == IF s.neu THEN s.pre ELSE <<s.ep, s.t>>
PostVals == (0..MaxEpoch) \X (0..MaxTx)
Seg == [t : 1..MaxTx, ep : 1..MaxEpoch, by : Nodes,
        kind : {"fence", "batch"}, neu : BOOLEAN, pre : PostVals]

\* A put in flight: create when cas = NoSeg, else replace the object `cas`
\* (if-match). `fl` is the epoch of the segment's predecessor (floor epoch).
Req == [seg : Seg, fl : 0..MaxEpoch, cas : Seg \cup {NoSeg}]

VARIABLES
  store,      \* [1..MaxTx -> Seg \cup {NoSeg}]
  net,        \* requests sent and not yet applied; any of them may land late
  resp,       \* answers sent and not yet read by the sender
  leaseEp,    \* last epoch granted
  holder,     \* [0..MaxEpoch -> Nodes \cup {"none"}] who got each epoch
  phase,      \* [Nodes -> {"Down","Acquired","Fencing","Serving"}]
  ep,         \* epoch of each node
  enc,        \* last encoded segment (or the restored head)
  q,          \* FIFO of [seg, fl, landed], the batches not yet acknowledged
  fenceAt,    \* TXID of the node's fence
  ackedSegs,  \* history: every acknowledged segment
  refused     \* history: batches whose callers got a definite refusal

vars == <<store, net, resp, leaseEp, holder, phase, ep, enc, q, fenceAt,
          ackedSegs, refused>>

(* The chain a restore computes: the longest contiguous run from the start
   whose checksums chain and whose epochs do not decrease. The rest is a tail
   and is ignored. *)
Link(prev, s) ==
  IF s = NoSeg THEN FALSE
  ELSE /\ (Bug \in {"ChainNoPre", "ChainNoEpochNoPre", "RefuseSent"} \/ s.pre = Post(prev))
       /\ (Bug \in {"ChainNoEpoch", "ChainNoEpochNoPre"} \/ s.ep >= prev.ep)

Prev(st, i) == IF i = 1 THEN Genesis ELSE st[i - 1]

ChainLen(st) ==
  CHOOSE k \in 0..MaxTx :
    /\ \A i \in 1..k : Link(Prev(st, i), st[i])
    /\ k = MaxTx \/ ~Link(Prev(st, k + 1), st[k + 1])

ChainHead(st) == IF ChainLen(st) = 0 THEN Genesis ELSE st[ChainLen(st)]

----------------------------------------------------------------------------
Init ==
  /\ store = [i \in 1..MaxTx |-> NoSeg]
  /\ net = {} /\ resp = {}
  /\ leaseEp = 0
  /\ holder = [e \in 0..MaxEpoch |-> "none"]
  /\ phase = [n \in Nodes |-> "Down"]
  /\ ep = [n \in Nodes |-> 0]
  /\ enc = [n \in Nodes |-> Genesis]
  /\ q = [n \in Nodes |-> <<>>]
  /\ fenceAt = [n \in Nodes |-> 0]
  /\ ackedSegs = {}
  /\ refused = {}

\* A node takes the lease with the next epoch. The previous holder is not
\* told: it may keep running.
Acquire(n) ==
  /\ phase[n] = "Down" /\ leaseEp < MaxEpoch
  /\ leaseEp' = leaseEp + 1
  /\ ep' = [ep EXCEPT ![n] = leaseEp + 1]
  /\ holder' = [holder EXCEPT ![leaseEp + 1] = n]
  /\ phase' = [phase EXCEPT ![n] = "Acquired"]
  /\ UNCHANGED <<store, net, resp, enc, q, fenceAt, ackedSegs, refused>>

\* Restore, check that the lease is still ours (Wake.ensure_lease, which runs
\* after the restore), then put the fence at head + 1 with if-none-match.
\* Also the "restore again" path after a fence was refused.
Restore(n) ==
  LET h == ChainHead(store)
      f == [t |-> h.t + 1, ep |-> ep[n], by |-> n, kind |-> "fence",
            neu |-> TRUE, pre |-> Post(h)]
  IN /\ phase[n] = "Acquired" /\ ep[n] = leaseEp /\ h.t < MaxTx
     /\ IF Bug = "NoFence"
          THEN /\ phase' = [phase EXCEPT ![n] = "Serving"]
               /\ enc' = [enc EXCEPT ![n] = h]
               /\ q' = [q EXCEPT ![n] = <<>>]
               /\ fenceAt' = [fenceAt EXCEPT ![n] = h.t + 1]
               /\ net' = net
          ELSE /\ phase' = [phase EXCEPT ![n] = "Fencing"]
               /\ enc' = [enc EXCEPT ![n] = f]
               /\ q' = [q EXCEPT ![n] = <<[seg |-> f, fl |-> h.ep,
                                           landed |-> FALSE]>>]
               /\ fenceAt' = [fenceAt EXCEPT ![n] = f.t]
               /\ net' = net \cup {[seg |-> f, fl |-> h.ep, cas |-> NoSeg]}
     /\ UNCHANGED <<store, resp, leaseEp, holder, ep, ackedSegs, refused>>

\* Encode the next batch right after the previous one, without waiting for
\* it to land, up to Depth batches beyond the acked prefix. Nothing is ever
\* encoded twice.
Encode(n) ==
  LET e == enc[n]
  IN /\ phase[n] = "Serving" /\ Len(q[n]) < Depth /\ e.t < MaxTx
     /\ \E neu \in BOOLEAN :
          LET s == [t |-> e.t + 1, ep |-> ep[n], by |-> n, kind |-> "batch",
                    neu |-> neu, pre |-> Post(e)]
          IN /\ enc' = [enc EXCEPT ![n] = s]
             /\ q' = [q EXCEPT ![n] = Append(@, [seg |-> s, fl |-> e.ep,
                                                 landed |-> FALSE])]
             /\ net' = net \cup {[seg |-> s, fl |-> e.ep, cas |-> NoSeg]}
     /\ UNCHANGED <<store, resp, leaseEp, holder, phase, ep, fenceAt,
                    ackedSegs, refused>>

\* Someone still waits for the answer to a request.
Relevant1(r) ==
  LET n == r.seg.by
  IN /\ phase[n] \in {"Fencing", "Serving"} /\ ep[n] = r.seg.ep
     /\ \E i \in 1..Len(q[n]) : q[n][i].seg = r.seg
Relevant(p) == Relevant1(p.req)

\* The store applies a request, in any order and at any time. Writers only
\* create; only an if-match request replaces an object, and only the very
\* object it read.
Apply(r) ==
  LET cur == store[r.seg.t]
      ok == IF r.cas = NoSeg THEN cur = NoSeg ELSE cur = r.cas
  IN /\ r \in net
     /\ net' = net \ {r}
     /\ store' = IF ok THEN [store EXCEPT ![r.seg.t] = r.seg] ELSE store
     /\ resp' = IF Relevant1(r) THEN resp \cup {[req |-> r, ok |-> ok, found |-> cur]}
                 ELSE resp
     /\ UNCHANGED <<leaseEp, holder, phase, ep, enc, q, fenceAt, ackedSegs, refused>>

\* An answer nobody waits for any more (sender crashed or restarted).
DropStale(p) ==
  /\ p \in resp /\ ~Relevant(p)
  /\ resp' = resp \ {p}
  /\ UNCHANGED <<store, net, leaseEp, holder, phase, ep, enc, q, fenceAt,
                 ackedSegs, refused>>

\* The batches of the cell that were sent and wait for an answer. A stop
\* answers their writers `outcome_unknown`; only the batch whose own put hit
\* another writer's object is refused definitively.
Sent(n) == {q[n][j].seg : j \in {k \in 1..Len(q[n]) : q[n][k].seg.kind = "batch"}}

Down(n) ==
  /\ phase' = [phase EXCEPT ![n] = "Down"]
  /\ enc' = [enc EXCEPT ![n] = Genesis]
  /\ q' = [q EXCEPT ![n] = <<>>]
  /\ fenceAt' = [fenceAt EXCEPT ![n] = 0]

\* The node crashes, or an upload fails and the cell stops. Requests in
\* flight stay in `net`: they may land after the next owner's fence.
Crash(n) ==
  /\ phase[n] # "Down"
  /\ Down(n)
  /\ refused' = IF Bug = "RefuseSent" THEN refused \cup Sent(n) ELSE refused
  /\ UNCHANGED <<store, net, resp, leaseEp, holder, ep, ackedSegs>>

\* The sender reads an answer. 412 handling: same bytes and epoch means it
\* landed; an object of an epoch below the predecessor's is a zombie and is
\* replaced with if-match; anything else means taken (fenced).
Learn(p) ==
  LET r == p.req
      n == r.seg.by
      i == CHOOSE j \in 1..Len(q[n]) : q[n][j].seg = r.seg
      landed == p.ok \/ p.found = r.seg
      zombie == p.found # NoSeg /\ (p.found.ep < r.fl \/ Bug = "OverwriteAny")
  IN /\ p \in resp /\ Relevant(p)
     /\ resp' = resp \ {p}
     /\ refused' = IF ~landed /\ p.found # NoSeg /\ ~zombie /\ r.seg.kind = "batch"
                     THEN refused \cup (IF Bug = "RefuseSent" THEN Sent(n) ELSE {r.seg})
                     ELSE refused
     /\ IF landed
          THEN /\ net' = net
               /\ IF r.seg.kind = "fence"
                    THEN /\ phase' = [phase EXCEPT ![n] = "Serving"]
                         /\ q' = [q EXCEPT ![n] = <<>>]
                         /\ UNCHANGED <<enc, fenceAt>>
                    ELSE /\ q' = [q EXCEPT ![n][i].landed = TRUE]
                         /\ UNCHANGED <<phase, enc, fenceAt>>
          ELSE IF p.found = NoSeg
          THEN /\ net' = net \cup {[r EXCEPT !.cas = NoSeg]}
               /\ UNCHANGED <<phase, enc, q, fenceAt>>
          ELSE IF zombie
          THEN /\ net' = net \cup {[r EXCEPT !.cas = p.found]}
               /\ UNCHANGED <<phase, enc, q, fenceAt>>
          ELSE /\ net' = net
               /\ IF r.seg.kind = "fence"
                    THEN /\ phase' = [phase EXCEPT ![n] = "Acquired"]
                         /\ q' = [q EXCEPT ![n] = <<>>]
                         /\ UNCHANGED <<enc, fenceAt>>
                    ELSE Down(n)
     /\ UNCHANGED <<store, leaseEp, holder, ep, ackedSegs>>

\* A retry after a timeout: the same bytes again.
Resend(n, i) ==
  /\ phase[n] \in {"Fencing", "Serving"} /\ i \in 1..Len(q[n])
  /\ ~q[n][i].landed
  /\ \A r \in net : r.seg # q[n][i].seg
  /\ \A p \in resp : p.req.seg # q[n][i].seg
  /\ net' = net \cup {[seg |-> q[n][i].seg, fl |-> q[n][i].fl, cas |-> NoSeg]}
  /\ UNCHANGED <<store, resp, leaseEp, holder, phase, ep, enc, q, fenceAt,
                 ackedSegs, refused>>

Min1(len) == IF len = 0 THEN 0 ELSE 1

\* The only place a write is answered: the oldest batch, once it landed.
Ack(n) ==
  /\ phase[n] = "Serving"
  /\ \E i \in IF Bug = "AckOutOfOrder" THEN 1..Len(q[n]) ELSE 1..Min1(Len(q[n])) :
       /\ q[n][i].landed
       /\ ackedSegs' = ackedSegs \cup {q[n][i].seg}
       /\ q' = [q EXCEPT ![n] = SubSeq(@, 1, i - 1) \o SubSeq(@, i + 1, Len(@))]
  /\ UNCHANGED <<store, net, resp, leaseEp, holder, phase, ep, enc, fenceAt,
                 refused>>

Next ==
  \/ \E n \in Nodes : Acquire(n) \/ Restore(n) \/ Encode(n) \/ Crash(n) \/ Ack(n)
  \/ \E r \in net : Apply(r)
  \/ \E p \in resp : Learn(p) \/ DropStale(p)
  \/ \E n \in Nodes, i \in 1..Depth : Resend(n, i)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* Invariants and properties *)

TypeOK ==
  /\ store \in [1..MaxTx -> Seg \cup {NoSeg}]
  /\ net \subseteq Req
  /\ \A p \in resp : p.req \in Req /\ p.ok \in BOOLEAN
  /\ leaseEp \in 0..MaxEpoch
  /\ phase \in [Nodes -> {"Down", "Acquired", "Fencing", "Serving"}]
  /\ ep \in [Nodes -> 0..MaxEpoch]
  /\ \A n \in Nodes : Len(q[n]) <= Depth
  /\ ackedSegs \subseteq Seg
  /\ refused \subseteq Seg

\* Every acknowledged TXID is on the chain any restore computes, with the
\* bytes of the writer that acknowledged it.
NoAckedCommitLost ==
  \A s \in ackedSegs : s.t <= ChainLen(store) /\ store[s.t] = s

\* Acknowledgements of one owner are a prefix in TXID order: no landed but
\* unacknowledged batch of its epoch lies below an acknowledged one, and
\* nothing it still queues is older than what it acknowledged. (A stale owner
\* may acknowledge late, after a newer owner did, so order is per epoch.)
InOrderAck ==
  /\ \A s \in ackedSegs : \A u \in 1..(s.t - 1) :
       (store[u].ep = s.ep /\ store[u].kind = "batch") => store[u] \in ackedSegs
  /\ \A n \in Nodes : \A i \in 1..Len(q[n]) : \A s \in ackedSegs :
       (phase[n] # "Down" /\ s.ep = ep[n]) => s.t < q[n][i].seg.t

AllSegs ==
  {store[i] : i \in {j \in 1..MaxTx : store[j] # NoSeg}}
  \cup {r.seg : r \in net} \cup {p.req.seg : p \in resp}
  \cup UNION {{q[n][i].seg : i \in 1..Len(q[n])} : n \in Nodes}

\* An epoch is granted once, only its holder writes under it, and it writes
\* each TXID once (no re-encode).
OneWriterPerEpoch ==
  /\ \A n, m \in Nodes : (ep[n] > 0 /\ ep[n] = ep[m]) => n = m
  /\ \A a \in AllSegs : holder[a.ep] = a.by
  /\ \A a, b \in AllSegs : (a.ep = b.ep /\ a.t = b.t) => a = b

\* A write refused definitively (`fenced`: the caller may repeat it) never
\* reaches the chain. A batch that was sent and not refused by its own put
\* may still land, so its writers get `outcome_unknown`.
NoRefusedDurable ==
  \A s \in refused : ~(s.t <= ChainLen(store) /\ store[s.t] = s)

\* R2: along the chain epochs never decrease.
ChainEpochsMonotone ==
  \A i \in 2..ChainLen(store) : store[i].ep >= store[i - 1].ep

\* Once an owner serves, its fence is above everything an older epoch
\* acknowledged: older owners collide on TXIDs the new owner holds.
FenceEffective ==
  \A n \in Nodes : phase[n] = "Serving" =>
    \A s \in ackedSegs : s.ep < ep[n] => s.t < fenceAt[n]

\* A reader's chain only grows: what a restore, a head read or a replica saw
\* stays a prefix of what the next one sees.
ChainPrefixStable ==
  [][/\ ChainLen(store) <= ChainLen(store')
     /\ \A i \in 1..ChainLen(store) : store'[i] = store[i]]_vars
=========================================================================
