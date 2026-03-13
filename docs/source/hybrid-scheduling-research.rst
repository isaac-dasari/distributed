Hybrid Scheduling Research Notes
================================

This page records the external work that is most relevant to hybrid scheduling
in Dask Distributed and how each source should influence implementation choices.

Primary Questions
-----------------

1. Is the bottleneck really centralized scheduling overhead?
2. When does bounded local decision-making outperform per-task central control?
3. How can that be added without weakening fairness and observability too much?

Most Relevant Papers
--------------------

Sparrow (SOSP 2013)
~~~~~~~~~~~~~~~~~~~

Ousterhout et al., *Sparrow: Distributed, Low Latency Scheduling*

Why it matters:

- focuses directly on low-latency task scheduling
- explains why centralized per-task scheduling becomes expensive for short tasks
- provides a useful contrast for Dask, even though Dask cannot adopt Sparrow's
  fully decentralized assumptions wholesale

What to take from it:

- task launch latency deserves first-class treatment
- probe- or batch-oriented ideas are useful, but Dask still needs stronger
  central semantics than Sparrow

Firmament (OSDI 2016)
~~~~~~~~~~~~~~~~~~~~~

Gog et al., *Firmament: Fast, Centralized Cluster Scheduling at Scale*

Why it matters:

- shows that centralized scheduling can still scale if the control algorithm and
  data structures are optimized aggressively

What to take from it:

- do not assume decentralization is the only route to scale
- semantic batching and hot-path optimization may produce a large fraction of
  the available benefit before leases are required

Llumnix (OSDI 2024)
~~~~~~~~~~~~~~~~~~~

Sun et al., *Llumnix: Dynamic Scheduling for Large Language Model Serving*

Why it matters:

- demonstrates a practical global-scheduler plus local-scheduler split
- matches the design instinct behind bounded execution leases

What to take from it:

- global policy plus local execution control is viable
- the boundary must be explicit and narrow

Programmable and Adaptive Scheduling (HotNets 2025)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The HotNets 2025 hybrid scheduling paper cited in the local design notes is
relevant to deciding how much control should remain centralized.

What to take from it:

- globally sensitive policy should remain centralized
- bounded local choices can improve throughput without the full cost of complete
  decentralization

Control-path latency in fast runtimes (OSDI 2025)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The AFaaS-related paper cited in the local design notes is useful because it
reinforces a pattern seen in Dask as well: once the byte path is reasonably
efficient, control latency dominates small work.

What to take from it:

- optimize control-path cost explicitly
- measure message count and scheduler CPU as first-class metrics

Research Follow-ups Still Worth Checking
----------------------------------------

- scheduling work on fairness under bounded local discretion
- queueing and occupancy models for bursty short-task systems
- traces from modern Dask tiny-task workloads to validate assumptions

Research Conclusions for Dask
-----------------------------

- Dask should not jump straight to a decentralized scheduler
- Dask should first exploit semantic batching and hot-path reduction
- if leases are added, they should start as single-worker, short-lived, and
  tightly constrained
- any lease design must preserve scheduler visibility and attribution
