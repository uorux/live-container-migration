# JVM options vs. CRIU pre-copy: lowering the dirty rate and the final dump

Context: the heavy modded server (Java 21, G1, `-Xmx10G`, about 5.6 GB RSS) re-dirties 1.3-2.5 GB per
~10 s pre-copy round. Observed rounds were 5.63 GB, 2.50, 2.19, 1.63, then a 1.31 GB final dump, for a ~14 s
freeze (dump 3 s + ship 5 s + restore 6 s). Measuring tool: `tools/dirtyrate` (commit below). Everything in
section 1 marked **verified** was checked against kernel/CRIU/HotSpot source and, where possible, by a
test on this machine (kernel 7.3, OpenJDK 21.0.12).

## 1. What CRIU actually dumps (the mechanics every option is judged against)

1. **Round 1** (`pre-1`) dumps every resident private page: present or swapped, not the shared zero page,
   and not a never-written page of a private file mapping. CRIU checks this 4 KiB page by 4 KiB page
   (`criu/mem.c` `should_dump_page` / `generate_iovs`). **Verified.**
2. **Later rounds and the final dump** dump only pages whose soft-dirty bit is set. All other pages become
   "in parent" holes (`page_in_parent`). The soft-dirty bit is set by any write after `clear_refs` (CRIU
   clears it at each pre-dump). **Verified.**
3. **A page freed by the GC but still resident and not rewritten is clean.** CRIU doesn't dump it again
   and restores it from the parent image (stale garbage, which is fine). HotSpot product builds don't zap
   freed heap (`ZapUnusedHeapArea` is a `develop` flag, `runtime/globals.hpp`). So "freed" costs nothing,
   but rewriting does. **Verified (source).**
4. **A page that is not resident is never dumped**: uncommitted (`mmap(PROT_NONE)` over it), `MADV_DONTNEED`,
   or never touched. Reserved-but-untouched heap is free. **Verified.**
5. **New or expanded VMAs are soft-dirty as a whole.** `mm/vma.c` `__mmap_complete` says "New (or expanded)
   vma always get soft dirty status", and `fs/proc/task_mmu.c` then reports *every* page of that VMA as
   soft-dirty. HotSpot commits memory with `mmap(MAP_FIXED)` over its `PROT_NONE` reservation
   (`os_linux.cpp` `commit_memory_impl`), so the new range merges with the adjacent committed heap VMA.
   **Committing even one more G1 region marks the whole contiguous committed heap dirty, and CRIU re-dumps
   all of it.** **Verified.** `TestVMAExpansionMarksAllSoftDirty`: after committing one adjacent page,
   256 of 256 old pages read soft-dirty. Also seen live: the `g1-grow` run below (`-Xms256m -Xmx2g`)
   re-dirtied the full 1.3 GiB heap in a single second, twice in 30 s.
   Uncommit (`mmap(PROT_NONE)`) only splits the heap VMA. That is harmless in itself, but the next
   re-commit triggers the hazard.
6. **THP: soft-dirty is tracked per 2 MiB PMD.** `clear_soft_dirty_pmd` write-protects the whole PMD, and
   pagemap copies `pmd_soft_dirty` to all 512 entries. CRIU still dumps at 4 KiB, but all 512 pages count
   as dirty. **Verified.** `TestTHPDirtyGranularity`: 4 one-byte writes gave 2048 dirty pages. The live
   `sparse-thp` run below showed a 24× higher dirty rate. Multi-size THP (16K-1M folios) are PTE-mapped,
   so they keep 4 KiB tracking here. All mTHP sizes are `never` on this box anyway.
7. **Shared memfd / shared anonymous mappings are not pre-copied at all.** CRIU dumps a memfd's whole
   content (every data page, no parent) in the final dump (`memfd.c` → `shmem.c`
   `dump_one_memfd_shmem`/`do_dump_one_shmem` with no `pstate_map`). `MAP_SHARED|MAP_ANONYMOUS` gets
   dirty tracking only with `CRIU_TRACK_SHMEM=1`, which CRIU calls buggy. **ZGC's heap is a memfd**
   (`/memfd:java_heap`), so the whole ZGC heap goes into the freeze. **Verified (source + `zgc` run:
   1.0-1.2 GiB in `shared:memfd:java_heap`).**
8. **`MAP_HUGETLB` is skipped by pre-dump and always dumped whole** (`mem.c`: "We totally ignore
   MAP_HUGETLB on pre-dump"). That is what `-XX:+UseLargePages` gives you if the node has hugetlbfs pages
   reserved (`vm.nr_hugepages > 0`). **Verified (source).**

What follows for a G1 heap. Roughly, the pages dirtied in a window of T seconds are:

```
D(T) ≈ min(alloc_rate × T, young span)        eden + survivors: G1 takes young regions from the TAIL of the
                                              address-ordered free list (heapRegionManager.cpp:
                                              from_head = !type.is_young()), so a constant-size young gen
                                              keeps reusing the same regions
     + promoted bytes + old-gen mutation       distinct pages; ×512 amplification under THP
     + mixed-GC evacuation copies              live bytes copied out of old regions
     + concurrent-cycle bitmaps                mark bitmap = heap/64 (160 MB at 10 G), written + cleared per cycle
     + card table / remsets / native           usually tens of MB
     + whole committed heap, if G1 expanded the heap in the window
```

Once T exceeds one eden fill, the allocation term saturates at the young-generation size, and that is the
**plateau** in our rounds. More pre-copy rounds can't beat it. The final dump is D(last round's
duration). The lever is the young span plus the old-gen churn, not the allocation rate.

## 2. Local measurements (synthetic, for direction only)

`Alloc` holds a 512 MiB live set of 64 KiB arrays and allocates 200 MB/s of 1 KiB garbage. Each second
it rewrites 50 MB/s of random live arrays in full (with 1/8 of them replaced, as promotion churn). Heap
is 2 GiB. Each run: 15 s warm-up, then `dirtyrate -interval 1s -duration 30s -bw 60M`. JDK 21.0.12.
THP on this machine = madvise.

| run | flags (beyond the collector) | resident | D(1s) | D(10s) | D(30s) | young pauses in 30 s (avg) |
|---|---|---|---|---|---|---|
| g1-fixed | `-Xms2g -Xmx2g` | 1.96 GiB | 254 MiB | **1.59 GiB** | 1.79 GiB | 5 (10.3 ms) |
| g1-xmn256 | `-Xms2g -Xmx2g -Xmn256m` | 1.07 GiB | 290 MiB | **649 MiB** | 901 MiB | 26 (2.5 ms) |
| g1-maxnew10 | `… -XX:G1MaxNewSizePercent=10` | 1.02 GiB | 268 MiB | **598 MiB** | 852 MiB | 32 (3.0 ms) |
| g1-aikar | Aikar's flags (NewSize 30/40 %, IHOP 15, MTT 1, …) | 1.63 GiB | 254 MiB | 1.24 GiB | 1.52 GiB | 14 (3.6 ms) |
| g1-grow | `-Xms256m -Xmx2g` | 1.37 GiB | 254 MiB | 1.31 GiB (**96 %**, two whole-heap spikes) | 1.35 GiB | 15 (7.3 ms) |
| parallel | `-XX:+UseParallelGC -Xms2g -Xmx2g` | 1.43 GiB | 272 MiB | 707 MiB | 1.08 GiB | 23 (3.1 ms) |
| shenandoah | `-XX:+UseShenandoahGC -Xms2g -Xmx2g` | 2.06 GiB | 253 MiB | 1.62 GiB | 1.98 GiB | 24 (sub-ms) |
| zgc | `-XX:+UseZGC -XX:+ZGenerational` | 111 MiB private + **1.22 GiB memfd** | – | memfd dumped whole | – | – |
| sparse-nothp | G1 `-Xmn256m`, 20 MB/s alloc, one 8-byte write per 64 KiB array | 837 MiB | 23 MiB | **226 MiB** | 328 MiB | 2 |
| sparse-thp | same + `-XX:+UseTransparentHugePages` | 880 MiB (850 MiB THP) | 542 MiB | **765 MiB** | 856 MiB | 3 |

Takeaways. The workload is synthetic: the old-gen mutation here is deliberately heavy, so treat these as
directions, not magnitudes.

- **Capping the young generation cut D(10s) by 2.5×** (1.59 GiB → 0.6-0.65 GiB). As a bonus, resident
  memory fell from 1.96 to 1.07 GiB, because G1 never touched the unused regions. GC count went up 5×,
  but pauses got shorter: total pause time 52 → 66-96 ms over 30 s. What is left is mostly this
  workload's old-gen mutation.
- **A growing heap (Xms < Xmx) re-dirties the whole heap whenever G1 commits a region.** That is fatal
  for convergence and invisible unless you look at VMAs. The tool's `new-vma` column shows it.
- **THP made sparse old-gen writes 24× more expensive** (23 → 542 MiB/s) and raised the predicted final
  dump from 50 MiB to 821 MiB.
- Shenandoah behaved like an uncapped G1. ZGC can't be pre-copied (memfd). Parallel bounds eden like
  capped G1, but its full GCs compact (rewrite) the whole old gen, with second-long pauses: not for
  players.

## 3. Options, expected effect, cost

Ordered by expected value for our problem.

### 3.1 Cap the young generation (biggest lever)
- `-Xmn<size>` (fixes young size; disables G1's pause-time young sizing), or keep G1 adaptive within
  bounds: `-XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=2 -XX:G1MaxNewSizePercent=10..20`
  (defaults 5 / 60, `g1_globals.hpp`). A lower `-XX:MaxGCPauseMillis` (default 200) also makes G1
  pick a smaller eden, adaptively.
- **Effect:** D(T) for T ≥ one eden fill drops to about young size + old churn. At 10 G heap, G1 may
  grow young to 6 GB (60 %). **Aikar's flags force a minimum young of 30 % = 3 GB**, which matches the
  plateau we see. With `-Xmn1g` I'd expect the final dump to fall from 1.31 GB toward 0.5-0.8 GB, i.e.
  final dump + ship from ~8 s to ~3-4 s.
- **Cost:** young GCs become proportionally more frequent (eden 1 GB at ~0.5 GB/s allocation = one young
  GC every ~2 s). Each pause scales with surviving objects, not eden size, so pauses stay similar or get
  shorter. Risk: premature promotion of medium-lived objects (chunk data in flight), which means more old
  growth and more mixed GCs. Watch MSPT p99 and the old-gen growth rate in GC logs. The OpenJDK tuning
  guide advises against `-Xmn` for pause-goal reasons, so prefer `G1MaxNewSizePercent` if pauses suffer.
  Source: [G1 tuning guide (JDK 21)](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-garbage-collector-tuning.html).

### 3.2 Fixed heap: `-Xms` = `-Xmx`
- **Effect:** no commit/uncommit after start-up, so no whole-VMA re-dirtying (§1.5). Each G1 expansion
  during pre-copy otherwise re-ships the entire committed heap. This might explain part of our
  non-converging rounds, if the image doesn't already set Xms = Xmx (itzg's `MEMORY` sets both;
  `INIT_MEMORY`/`MAX_MEMORY` don't).
- **Cost:** with a fixed heap, G1 never uncommits, so RSS ratchets up to the heap's high-water mark of
  touched regions. Round 1 and restore time grow with RSS. Mitigate with a tighter Xmx (3.3). Note that
  a fixed heap does *not* make the whole Xmx resident; only touched regions count. `g1-xmn256` stayed at
  1.07 GiB resident with a 2 GiB fixed heap.

### 3.3 Smaller heap (Xmx)
- **Effect:** caps RSS, so round 1 and **restore (6 s, which scales with total memory, not the delta)**
  shrink. D(T) changes little.
- **Cost:** more frequent concurrent cycles and mixed GCs, which add old-gen copying and bitmap writes.
  The risk is evacuation failure followed by a full GC, which rewrites the entire live heap. Size Xmx at
  ~2-2.5× the live set measured after a full GC (`jcmd GC.heap_info` after `GC.run` in a quiet moment, or
  old-gen occupancy after mixed GCs in the GC log).

### 3.4 Transparent huge pages off for the heap
- Check `/sys/kernel/mm/transparent_hugepage/enabled` on the k3s nodes (`madvise` here). Check that the
  launch flags don't contain `-XX:+UseTransparentHugePages` or `-XX:+UseLargePages` (the latter with
  hugetlbfs is worse, §1.8). The tool's `thp` column shows AnonHugePages.
- **Effect:** removes up to 512× amplification of sparse old-gen writes (24× measured). Neutral for eden,
  which is written densely anyway.
- **Cost:** a few % CPU from TLB misses on a big heap. Also, with THP=`always` on the node, the kernel
  applies THP to the heap even without the JVM flag.

### 3.5 Less old-gen churn (relevant if the images use Aikar's flags)
- Aikar's set: `InitiatingHeapOccupancyPercent=15` (a marking cycle almost continuously, each writing and
  clearing a heap/64 bitmap), `MaxTenuringThreshold=1` (promote after one GC, so more promotion writes and
  more old garbage), `G1MixedGCLiveThresholdPercent=90` and `G1MixedGCCountTarget=4` (copy fuller regions,
  more aggressively), `SurvivorRatio=32` (tiny survivors, so overflow promotion),
  `G1NewSizePercent=30/G1MaxNewSizePercent=40` (big young, §3.1).
  Source: [PaperMC: Aikar's flags](https://docs.papermc.io/paper/aikars-flags).
- **Effect (expected):** going back to defaults (adaptive IHOP, MTT 15, live threshold 85, count target 8,
  SurvivorRatio 8) should lower the old-gen term of D(T). That matters once young is capped.
- **Cost:** Aikar tuned these for pause smoothness on Paper. Defaults may give longer mixed phases or more
  old-gen growth; measure MSPT. Second-order compared with 3.1/3.2.

### 3.6 G1 region size (`-XX:G1HeapRegionSize`, default heap/2048 → 4 MB at 10 G; Aikar 8 MB)
- **Effect (expected small):** larger regions raise the humongous threshold (half a region). Humongous
  objects bypass eden and land in fresh regions anywhere, which scatters dirtying. Coarser regions mean
  slightly more waste. Low priority; measure only if `javaheap` dirty doesn't track young size.

### 3.7 TLAB sizing (`-XX:TLABSize`, `-XX:-ResizeTLAB`)
- **No meaningful effect.** TLABs subdivide eden, but the bytes allocated (and zeroed by Java semantics)
  are the same. Skip.

### 3.8 "AdaptiveSizePolicy" equivalents for G1
- `UseAdaptiveSizePolicy` is a Parallel GC flag. G1's equivalent knobs are the young bounds above
  (`-Xmn` turns adaptivity off; `G1New/MaxNewSizePercent` bound it) and `MaxGCPauseMillis`/`GCTimeRatio`.
  `GCTimeRatio` also drives heap expansion when Xms < Xmx.

### 3.9 `-XX:SoftMaxHeapSize`
- **G1 in JDK 21 ignores it.** It is honoured by ZGC (and Shenandoah). G1 support is
  [JDK-8236073](https://bugs.openjdk.org/browse/JDK-8236073), still open, fix version tbd. Not usable for us.

### 3.10 Periodic GC / uncommit (`-XX:G1PeriodicGCInterval`, JEP 346)
- `G1PeriodicGCInterval` (ms, *manageable*: settable at runtime with `jcmd VM.set_flag`) runs a GC when
  none happened for that long and the load average is below `G1PeriodicGCSystemLoadThreshold`. It runs a
  concurrent cycle by default; set `-XX:-G1PeriodicGCInvokesConcurrent` for a STW full GC. G1 then shrinks
  the heap toward Xms per `MaxHeapFreeRatio` and uncommits the free regions concurrently.
  [JEP 346](https://openjdk.org/jeps/346).
- **Effect:** only with Xms < Xmx, and mainly for **idle** servers: RSS drops, so round 1 and restore
  shrink. Uncommitted regions are not resident, so CRIU never dumps them (§1.4).
- **Cost/conflict:** shrinking implies later re-expansion, and re-expansion means whole-heap re-dirtying
  (§1.5). Fine for an idle server, which won't re-expand during a migration. Harmful for a busy one. So
  choose per pack, or keep Xms = Xmx. Uncommit uses `mmap(PROT_NONE)`, not `MADV_DONTNEED`, so it changes
  VMAs (harmless itself, but it sets up the re-commit hazard).

### 3.11 Collector choice
- **ZGC (generational or not): excluded.** The heap is a memfd and goes into the final dump whole (§1.7).
  Measured: 1.2 GiB memfd. The predicted final dump was the whole heap regardless of pre-copy.
  [JEP 439](https://openjdk.org/jeps/439).
- **Shenandoah:** private anonymous memory like G1, so pre-copy works. In JDK 21 it is single-generation:
  every cycle marks the whole heap and evacuates regions, and mutator allocation wanders over the free
  set. Measured the same as uncapped G1 (1.62 GiB / 10 s). No young cap exists to bound it (generational
  Shenandoah is JDK 24+ experimental, [JEP 404](https://openjdk.org/jeps/404)). It wins on pauses, not on
  dirtying.
- **Parallel:** eden is a fixed contiguous range, so the dirty set is bounded (707 MiB / 10 s). But full
  GCs slide-compact the old gen (the whole live set gets rewritten, with second-long pauses on 4+ GB). Not
  viable for a game server.
- Stay on G1.

### 3.12 `-XX:+AlwaysPreTouch`: keep it off
- It makes all of Xmx resident (round 1 and restore get bigger). Dirty rate is unchanged, since pages
  stay clean until written. Already advised off in docs/research/k8s-checkpoint-restore.md.

### 3.13 Native side (`MALLOC_ARENA_MAX=2`)
- Fewer glibc arenas mean less scattered native dirtying (the `anon` label). It was small here (~10 MiB
  per 10 s), but modded servers with many threads and Netty may differ. Cheap; measure the `anon` label.

## 4. "JVM-cooperative" ideas, reasoned through

- **GC right before the final dump (`jcmd GC.run`, or a young GC via JVMTI/WhiteBox): no gain, likely a
  loss.** The final dump contains pages written since the last pre-dump. The eden pages written in that
  window are dirty whether their objects are now dead or not, because a GC doesn't clean or release them
  (§1.3). The GC then adds its own writes: survivor copies, promotions, card table, remsets. `GC.run` is
  a **full GC** in G1 (unless `-XX:+ExplicitGCInvokesConcurrent`), which compacts and rewrites the whole
  live heap. That would make the final dump about as big as the live set. JVMTI's `ForceGarbageCollection`
  is also a full GC, and a young-only GC on demand exists only in the test-only WhiteBox API.
  (`-XX:+DisableExplicitGC` blocks `System.gc()` but not `jcmd GC.run`.)
- **Empty eden + uncommit before the final dump** would help, because non-resident pages aren't dumped.
  But G1 never uncommits eden on demand. Only a full GC or a Remark-time shrink plus concurrent uncommit
  does, and only down to Xms. The cost is a full-GC pause (seconds at a 4 GB live set) plus the
  re-expansion hazard. Not worth it for a busy server.
- **Full GC *before pre-copy starts*** (pre-migrate hook, round 1 not yet taken): with Xms < Xmx this
  compacts and shrinks the heap, so round 1 and restore get smaller. The compaction writes don't matter
  because nothing has been dumped yet. Risks: the pause (players notice), and G1 re-expanding the heap
  during pre-copy (whole-heap re-dirty). For idle or near-empty servers it is a clear win; for busy ones,
  measure (E9).
- **Stop the game from churning during the last rounds.** Vanilla 1.20.3+ has `/tick freeze`, which stops
  world and entity ticking while players stay connected and chat works. Run from the existing migration
  hooks just before the last pre-dump, it should collapse D(T) to near zero, so the final dump is tiny.
  Players see a frozen world for a few seconds instead of a stall. It isn't available on 1.20.1 Forge
  packs (ATM9 etc.), and it's not generic, so it is a per-pack option.
- **Lcm-side corollary** (not a JVM option): D(T) saturates, so more rounds don't help. What does help is
  making the last round short (the final dump is D(last round duration)), e.g. a quick extra round once
  rounds stop shrinking.
- **Restore (6 s)** scales with total memory restored (~5.6 GB), not the delta. Only a smaller RSS (3.2,
  3.3, the young-cap side effect) or lazy restore help it.

## 5. Experiment matrix (Minecraft server under bot load)

Setup, in the lab cluster first (no prod nodes):
- Server: the heavy modded pack with its current flags, plus `-XX:NativeMemoryTracking=summary` (for
  labels; overhead is small for summary but not zero, so remove it afterwards) and `-Xlog:gc*:file=…`.
- Load: `tools/mcbot` with a fixed number of bots doing the same route (exploration means chunk
  generation, the worst case). Warm up 5 min (JIT, heap steady state) before each measurement.
- Measure on the node as root:
  ```
  CGO_ENABLED=0 go build -o dirtyrate ./tools/dirtyrate      # static binary; copy to the node
  PID=$(crictl inspect <container-id> | jq .info.pid)        # the container's init (lcm-relay)
  JPID=$(pgrep -P $PID java)                                 # the JVM it supervises
  jcmd $JPID System.map > map.txt   # or: kubectl exec <pod> -- jcmd $(pgrep java) System.map  (JDK image)
  ./dirtyrate -pid $JPID -interval 2s -duration 120s -window 10s -labels map.txt -by-label \
      -bw <round-1 bytes / (dump+ship s) from a real migration> -json E0-run1.json
  ```
  **Never while that pod is migrating or might be migrated** (pause auto-migration). The tool refuses to
  start, and stops early, if it sees criu running or an `/var/lib/lcm/ckpt/*` in progress.
- Record per run: D(2s), D(10s), D(30s), `new-vma` spikes, `thp`, resident, per-label table; MSPT/TPS
  (mean, p99), young/mixed GC count and pause p99, CPU. Do 3 runs per configuration.
- **Validate the tool first:** for E0, compare `-bw` predicted rounds with the real migration's rounds
  (5.63/2.50/2.19/1.63 → 1.31 GB). If they agree within ~20 %, the matrix can be run with the tool alone.
  Do real lab migrations only for the final 1-2 candidates.

| # | change vs. baseline | prediction | cost to watch |
|---|---|---|---|
| E0 | baseline (current flags) | D(10s) ≈ 1.5-2.5 GB; check `new-vma` for heap-growth spikes | – |
| E1 | `-Xms10G` = `-Xmx10G` (if not already) | no `new-vma` spikes; rounds that were inflated by growth converge | RSS rises to the touched high-water mark |
| E2a | E1 + `-Xmn2g` | D(10s) drops by about (current young − 2 GB) | young GC frequency |
| E2b | E1 + `-Xmn1g` | D(10s) ≈ 1 GB + old churn; **final dump 0.5-0.8 GB** | MSPT p99, promotion rate |
| E2c | E1 + `G1NewSizePercent=2 G1MaxNewSizePercent=10` (unlock experimental) | like E2b but adaptive | same |
| E2d | E1 + `MaxGCPauseMillis=50` (no young cap) | smaller adaptive eden, between E0 and E2b | more GCs, throughput |
| E3 | best E2 + drop Aikar's IHOP 15 / MTT 1 / LiveThreshold 90 / SurvivorRatio 32 (if present) | old-gen + `gc`-label dirty down (fewer marking cycles, less copying) | mixed-GC pauses, old growth |
| E4 | best so far, with THP vs. without (`-XX:±UseTransparentHugePages`; node setting) | if THP was on: big drop in `javaheap` dirty for sparse writes | 1-5 % CPU |
| E5 | best so far with Xmx 8G and 7G (Xms = Xmx) | resident, round 1 and restore down ~proportionally; D roughly flat | marking frequency, risk of full GC |
| E6 | best so far with `G1HeapRegionSize` 8M / 16M | small; fewer humongous regions | waste |
| E7 | Shenandoah (same heap) | ≈ E0 or worse (no young cap) | – |
| E8 | idle server: `-Xms2G -Xmx10G -XX:G1PeriodicGCInterval=60000` | idle RSS shrinks (smaller round 1/restore); no `new-vma` while idle | heap growth at next join, whole-heap re-dirty if mid-migration |
| E9 | pre-migrate `jcmd GC.run` before pre-copy (Xms < Xmx) | round 1 ≈ live set; watch for `new-vma` after it | full-GC pause seconds |
| E10 | `MALLOC_ARENA_MAX=2` | `anon` label dirty down a little | none expected |

## 6. Top experiments to run first
1. **E1 + E0 `new-vma` check.** Is the heap resizing during pre-copy? If yes, Xms = Xmx alone could fix a
   large share of the non-convergence (whole-heap re-dirty per expansion).
2. **E2b/E2c: young cap at ~1 GB.** Predicted final dump 1.31 → ~0.5-0.8 GB, i.e. freeze −4 to −5 s. Cost:
   young GCs every ~1-3 s with similar or shorter pauses.
3. **E4: confirm THP is off** for the heap (node setting + flags). If it is on, this is a large, nearly
   free win (24× on sparse writes here).
4. **E3: de-Aikar the old-gen flags**, if the images use them. Second-order but cheap.
5. **E5: tighter Xmx** for restore time, which is the next bottleneck once the delta shrinks.

## 7. Measured: a real Fabric server under bot load (2026-10-24)

Fabric 1.21.10 (loader 0.20.1), OpenJDK 21.0.12, G1, `-Xmx4G`, one shared pre-generated world,
on a 32-core workstation. Load: `tools/mcbot` bots walking near spawn; "heavy" adds 20 bots, 400
cows and villagers, and `randomTickSpeed 30`, for MSPT around 30 ms. After 60 s of warm-up,
`tools/dirtyrate -interval 10s -duration 120s -bw 136M` (the tailnet throughput measured on
prod) measured the dirty rate and predicted lcm's pre-copy. MSPT came from `tick query`, pauses
from `-Xlog:gc`. Each batch ran in parallel, so compare within a batch.

"Aikar" is the flag set the pack images use (`G1NewSizePercent=30`, `G1MaxNewSizePercent=40`,
plus the rest). "capN" is the same set with `G1NewSizePercent=5` (3 for cap10) and
`G1MaxNewSizePercent=N`.

| batch | flags | dirty / 10 s | resident | predicted final dump | MSPT p50 / p99 | young pause p50 / max |
|---|---|---|---|---|---|---|
| light | Aikar, Xms2G | 782 MiB | 1.9 GiB | 311 MiB | 4.6 / 11.3 ms | 3.6 / 10 ms |
| light | cap15, Xms2G | **390 MiB** | 1.4 GiB | **63 MiB** | 4.0 / 6.7 ms | n/a |
| light | cap10, Xms4G | 498 MiB | 1.5 GiB | 99 MiB | 4.1 / 6.9 ms | n/a |
| light | Aikar, Xms4G (=Xmx) | 821 MiB | 2.8 GiB | 496 MiB | 5.8 / 11.8 ms | 8.6 / 30 ms |
| light | cap15, Xms4G | 691 MiB | 1.8 GiB | 213 MiB | 5.7 / 10.9 ms | 3.7 / 26 ms |
| light | G1 default young sizing, Xms4G | 825 MiB | 3.6 GiB | 620 MiB | 6.1 / 17.4 ms | 7.9 / 23 ms |
| heavy | Aikar, Xms2G | 999 MiB | 2.1 GiB | 628 MiB | 29.7 / 62.1 ms | 4.2 / 29 ms |
| heavy | cap15, Xms2G | **485 MiB** | 1.7 GiB | **94 MiB** | 29.2 / 53.9 ms | 3.5 / 44 ms |
| heavy | cap25, Xms2G | 700 MiB | 1.8 GiB | 221 MiB | 34.8 / 53.8 ms | 3.3 / 33 ms |

Mixed collections were the same under heavy load (10 for cap15, 11 for Aikar), and there were
no full GCs.

What this shows:

- **The dirty set per round is about the young generation.** G1 sizes the young gen as a percent
  of the *current* heap, which is why cap15 with Xms2G (heap stayed at 2 GiB, so the young gen
  is at most ~300 MiB) beats cap15 with Xms4G. Every young collection rewrites the survivors
  and the eden the next allocations reuse, so whatever the young gen holds is dirty again each
  round.
- **Capping it costs nothing measurable here.** Young collections run 2-3x as often but stay a
  few ms (p50 3.5 ms). MSPT didn't get worse, and its p99 got better. The old gen saw no extra
  mixed GCs.
- **The final dump (the blackout's biggest part) shrinks 5-7x:** 628 MiB to 94 MiB under heavy
  load. At 136 MB/s that's ~4.6 s of shipping down to ~0.7 s, and the pre-copy converges in 4
  rounds instead of stalling at ~700 MiB a round.
- `Xms=Xmx` doesn't help: it grows the resident set (more to pre-copy) and, with percent-based
  young sizing, the young gen with it.

**Recommendation:** in the pack images' JVM flags, replace Aikar's `-XX:G1NewSizePercent=30
-XX:G1MaxNewSizePercent=40` with `-XX:G1NewSizePercent=5 -XX:G1MaxNewSizePercent=15`. Keep the
rest, and keep `Xms` below `Xmx`. Caveats: one vanilla-ish Fabric server, not a 200-mod pack. Big
modpacks allocate faster, so the young gen fills sooner and objects get promoted earlier
(`MaxTenuringThreshold=1`). Try it on one pack first, and compare MSPT and GC logs over a
play session. `tools/dirtyrate` on the live server (with migrations paused) gives that pack's
before and after.

## Sources
- Kernel: [soft-dirty PTEs](https://docs.kernel.org/admin-guide/mm/soft-dirty.html),
  [pagemap](https://docs.kernel.org/admin-guide/mm/pagemap.html), [THP](https://docs.kernel.org/admin-guide/mm/transhuge.html);
  source `mm/vma.c` (`__mmap_complete`), `fs/proc/task_mmu.c` (`pte_to_pagemap_entry`, `pagemap_pmd_range_thp`, `clear_soft_dirty_pmd`).
- CRIU source (`criu-dev`): `criu/mem.c` (`should_dump_page`, `generate_iovs`, MAP_HUGETLB on pre-dump),
  `criu/shmem.c` (`is_shmem_tracking_en`, `do_dump_one_shmem`), `criu/memfd.c`; [CRIU: memory changes tracking](https://criu.org/Memory_changes_tracking).
- HotSpot (jdk21u): `os/linux/os_linux.cpp` (`commit_memory_impl` = `mmap(MAP_FIXED)`, uncommit = `mmap(PROT_NONE)`),
  `gc/g1/heapRegionManager.cpp` (`allocate_free_region`: young from the tail), `gc/g1/g1_globals.hpp`, `runtime/globals.hpp`.
- OpenJDK: [JEP 346 (G1 periodic GC / uncommit)](https://openjdk.org/jeps/346), [JEP 439 (Generational ZGC)](https://openjdk.org/jeps/439),
  [JEP 404 (Generational Shenandoah)](https://openjdk.org/jeps/404), [JDK-8236073 (G1 SoftMaxHeapSize, open)](https://bugs.openjdk.org/browse/JDK-8236073),
  [G1 tuning guide](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-garbage-collector-tuning.html),
  `jcmd System.map` (JDK-8318636, extended by [JDK-8322475](https://mail.openjdk.org/pipermail/hotspot-dev/2024-July/090980.html); present in 21.0.12).
- [PaperMC: Aikar's flags](https://docs.papermc.io/paper/aikars-flags).
