# Run guide: Milestones 9 and 10 (Debian VM)

Same routine as before: one SSH window, `cd tower_fm`, `pkill -f "nodes[.]run"` before every run, `nohup` + `tail -f` for long runs.
One experiment at a time (the lock file refuses a second one). Close browsers first (1 CPU).
Each experiment starts its own cluster (about 6 to 11 node processes plus the driver) and needs about 300 MB of RAM.

## 0. Install (once, about 3 minutes)
PowerShell (go to the folder that holds the zip first):

    cd $HOME\Downloads
    C:\Windows\System32\OpenSSH\scp.exe -P 2222 .\tower_fm_snapshot6.zip victor@127.0.0.1:~/

SSH:

    cd ~
    cp -r tower_fm tower_fm_backup
    unzip -o tower_fm_snapshot6.zip -x "tower_fm/results/*"
    cd tower_fm
    python3 cluster.py

Undo: `rm -rf ~/tower_fm && mv ~/tower_fm_backup ~/tower_fm`.
New: `services/store.py`, `naming.py`, `transparency.py`, `procmgmt.py`, `experiments/_lab.py`, `week9_transparency.py`,
`week10_naming_procs.py`. Changed by a few lines: `nodes/node.py` (routes OBJ_/NS_/PM_ messages), `comms/transport.py`
(message line limit 64 KB -> 16 MB, needed to register thousands of names at once).

## 1. Milestone 9: transparency and reliability
| # | Command | Time | Shows |
|---|---|---|---|
| 9a | `python3 -m experiments.week9_transparency --demo` | ~1.5 min | the five transparencies, narrated |
| 9b | `python3 -m experiments.week9_transparency --quick` | ~11 min | everything, 1 trial (dry run) |
| 9c | `nohup python3 -m experiments.week9_transparency > week9_output.txt 2>&1 &` | ~35 min | **full run for the report**, 3 trials |
| 9d | `python3 -m experiments.week9_transparency --part failure,fencing` | ~5 min | only some parts (overhead, replication, migration, failure, reliability, detection, fencing) |

    pkill -f "nodes[.]run"
    nohup python3 -m experiments.week9_transparency > week9_output.txt 2>&1 &
    tail -f week9_output.txt
    cat results/week9_hypotheses.txt      # H9.1 to H9.7, SUPPORTED / NOT SUPPORTED with numbers
    cat results/week9_summary.csv         # mean, sd, 95% CI for every measure

Expect about (my 1-CPU sandbox; replace with yours): warm-cache read = direct read (about 6.7 ms), cold lookup about 13.5 ms (4 messages
instead of 2), a copy on the client's own node 0.01 ms; write latency rf=3 sync about x2.3 and async about x1.0, async stale reads about 60%;
migration: 0 errors, freeze about 4 ms, slowest operation about 35 ms, baseline fails 100% after the move; primary killed: rf=2 outage about
0.03 s and 0 errors, baseline about 5 to 6 s; sync loses 0 acknowledged writes, async a few, in-memory rf=1 loses everything;
availability measured matches the model "down only while every copy is down, minus the 1 s retry window"; H9.6 is NOT SUPPORTED
(client suspicion detects as fast as the monitor): report it as a finding, not a failure.

## 2. Milestone 10: naming and process management
| # | Command | Time | Shows |
|---|---|---|---|
| 10a | `python3 -m experiments.week10_naming_procs --demo` | ~2.5 min | naming, staleness, a workflow's lifecycle, coordinator crash |
| 10b | `nohup python3 -m experiments.week10_naming_procs --quick > week10_output.txt 2>&1 &` | ~22 min | everything, 1 trial (dry run) |
| 10c | `nohup python3 -m experiments.week10_naming_procs > week10_output.txt 2>&1 &` | ~65 min | **full run for the report**, 3 trials |
| 10d | `python3 -m experiments.week10_naming_procs --part resolution,staleness` | ~3 min | naming only; other parts: scale, nsfailure, lifecycle, policy, procscale, coordfail, workerfail, terminate |

    cat results/week10_hypotheses.txt     # H10.1 to H10.9
    cat results/week10_summary.csv        # mean, sd, 95% CI

Expect about: messages per resolution static 0, nameserver 2, prefix 2, broadcast 2 x holders (10 with 5 holders; 8, 12, 20 for 4, 6, 10
holders); cached and static resolvers wrong after a migration (cached until the TTL, static for ever); with the master dead nameserver clients
resolve 0%, prefix and cached clients about 100%, broadcast about 86% (the rest have their primary on the dead master); create about 4 ms,
schedule about 35 ms, locate about 7 ms, terminate 6 processes about 105 ms; leader killed: durable state 12 of 12 workflows complete, memory
only 0 of 12, new coordinator after about 1.9 s; worker killed: all complete, makespan about +0.7 s. H10.5 (probe2 vs registry) came out
NOT SUPPORTED on mixed task sizes in my sandbox (the stale registry won): report what YOUR runs show.

## 3. If something breaks
| Symptom | Fix |
|---|---|
| Another experiment is already running | `pkill -f 'experiments[.]'; pkill -f "nodes[.]run"`, wait 5 s, rerun |
| `cluster not ready (no leader)` / address in use | `pkill -f "nodes[.]run"`, wait 5 s, rerun |
| A run seems frozen | the VM has 1 CPU: 9c about 35 min, 10c about 65 min. Check `tail week10_output.txt` |
| `ValueError: Separator is not found` / line too long | the old `comms/transport.py` is still in place: re-unzip over the folder |
| Results differ from this sheet | expected: quote YOUR numbers as mean ± sd |
| One part looks wrong | run it alone with `--part`; per-node logs are in `logs/m9`, `logs/m10n`, `logs/m10p` |
| Low on time | demos 9a and 10a, and show the saved CSVs |

## 4. Live demo
1. `python3 -m experiments.week9_transparency --demo`: say **Failure -> Detection -> Recovery -> Service Restoration**: the transparent client saw a
   30 ms hiccup, the baseline was down for seconds and lost its data.
2. `python3 -m experiments.week10_naming_procs --demo`: naming table (messages per resolution), the stale answers after a migration, a workflow
   through Create -> Locate -> Schedule -> Coordinate -> Terminate, and the coordinator crash: with durable state nothing is lost.

## 5. Reproducibility
Seed 42, link 5 ms +-2 ms, 3 managers + cloud + core (+ 4 sites for process management), no sensor traffic. Every run writes `results/environment.txt`.
Limits to state: one machine with one vCPU (the load generator shares the CPU, so throughput is a lower bound); a name server that is a single
master (its journal survives a restart, but no resolution of NEW names while it is down); fault injection by SIGKILL and SIGSTOP, not a real network.
