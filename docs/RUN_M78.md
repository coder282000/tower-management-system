# Run guide: Milestones 7 and 8 (Debian VM)

Same routine as Milestones 3-6: one SSH window, `cd tower_fm`, `pkill -f "nodes[.]run"` before every run, `nohup` + `tail -f`
for long runs. **Run one experiment at a time.** From this snapshot on, a second run refuses to start while one is running
(`.experiment.lock`), so the Week 2 double-run collision cannot happen again. If a run is killed, the lock frees itself.
Close browsers and other heavy apps first (the VM has 1 CPU).

## 0. Install the Milestone 7 and 8 update (once, about 3 minutes)
PowerShell window, copy the zip (same port as your ssh command):

    C:\Windows\System32\OpenSSH\scp.exe -P 2222 .\tower_fm_snapshot5.zip victor@127.0.0.1:~/

SSH window, back up, then unzip over your folder (your `results/` folder is skipped so saved results stay):

    cd ~
    cp -r tower_fm tower_fm_backup
    unzip -o tower_fm_snapshot5.zip -x "tower_fm/results/*"
    cd tower_fm
    python3 cluster.py

Undo everything: `rm -rf ~/tower_fm && mv ~/tower_fm_backup ~/tower_fm`.
Changed files: `services/faulttolerance.py`, `experiments/week7_faulttolerance.py`, `experiments/week8_models.py`,
`nodes/node.py` (one line: optional append-mode log), `metrics/logger.py` (same). New: `experiments/_common.py`.
Check the unzip did not overwrite VM-only edits: `diff ~/tower_fm_backup/nodes/node.py ~/tower_fm/nodes/node.py`
(expect only the `append=cfg.get("log_append", False)` line).

## 1. Milestone 7: fault tolerance and recovery
| # | Command | Time | Shows |
|---|---|---|---|
| 7a | `python3 -m experiments.week7_faulttolerance --demo` | about 1.5 min | mgr1 is killed: baseline vs proposed, narrated Failure -> Detection -> Recovery -> Restoration |
| 7b | `python3 -m experiments.week7_faulttolerance --only site_crash,byzantine --quick` | about 2.5 min | the two new scenarios |
| 7c | `python3 -m experiments.week7_faulttolerance --quick` | about 8 min | all 7 scenarios, 1 trial (dry run) |
| 7d | `nohup python3 -m experiments.week7_faulttolerance > week7_output.txt 2>&1 &` | about 25 min | **full run for the report**, 3 trials |
| 7e | `nohup python3 -m experiments.week7_faulttolerance --trials 10 > week7_output.txt 2>&1 &` | about 80 min | tighter numbers (run overnight / before the final) |
| 7f | `nohup python3 -m experiments.week7_faulttolerance --sweep > week7_sweep.txt 2>&1 &` | about 13 min | detection-parameter sweep (speed vs false alarms) |

    pkill -f "nodes[.]run"
    nohup python3 -m experiments.week7_faulttolerance > week7_output.txt 2>&1 &
    tail -f week7_output.txt          # Ctrl+C stops watching only
Finished when the output ends with the "Hypothesis check" block. Results:

    cat results/week7_summary.csv     # mean, sd, 95% CI per scenario and mode   -> Table 7.x
    cat results/week7_faults.csv      # every single run                        -> appendix
    cat results/week7_sweep.csv       # only after --sweep
    cat results/environment.txt       # hardware / software for the reproducibility table

**What each scenario is** (fault model): `manager_crash`, `core_crash`, `site_crash` = crash / node failure at three tiers;
`omission` = 70% of mgr1's outgoing messages lost; `partition` = 100% lost (communication failure, mgr1 cut off);
`service_failure` = correlation service stopped on a live node; `byzantine` = mgr1 fabricates incidents.
**What the numbers mean:** `availability` = T_up / T_total of the faulted component (`avail_basis` says which); `mttr_s` = mean
down interval; `coverage` = share of real fault episodes at the sites that the core learned about (ground truth, the best
single "did the system keep working" number); `delivery_ratio` = share of escalations that reached the core;
`dup_processed` = escalations the core processed twice after a restart.
Expect (from the test runs in my sandbox; replace with yours): manager_crash MTTR about 7.5 s -> about 0.3 s; service_failure
MTTR 7.8 s -> 0.25 s; core_crash coverage 0 -> 1.0; partition delivery 0 -> 1.0; site_crash detected after about 3 s only in the
proposed system; byzantine: the core accepts every fabricated incident in BOTH modes (an honest negative result).

## 2. Milestone 8: distributed system models
| # | Command | Time | Shows |
|---|---|---|---|
| 8a | `python3 -m experiments.week8_models --demo` | about 3 min | 6 sites, 10 s per model, table only (best for live) |
| 8b | `nohup python3 -m experiments.week8_models --quick > week8_output.txt 2>&1 &` | about 14 min | 3 and 9 sites, 1 trial, crash runs (dry run) |
| 8c | `nohup python3 -m experiments.week8_models > week8_output.txt 2>&1 &` | about 50 min | **full run for the report**: 3/6/9 sites, 2 trials, controls, crash runs, stress |
| 8d | `nohup python3 -m experiments.week8_models --duration 30 > week8_output.txt 2>&1 &` | about 80 min | 30 s per measurement: more faults, tighter numbers |
| 8e | `python3 -m experiments.week8_models --part stress` | about 8 min | only the saturation test |

    pkill -f "nodes[.]run"
    nohup python3 -m experiments.week8_models > week8_output.txt 2>&1 &
    tail -f week8_output.txt
Finished when the last lines say `saved results/week8_hypotheses.txt`. Results:

    cat results/week8_summary.csv       # mean, sd, CI: latency, throughput, CPU, memory, messages   -> Table 8.1
    cat results/week8_reliability.csv   # crash runs, incl. the two control models                    -> Table 8.2
    cat results/week8_stress.csv        # saturation test                                            -> Figure 8.x
    cat results/week8_cost.csv          # ASSUMED unit costs: say so in the report                   -> Table 8.3
    cat results/week8_decision.csv      # ranking under 4 weight profiles, 5 cost variants, 3 sizes -> Table 8.4
    cat results/week8_hypotheses.txt    # SUPPORTED / NOT SUPPORTED per hypothesis (quote the numbers, not just the word)
Add `--no-controls` to skip the two control models (saves about 8 min) or `--part perf,rel` to leave out the stress test.

**What the new parts answer:** throughput (`throughput_inc_s`, incidents detected per second, and the share detected when the
offered load rises: Part E); utilisation (CPU and memory per tier); a robust choice (the decision matrix is re-scored under
different weights, unit costs and cluster sizes; if one model wins everywhere, the choice is robust, if not, say what it
depends on); and the controls `integrated_noft` and `server_based_ft` (is the integrated model's reliability from the model
or from the Milestone 7 fault-tolerance service?).

## 3. If something breaks
| Symptom | Fix |
|---|---|
| `Another experiment is already running` | `pkill -f 'experiments[.]'; pkill -f "nodes[.]run"`, wait 5 s, rerun |
| `address already in use` / `no leader elected` | `pkill -f "nodes[.]run"`, wait 5 s, rerun |
| The run seems frozen | The VM has 1 CPU, long runs are normal. Check with `tail week7_output.txt`, `jobs` |
| Numbers differ from this guide | Expected: real-time runs vary. Quote YOUR numbers (mean ± sd) |
| A scenario looks wrong | Run it alone: `--only <scenario>`; per-node logs are in `logs/week7/` and `logs/week8/` |
| Low on time | 7a and 8a are short and reliable; show the saved CSVs for the full runs |

## 4. Recommended order (if you have one evening)
1. Install (7 min), `python3 cluster.py` sanity check.
2. 7a demo, then start 7d (25 min). While it runs, read section 5 and write the Week 7 text.
3. 8a demo, then start 8c (50 min). Write the Week 8 text while it runs. Never run both at once.
4. Optional: 7f sweep (13 min) for the "detection speed vs false alarms" figure.
5. Copy `results/` and `config/` out, push to GitHub.

## 5. Live demo for the presentation
1. `python3 -m experiments.week7_faulttolerance --demo` : say **Failure -> Detection -> Recovery -> Service Restoration**; the
   narrated block prints the four steps with the measured numbers for baseline and proposed.
2. `python3 -m experiments.week8_models --demo` : five models, one table. Point at core data-plane load, detection latency
   and cost; then show `results/week8_decision.csv` and the hypothesis file for the full-run answer.
Key sentences: the proposed system turns an 8 s outage into about 0.3 s by failing over to the backup manager; the outbox
turns "lost" escalations into "late"; it tolerates crashes and omissions but not a lying manager (that needs 3f+1 managers);
no model wins on every criterion, and the decision matrix shows what the winner depends on.

## 6. Reproducibility
Seed 42 (`config/cluster.json`), sensor interval 0.25 s (Week 7) / 0.3 s (Week 8, 0.1 and 0.05 s in the stress part), link 5 ms
+-2 ms, 2 managers, fault timeline 5 s / 13 s / 24 s. Every run writes `results/environment.txt` (CPU, RAM, OS, kernel, Python,
psutil, command line). Message loss, crash timing and CPU sharing on one vCPU still vary a little between runs: report means.
