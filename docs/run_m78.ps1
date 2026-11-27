<#
  run_m78.ps1  -  run the Milestone 7 and 8 experiments in the VM over SSH and show the results in THIS window.

  Examples (from the folder that holds this file):
    powershell -ExecutionPolicy Bypass -File .\run_m78.ps1                         # demos only, about 5 min
    powershell -ExecutionPolicy Bypass -File .\run_m78.ps1 -Mode quick             # 1 trial each, about 25 min
    powershell -ExecutionPolicy Bypass -File .\run_m78.ps1 -Mode full              # for the report, about 90 min
    powershell -ExecutionPolicy Bypass -File .\run_m78.ps1 -Mode full -Sweep       # also the Week 7 parameter sweep (+13 min)

  Same idea as run_m56.ps1: kills stale node processes, runs the experiments one after the other in the VM (the VM
  refuses to start two at once), prints every result table here, saves a log (m78_run_<time>.log) and copies the
  VM's results folder to .\m78_results.
  (UNTESTED here: I could not run PowerShell in my sandbox, so read the first run's output carefully.)
  A full run takes over an hour: if your SSH session may drop, use the nohup commands in docs/RUN_M78.md instead.
#>
param(
  [string]$Target = "victor@127.0.0.1",
  [int]$Port = 2222,
  [string]$Dir = "~/tower_fm",
  [string]$Venv = "",
  [ValidateSet("demo","quick","full")][string]$Mode = "demo",
  [switch]$Sweep
)

$ssh = "C:\Windows\System32\OpenSSH\ssh.exe"
$scp = "C:\Windows\System32\OpenSSH\scp.exe"
$log = "m78_run_$(Get-Date -Format 'yyyyMMdd_HHmm').log"

function Run-Remote([string]$cmd) {
  Write-Host ""
  Write-Host ">>> $cmd" -ForegroundColor Cyan
  $pre = ""
  if ($Venv -ne "") { $pre = "source $Venv; " }
  & $ssh -p $Port $Target "${pre}cd $Dir; $cmd" 2>&1 | Tee-Object -FilePath $log -Append
}

function Show-Csv([string]$file) {
  Write-Host ""
  Write-Host "=== $file ===" -ForegroundColor Yellow
  & $ssh -p $Port $Target "cd $Dir; (column -s, -t < $file 2>/dev/null || cat $file)" 2>&1 | Tee-Object -FilePath $log -Append
}

Write-Host "Mode: $Mode   Target: $Target   Folder in VM: $Dir" -ForegroundColor Green
Run-Remote "pkill -f 'nodes[.]run'; sleep 1; python3 cluster.py | tail -3"

if ($Mode -eq "demo") {
  Run-Remote "python3 -u -m experiments.week7_faulttolerance --demo"
  Run-Remote "pkill -f 'nodes[.]run'; sleep 2; python3 -u -m experiments.week8_models --demo"
}
else {
  $flag = ""
  if ($Mode -eq "quick") { $flag = "--quick" }
  Run-Remote "python3 -u -m experiments.week7_faulttolerance $flag | tee results/week7_run.txt"
  Run-Remote "pkill -f 'nodes[.]run'; sleep 2; python3 -u -m experiments.week8_models $flag | tee results/week8_run.txt"
  if ($Sweep) { Run-Remote "pkill -f 'nodes[.]run'; sleep 2; python3 -u -m experiments.week7_faulttolerance --sweep | tee results/week7_sweep_run.txt" }
  Show-Csv "results/week7_summary.csv"
  Show-Csv "results/week8_summary.csv"
  Show-Csv "results/week8_reliability.csv"
  Show-Csv "results/week8_decision.csv"
  Run-Remote "cat results/week8_hypotheses.txt results/environment.txt"
}

Write-Host ""
Write-Host "Copying results from the VM to .\m78_results ..." -ForegroundColor Green
& $scp -P $Port -r "${Target}:$Dir/results" ".\m78_results"
Write-Host "Log saved to $log" -ForegroundColor Green
