<#
  run_m910.ps1 - run the Milestone 9 and 10 experiments in the VM over SSH, show the results here, copy them back.
    powershell -ExecutionPolicy Bypass -File .\run_m910.ps1                 # demos, about 5 min
    powershell -ExecutionPolicy Bypass -File .\run_m910.ps1 -Mode quick     # about 35 min
    powershell -ExecutionPolicy Bypass -File .\run_m910.ps1 -Mode full      # about 100 min (use nohup in docs/RUN_M910.md if SSH may drop)
  UNTESTED in PowerShell (I could not run it here): check the first run's output.
#>
param([string]$Target = "victor@127.0.0.1", [int]$Port = 2222, [string]$Dir = "~/tower_fm",
      [ValidateSet("demo","quick","full")][string]$Mode = "demo")
$ssh = "C:\Windows\System32\OpenSSH\ssh.exe"; $scp = "C:\Windows\System32\OpenSSH\scp.exe"
$log = "m910_run_$(Get-Date -Format 'yyyyMMdd_HHmm').log"
function Run-Remote([string]$cmd) {
  Write-Host ""; Write-Host ">>> $cmd" -ForegroundColor Cyan
  & $ssh -p $Port $Target "cd $Dir; $cmd" 2>&1 | Tee-Object -FilePath $log -Append
}
Run-Remote "pkill -f 'nodes[.]run'; sleep 1; python3 cluster.py | tail -2"
if ($Mode -eq "demo") {
  Run-Remote "python3 -u -m experiments.week9_transparency --demo"
  Run-Remote "pkill -f 'nodes[.]run'; sleep 2; python3 -u -m experiments.week10_naming_procs --demo"
} else {
  $flag = ""; if ($Mode -eq "quick") { $flag = "--quick" }
  Run-Remote "python3 -u -m experiments.week9_transparency $flag | tee results/week9_run.txt"
  Run-Remote "pkill -f 'nodes[.]run'; sleep 2; python3 -u -m experiments.week10_naming_procs $flag | tee results/week10_run.txt"
  Run-Remote "cat results/week9_hypotheses.txt results/week10_hypotheses.txt results/environment.txt"
}
Write-Host "Copying results to .\m910_results ..." -ForegroundColor Green
& $scp -P $Port -r "${Target}:$Dir/results" ".\m910_results"
Write-Host "Log saved to $log" -ForegroundColor Green
