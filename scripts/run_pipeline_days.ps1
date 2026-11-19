# Run Bronze → Silver → Gold dims → Gold facts for each pipeline day, then prove defects.
# Shows a progress board between steps.
#
# Usage (from repo root):
#   .\scripts\run_pipeline_days.ps1
#   .\scripts\run_pipeline_days.ps1 -Dates 2026-11-11,2026-11-12,2026-11-13
#   .\scripts\run_pipeline_days.ps1 -SkipGenerate
#   .\scripts\run_pipeline_days.ps1 -SkipProve
#   .\scripts\run_pipeline_days.ps1 -SkipGenerate -Dates 2026-11-13

param(
    [string[]]$Dates = @(
        "2026-11-10",
        "2026-11-11",
        "2026-11-12",
        "2026-11-13"
    ),
    [switch]$SkipGenerate,
    [switch]$SkipProve,
    [string]$ProveDate = "2026-11-13"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

$DayDags = @(
    "05_bronze_load",
    "07_silver",
    "08_gold_dims",
    "09_gold_facts"
)

$Steps = [System.Collections.Generic.List[object]]::new()
$i = 0

if (-not $SkipGenerate) {
    $i++
    $Steps.Add([pscustomobject]@{
        Index = $i; Kind = "generate"; Label = "generate_complex.py"
        Date = "-"; DagId = "-"; State = "pending"; Started = $null; Ended = $null; Sec = $null
    })
}

foreach ($d in $Dates) {
    foreach ($dag in $DayDags) {
        $i++
        $Steps.Add([pscustomobject]@{
            Index = $i; Kind = "dag"; Label = "$dag @ $d"
            Date = $d; DagId = $dag; State = "pending"; Started = $null; Ended = $null; Sec = $null
        })
    }
}

if (-not $SkipProve) {
    $i++
    $Steps.Add([pscustomobject]@{
        Index = $i; Kind = "prove"; Label = "10_prove_defects @ $ProveDate"
        Date = $ProveDate; DagId = "10_prove_defects"; State = "pending"
        Started = $null; Ended = $null; Sec = $null
    })
}

$Total = $Steps.Count
$ScriptStart = Get-Date

function Get-StateGlyph([string]$State) {
    switch ($State) {
        "pending" { "[ ]" }
        "running" { "[~]" }
        "done"    { "[x]" }
        "failed"  { "[!]" }
        default   { "[?]" }
    }
}

function Get-StateColor([string]$State) {
    switch ($State) {
        "pending" { "DarkGray" }
        "running" { "Yellow" }
        "done"    { "Green" }
        "failed"  { "Red" }
        default   { "White" }
    }
}

function Show-ProgressBoard {
    param([string]$Headline = "PIPELINE PROGRESS", [int]$CurrentIndex = 0)

    $done = @($Steps | Where-Object { $_.State -eq "done" }).Count
    $failed = @($Steps | Where-Object { $_.State -eq "failed" }).Count
    $running = @($Steps | Where-Object { $_.State -eq "running" }).Count
    $pending = @($Steps | Where-Object { $_.State -eq "pending" }).Count
    $elapsed = ((Get-Date) - $ScriptStart).ToString("hh\:mm\:ss")
    $pct = if ($Total -gt 0) { [int](100 * $done / $Total) } else { 0 }

    Write-Host ""
    Write-Host ("=" * 72) -ForegroundColor Cyan
    Write-Host " $Headline" -ForegroundColor Cyan
    Write-Host ("=" * 72) -ForegroundColor Cyan
    Write-Host (" Overall: {0}/{1} done ({2}%) | running={3} pending={4} failed={5} | elapsed {6}" -f `
        $done, $Total, $pct, $running, $pending, $failed, $elapsed)
    if ($CurrentIndex -gt 0) {
        Write-Host (" Current step: {0}/{1}" -f $CurrentIndex, $Total) -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host " Days:" -ForegroundColor Magenta
    foreach ($d in $Dates) {
        $daySteps = @($Steps | Where-Object { $_.Date -eq $d -and $_.Kind -eq "dag" })
        if ($daySteps.Count -eq 0) { continue }
        $dayDone = @($daySteps | Where-Object { $_.State -eq "done" }).Count
        $dayFail = @($daySteps | Where-Object { $_.State -eq "failed" }).Count
        $dayRun  = @($daySteps | Where-Object { $_.State -eq "running" }).Count
        $dayState = if ($dayFail -gt 0) { "failed" }
                    elseif ($dayDone -eq $daySteps.Count) { "done" }
                    elseif ($dayRun -gt 0 -or $dayDone -gt 0) { "running" }
                    else { "pending" }
        Write-Host ("   {0} {1}  ({2}/{3} layer DAGs)" -f (Get-StateGlyph $dayState), $d, $dayDone, $daySteps.Count) `
            -ForegroundColor (Get-StateColor $dayState)
    }
    Write-Host ""
    Write-Host " Steps:" -ForegroundColor Magenta
    foreach ($s in $Steps) {
        $secTxt = if ($null -ne $s.Sec) { "{0,6:N0}s" -f $s.Sec } else { "      -" }
        Write-Host ("   {0} {1,2}/{2}  {3,-42}  {4,-8}  {5}" -f `
            (Get-StateGlyph $s.State), $s.Index, $Total, $s.Label, $s.State, $secTxt) `
            -ForegroundColor (Get-StateColor $s.State)
    }
    Write-Host ("=" * 72) -ForegroundColor Cyan
    Write-Host ""
}

function Set-StepState {
    param([int]$Index, [string]$State, [datetime]$At = (Get-Date))
    $s = $Steps | Where-Object { $_.Index -eq $Index } | Select-Object -First 1
    if (-not $s) { return }
    $s.State = $State
    if ($State -eq "running") {
        $s.Started = $At
        $s.Ended = $null
        $s.Sec = $null
    }
    elseif ($State -in @("done", "failed")) {
        $s.Ended = $At
        if ($s.Started) {
            $s.Sec = [math]::Round(($s.Ended - $s.Started).TotalSeconds, 0)
        }
    }
}

function Invoke-Step {
    param($Step)

    Set-StepState -Index $Step.Index -State "running"
    Show-ProgressBoard -Headline "RUNNING: $($Step.Label)" -CurrentIndex $Step.Index

    try {
        if ($Step.Kind -eq "generate") {
            Write-Host ">> python scripts\generate_complex.py" -ForegroundColor Yellow
            python scripts\generate_complex.py
            if ($LASTEXITCODE -ne 0) { throw "generate_complex.py exit $LASTEXITCODE" }
        }
        else {
            Write-Host (">> docker compose run --rm airflow-cli airflow dags test {0} {1}" -f `
                $Step.DagId, $Step.Date) -ForegroundColor Yellow
            docker compose run --rm airflow-cli airflow dags test $Step.DagId $Step.Date
            if ($LASTEXITCODE -ne 0) {
                throw "FAILED: $($Step.DagId) $($Step.Date) (exit $LASTEXITCODE)"
            }
        }
        Set-StepState -Index $Step.Index -State "done"
        Show-ProgressBoard -Headline "DONE: $($Step.Label)" -CurrentIndex $Step.Index
    }
    catch {
        Set-StepState -Index $Step.Index -State "failed"
        Show-ProgressBoard -Headline "FAILED: $($Step.Label)" -CurrentIndex $Step.Index
        throw
    }
}

Write-Host ""
Write-Host "Plan: $($Dates.Count) day(s) x $($DayDags.Count) layer DAGs" -ForegroundColor White
if (-not $SkipGenerate) { Write-Host "  + generate_complex.py first" }
if (-not $SkipProve) { Write-Host "  + 10_prove_defects @ $ProveDate last" }
Write-Host "  = $Total total steps"

Show-ProgressBoard -Headline "START — all steps pending"

foreach ($step in $Steps) {
    Invoke-Step -Step $step
}

Show-ProgressBoard -Headline "COMPLETE — all steps finished"
Write-Host ("Finished OK in {0}." -f ((Get-Date) - $ScriptStart).ToString("hh\:mm\:ss")) -ForegroundColor Green
