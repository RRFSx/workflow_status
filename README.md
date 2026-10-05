# HPC Workflow Monitor (`workflow_status`)

A config-driven Python monitoring system for Rocoto-based HPC workflows with a GitHub Pages dashboard, email alerting, and multi-layer failure detection.

## Features

- **Single [`config.yml`](config.yml) configuration** — define shared defaults under `common:` and list all experiments under `experiments:` (each experiment inherits `common:` and can override any setting).
- **Full `rocotostat` structured parsing** — uses `rocotostat -s` to discover all `Active` cycles + the last $N$ `Done` cycles (works identically for **realtime** and **retrospective** workflows), then parses full task state and cycle wall-clock duration.
- **Dead job detection** — alerts on new `DEAD` jobs with MD5 deduplication (no repeated emails for the same failure).
- **Stall detection** — alerts when no jobs are running/queued/submitting beyond a configurable threshold.
- **Hung job detection** — optionally checks one or more `RUNNING` task types (`fcst`, `jedivar`, etc.) for stale log files, with optional `cancel_and_reboot` auto-remediation.
- **Zero-conflict per-HPC branches (`status-<MACHINE>`)** — each HPC pushes `<exp>.json` (containing both current status and rolling 7-day `history`) directly to its own dedicated branch `status-<MACHINE>` (`https://raw.githubusercontent.com/<owner>/<repo>/status-<machine>/<exp>.json`) without modifying the working tree or `main` branch.
- **Three-layer failure detection**:
  1. **healthchecks.io heartbeat** — detects monitor or cluster/scrontab death within ~15 min.
  2. **GitHub Actions watchdog** ([`.github/workflows/stale-check.yml`](.github/workflows/stale-check.yml)) — opens a GitHub Issue if any `<exp>.json` on any `status-*` branch goes $>30$ min stale.
  3. **Dashboard UI** ([`docs/index.html`](docs/index.html)) — color-coded staleness indicator visible at a glance.

## Prerequisites

1. **Git SSH access** (`git@github.com:...`) configured on the HPC cluster (with an SSH key that does not prompt for an interactive passphrase when running under `scrontab`).
2. **`pyDAmonitor` Conda environment** (`Miniforge3/envs/pyDAmonitor/bin/python3`), invoked directly by [`workflow_status.sh`](workflow_status.sh) without needing `conda activate`.
3. **Rocoto** module on the target HPC cluster (automatically loaded by [`workflow_status.sh`](workflow_status.sh)).

## Supported HPC Systems (`MACHINE` values)

| `MACHINE` | Status Branch | Rocoto Module Path | `pyDAmonitor` Base Directory (`Miniforge3`) |
| :--- | :--- | :--- | :--- |
| **`gaeac6`** | `status-gaeac6` | `/gpfs/f6/arfs-gsl/world-shared/gge/rocoto/modulefiles` | `/gpfs/f6/bil-fire10-oar/world-shared/gge/Miniforge3` |
| **`gaeac7`** | `status-gaeac7` | `/gpfs/f7/arfs-gsl/world-shared/gge/rocoto/modulefiles` | `/gpfs/f7/wrfruc/world-shared/gge/Miniforge3` |
| **`hera`** | `status-hera` | `/scratch4/BMC/zrtrr/gge/rocoto_hera/modulefiles` | `/scratch3/BMC/wrfruc/hera/Miniforge3` |
| **`ursa`** | `status-ursa` | `/scratch4/BMC/zrtrr/gge/rocoto/modulefiles` | `/scratch3/BMC/wrfruc/gge/Miniforge3` |
| **`orion`** | `status-orion` | `/work/noaa/zrtrr/gge/rocoto/modulefiles` | `/work/noaa/zrtrr/gge/Miniforge3` |
| **`hercules`** | `status-hercules` | `/work/noaa/zrtrr/gge/hercules/rocoto/modulefiles` | `/work/noaa/zrtrr/gge/hercules/Miniforge3` |
| **`derecho`** | `status-derecho` | `/glade/work/geguo/rocoto/modulefiles` | `/glade/work/geguo/Miniforge3` |

## Quick Start

### 1. Clone the Repo via SSH on HPC

```bash
git clone git@github.com:guoqing-noaa/workflow_status.git
cd workflow_status
```

### 2. Edit `config.yml`

Edit [`config.yml`](config.yml) to configure `common:` defaults and your list of `experiments:`:

```yaml
common:
  workflow_xml: rrfs.xml
  workflow_db: rrfs.db
  lookback_cycles: 4
  recipients:
    - Guoqing.Ge@noaa.gov
  checks:
    dead_jobs:
      enabled: true
    stall:
      enabled: true
      threshold_sec: 3600

experiments:
  - name: rrfsdet_rt
    expdir: /gpfs/f7/arfs-gsl/world-shared/gge/rrfs2/OPSROOT/conus12km/exp/rrfsdet
    subject_prefix: rrfsv2x_rt
```

*(Optional)* For `healthchecks.io` dead-man's-switch monitoring, put your UUID in an untracked `healthchecks_uuid.txt` file at the repo root (git-ignored):

```bash
echo 'YOUR-UUID-HERE' > healthchecks_uuid.txt
chmod 600 healthchecks_uuid.txt
```

### 3. Test (`--dry-run`)

By default, [`workflow_status.sh`](workflow_status.sh) reads `config.yml` in the repo root (or you can pass a custom `.yml` path):

```bash
MACHINE=gaeac7 ./workflow_status.sh --dry-run
```

### 4. Add to `scrontab`

```
#SCRON --partition=cron_c7
#SCRON --account=arfs-gsl
#SCRON --time=00:10:00
#SCRON --mem=8G
#SCRON --dependency=singleton
#SCRON --job-name=workflow_status
#SCRON --output=/dev/null
*/10 * * * * MACHINE=gaeac7 /path/to/workflow_status/workflow_status.sh
```

## Repository Structure

```
workflow_status/
├── README.md
├── config.yml                   # Unified config (common defaults + experiments list)
├── workflow_status.sh           # Thin launcher (uses MACHINE to set Rocoto + pyDAmonitor python3)
├── workflow_status.py           # Core monitoring engine (pushes <exp>.json to branch status-<MACHINE>)
├── docs/                        # GitHub Pages dashboard (reads status-* branches across all HPCs)
│   └── index.html
└── .github/
    └── workflows/
        └── stale-check.yml      # GitHub Actions 30-min staleness watchdog across status-* branches
```
