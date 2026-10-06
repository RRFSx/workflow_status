# HPC Workflow Status

A config-driven Python monitoring system for Rocoto-based HPC workflows with a GitHub Pages dashboard, email alerting, and multi-layer failure detection.

## Features

- **Untracked [`myexps.yml`](config.yml) user configuration** — copy the tracked template [`config.yml`](config.yml) to `myexps.yml` (git-ignored) so sensitive information (email addresses, local experiment paths) stays out of GitHub and `git pull` never conflicts.
- **Full `rocotostat` structured parsing** — uses `rocotostat -s` to discover all `Active` cycles + the last $N$ `Done` cycles (works identically for **realtime** and **retrospective** workflows), then parses full task state and cycle wall-clock duration.
- **Dead job detection** — alerts on new `DEAD` jobs with MD5 deduplication (no repeated emails for the same failure).
- **Stall detection** — alerts when no jobs are running/queued/submitting beyond a configurable threshold.
- **Hung job detection** — optionally checks one or more `RUNNING` task types (`fcst`, `jedivar`, etc.) for stale log files, with optional `cancel_and_reboot` auto-remediation.
- **Zero-conflict per-HPC branches (`status-<MACHINE>`)** — each HPC pushes `<exp>.json` directly to its own dedicated branch `status-<MACHINE>` (`https://raw.githubusercontent.com/<owner>/<repo>/status-<machine>/<exp>.json`) without modifying the working tree or `main` branch.
- **Three-layer failure detection**:
  1. **healthchecks.io heartbeat** — detects monitor or cluster/scrontab death within ~15 min.
  2. **GitHub Actions watchdog** ([`.github/workflows/stale-check.yml`](.github/workflows/stale-check.yml)) — opens a GitHub Issue if any `<exp>.json` on any `status-*` branch goes $>30$ min stale.
  3. **Dashboard UI** ([`docs/index.html`](docs/index.html)) — color-coded staleness indicator visible at a glance.

## Prerequisites

1. **Git SSH access** (`git@github.com:...`) configured on the HPC cluster (with an SSH key that does not prompt for an interactive passphrase when running under `scrontab`).
2. **`pyDAmonitor` Conda environment** (`Miniforge3/envs/pyDAmonitor/bin/python3`), invoked directly by [`run.sh`](run.sh) without needing `conda activate`.
3. **Rocoto** module on the target HPC cluster (automatically loaded by [`run.sh`](run.sh)).

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

### 2. Copy `config.yml` to `myexps.yml` and Edit

Copy the example template [`config.yml`](config.yml) to `myexps.yml` (which is ignored by git so sensitive information stays local) and configure `common:` defaults and your `experiments:` list:

```bash
cp config.yml myexps.yml
```

```yaml
common:
  default_exp: ursa/rrfsv2x_det
  workflow_xml: rrfs.xml
  workflow_db: rrfs.db
  lookback_cycles: 72
  recipients:
    - first.last@noaa.gov
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

By default, [`run.sh`](run.sh) reads `myexps.yml` in the repo root, or you can specify any custom `.yml` config file on the command line:

```bash
# Use default myexps.yml
MACHINE=gaeac7 ./run.sh --dry-run

# Or specify a custom YAML config file
MACHINE=gaeac7 ./run.sh custom_exps.yml --dry-run
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
*/10 * * * * MACHINE=gaeac7 /path/to/workflow_status/run.sh
# Or with a custom config file:
# */10 * * * * MACHINE=gaeac7 /path/to/workflow_status/run.sh /path/to/custom_exps.yml
```

## Repository Structure

```
workflow_status/
├── README.md
├── config.yml                   # Example config template (copy to myexps.yml)
├── myexps.yml                   # Untracked local user config (git-ignored)
├── run.sh                       # Thin launcher (uses MACHINE to set Rocoto + pyDAmonitor python3)
├── workflow_status.py           # Core monitoring engine (pushes <exp>.json to branch status-<MACHINE>)
├── docs/                        # GitHub Pages dashboard (reads status-* branches across all HPCs)
│   └── index.html
└── .github/
    └── workflows/
        └── stale-check.yml      # GitHub Actions 30-min staleness watchdog across status-* branches
```
