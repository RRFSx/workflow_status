#!/usr/bin/env bash
# run.sh — Launcher for workflow_status.py on NOAA HPC clusters
#
# Resolves the machine's pyDAmonitor Python executable and Rocoto module,
# then executes workflow_status.py without needing `conda activate`.
#
# Usage:
#   MACHINE=gaeac7 ./run.sh [myexps.yml] [--dry-run] [--verbose]

set -o pipefail

# Unset SLURM memory variables inherited from scrontab
unset SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU SLURM_MEM_PER_GPU

export CALLER_PWD="${PWD:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)"
cd "${SCRIPT_DIR}" 2>/dev/null
ROCOTO_MOD="${ROCOTO_MOD:-rocoto/1.3.7g}"

if [[ -z "${MACHINE:-}" ]]; then
  echo "ERROR: MACHINE environment variable is required." >&2
  echo "Usage: MACHINE=<gaeac6|gaeac7|hera|ursa|orion|hercules|derecho> $(basename "$0") [myexps.yml] [--dry-run] [--verbose]" >&2
  exit 1
fi
export MACHINE

# ── Load Rocoto Module & Set Miniforge3 BASEDIR ─────────────────────────
command -v module &>/dev/null || { [[ -f /etc/profile ]] && source /etc/profile 2>/dev/null || true; }

BASEDIR=""
case "${MACHINE}" in
  hera)
    BASEDIR=/scratch3/BMC/wrfruc/hera/Miniforge3
    module use /scratch4/BMC/zrtrr/gge/rocoto_hera/modulefiles 2>/dev/null || true
    ;;
  ursa)
    BASEDIR=/scratch3/BMC/wrfruc/gge/Miniforge3
    module use /scratch4/BMC/zrtrr/gge/rocoto/modulefiles 2>/dev/null || true
    ;;
  derecho)
    BASEDIR=/glade/work/geguo/Miniforge3
    [[ -f /etc/profile.d/z00_modules.sh ]] && source /etc/profile.d/z00_modules.sh 2>/dev/null || true
    module use /glade/work/geguo/rocoto/modulefiles 2>/dev/null || true
    ;;
  orion)
    BASEDIR=/work/noaa/zrtrr/gge/Miniforge3
    module use /work/noaa/zrtrr/gge/rocoto/modulefiles 2>/dev/null || true
    ;;
  hercules)
    BASEDIR=/work/noaa/zrtrr/gge/hercules/Miniforge3
    module use /work/noaa/zrtrr/gge/hercules/rocoto/modulefiles 2>/dev/null || true
    ;;
  gaeac?)
    if [[ -d /gpfs/f6 ]]; then
      BASEDIR=/gpfs/f6/bil-fire10-oar/world-shared/gge/Miniforge3
      module use /gpfs/f6/arfs-gsl/world-shared/gge/rocoto/modulefiles 2>/dev/null || true
    elif [[ -d /gpfs/f7 ]]; then
      BASEDIR=/gpfs/f7/wrfruc/world-shared/gge/Miniforge3
      module use /gpfs/f7/arfs-gsl/world-shared/gge/rocoto/modulefiles 2>/dev/null || true
    else
      echo "ERROR: unsupported gaea cluster: ${MACHINE}" >&2
      exit 1
    fi
    ;;
  local)
    ;;
  *)
    echo "ERROR: unsupported MACHINE=${MACHINE}" >&2
    exit 1
    ;;
esac

if command -v module &>/dev/null; then
  module load "${ROCOTO_MOD}" 2>/dev/null || true
fi

if [[ -n "${BASEDIR}" && -x "${BASEDIR}/envs/pyDAmonitor/bin/python3" ]]; then
  PYTHON_BIN="${BASEDIR}/envs/pyDAmonitor/bin/python3"
elif [[ -n "${PYDAMONITOR_PYTHON:-}" && -x "${PYDAMONITOR_PYTHON}" ]]; then
  PYTHON_BIN="${PYDAMONITOR_PYTHON}"
else
  echo "ERROR: pyDAmonitor python3 not found at ${BASEDIR}/envs/pyDAmonitor/bin/python3 (MACHINE=${MACHINE})" >&2
  exit 1
fi

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/workflow_status.py" "$@"
