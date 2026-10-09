#!/usr/bin/env bash
# ens_fcst_rescue.sh — Rescue a dead ensemble forecast task (fcst_mXXX)
#
# Copies the pure 1h forecast background mpasout file from the previous cycle:
#   ${COMROOT}/${NET}/${rrfs_ver}/${RUN}.${PDY_prev}/${cyc_prev}/fcst/enkf/memXXX/mpasout.${timestr}.nc
# to the current cycle's prep_ic directory:
#   ${DATAROOT}/${PDY}/${RUN}_prep_ic_${cyc}_${rrfs_ver}/enkf/memXXX/mpasout.nc
# and rewinds the dead fcst_mXXX task via rocotorewind so rocotorun resubmits it cleanly.

set -euo pipefail

unset SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU SLURM_MEM_PER_GPU

SRC_FILE="${SRC_FILE:?ERROR: SRC_FILE is required}"
DST_FILE="${DST_FILE:?ERROR: DST_FILE is required}"
FCST_DIR="${FCST_DIR:-}"
EXPDIR="${EXPDIR:?ERROR: EXPDIR is required}"
WORKFLOW_XML="${WORKFLOW_XML:-rrfs.xml}"
WORKFLOW_DB="${WORKFLOW_DB:-rrfs.db}"
CDATE="${CDATE:?ERROR: CDATE is required}"
TASK_NAME="${TASK_NAME:?ERROR: TASK_NAME is required}"
MACHINE="${MACHINE:?ERROR: MACHINE is required}"
ROCOTO_MOD="${ROCOTO_MOD:-rocoto/1.3.7g}"

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting ensemble forecast rescue for ${TASK_NAME} at cycle ${CDATE}"
echo "  SRC_FILE: ${SRC_FILE}"
echo "  DST_FILE: ${DST_FILE}"
echo "  FCST_DIR: ${FCST_DIR}"
echo "  EXPDIR:   ${EXPDIR}"

if [[ ! -s "${SRC_FILE}" ]]; then
  echo "ERROR: Source 1h forecast mpasout file not found or empty: ${SRC_FILE}" >&2
  exit 1
fi

DST_DIR="$(dirname "${DST_FILE}")"
if [[ ! -d "${DST_DIR}" ]]; then
  echo "ERROR: Destination prep_ic directory not found: ${DST_DIR}" >&2
  exit 1
fi

# 1. Move previous mpasout.nc under prep_ic to bad.mpasout.nc for debugging, then copy 1h forecast mpasout file
if [[ -e "${DST_FILE}" || -L "${DST_FILE}" ]]; then
  mv -f "${DST_FILE}" "${DST_DIR}/bad.mpasout.nc"
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Moved previous ${DST_FILE} to ${DST_DIR}/bad.mpasout.nc"
fi
cp -f "${SRC_FILE}" "${DST_FILE}"
touch "${DST_DIR}/ens_fcst_rescue.done"
echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Successfully copied mpasout.nc to ${DST_FILE}"

# 2. Clean up leftover files in the member's forecast umbrella directory
if [[ -n "${FCST_DIR}" && -d "${FCST_DIR}" ]]; then
  rm -rf "${FCST_DIR:?}"/*
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Cleaned leftover forecast files in ${FCST_DIR}"
fi

# 3. Load Rocoto module for this machine
command -v module &>/dev/null || { [[ -f /etc/profile ]] && source /etc/profile 2>/dev/null || true; }

case "${MACHINE}" in
  hera)
    module use /scratch4/BMC/zrtrr/gge/rocoto_hera/modulefiles 2>/dev/null || true
    ;;
  ursa)
    module use /scratch4/BMC/zrtrr/gge/rocoto/modulefiles 2>/dev/null || true
    ;;
  derecho)
    [[ -f /etc/profile.d/z00_modules.sh ]] && source /etc/profile.d/z00_modules.sh 2>/dev/null || true
    module use /glade/work/geguo/rocoto/modulefiles 2>/dev/null || true
    ;;
  orion)
    module use /work/noaa/zrtrr/gge/rocoto/modulefiles 2>/dev/null || true
    ;;
  hercules)
    module use /work/noaa/zrtrr/gge/hercules/rocoto/modulefiles 2>/dev/null || true
    ;;
  gaeac?)
    if [[ -d /gpfs/f6 ]]; then
      module use /gpfs/f6/arfs-gsl/world-shared/gge/rocoto/modulefiles 2>/dev/null || true
    elif [[ -d /gpfs/f7 ]]; then
      module use /gpfs/f7/arfs-gsl/world-shared/gge/rocoto/modulefiles 2>/dev/null || true
    fi
    ;;
esac

if command -v module &>/dev/null; then
  module load "${ROCOTO_MOD}" 2>/dev/null || true
fi

# 4. Rewind the dead fcst_mXXX task so rocotorun resubmits it cleanly from the login node
cd "${EXPDIR}"
echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Running: rocotorewind -w ${WORKFLOW_XML} -d ${WORKFLOW_DB} -c ${CDATE} -t ${TASK_NAME}"
rocotorewind -w "${WORKFLOW_XML}" -d "${WORKFLOW_DB}" -c "${CDATE}" -t "${TASK_NAME}"
echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Rescue completed for ${TASK_NAME} (${CDATE})"
