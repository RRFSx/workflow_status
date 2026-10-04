#!/usr/bin/env python3
"""
workflow_status.py — Unified HPC Rocoto Workflow Monitor

Usage:
  MACHINE=gaeac7 workflow_status.sh exp1.yaml [exp2.yaml ...] [--dry-run] [--verbose]

Features:
  - Automatically loads `common.yaml` / `common.yml` from the config file's directory
    and deep-merges each experiment YAML on top of it
  - Queries `rocotostat -s` and `rocotostat -c` for both realtime & retrospective runs
  - Detects new DEAD jobs (MD5-deduplicated), workflow stalls, and hung jobs (log staleness)
  - Sends email alerts via `mail` only on state transitions
  - Pushes combined status + 7-day rolling history to a single JSON file per experiment
    on GitHub (`status/<cluster>/<exp>.json`) with automatic HTTP 409 retry
  - Pings healthchecks.io dead-man's-switch heartbeat
"""

import argparse
import base64
import copy
import datetime as dt
import hashlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

try:
    import requests
except ImportError:
    requests = None
import urllib.error
import urllib.request

GITHUB_API = "https://api.github.com"
CYCLE_RE = re.compile(r"^\d{12}$")


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge `override` dict on top of `base` dict."""
    result = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result


def load_merged_config(config_file: Path, explicit_common: Optional[Path] = None) -> Dict[str, Any]:
    """Load common.yaml/common.yml (if present) and merge config_file on top."""
    common_cfg: Dict[str, Any] = {}
    candidates: List[Path] = []
    if explicit_common:
        candidates.append(explicit_common)
    else:
        candidates.extend([
            config_file.parent / "common.yaml",
            config_file.parent / "common.yml",
        ])

    for cand in candidates:
        if cand.is_file() and cand.resolve() != config_file.resolve():
            common_cfg = yaml.safe_load(cand.read_text()) or {}
            break

    exp_cfg = yaml.safe_load(config_file.read_text()) or {}
    return deep_merge(common_cfg, exp_cfg)


def parse_rocoto_time(ts_str: Optional[str]) -> Optional[dt.datetime]:
    """Parse Rocoto summary timestamp like 'Oct 03 2026 19:50:08'."""
    if not ts_str or ts_str == "-":
        return None
    for fmt in ("%b %d %Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(ts_str.strip(), fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    return None


def compute_wall_time_min(activated: Optional[str], deactivated: Optional[str]) -> Optional[float]:
    t1 = parse_rocoto_time(activated)
    t2 = parse_rocoto_time(deactivated)
    if t1 and t2 and t2 >= t1:
        return round((t2 - t1).total_seconds() / 60.0, 1)
    return None


def run_rocoto_cmd(cmd: List[str], expdir: Path) -> str:
    """Run a rocoto command inside expdir (rocoto module is loaded by workflow_status.sh)."""
    proc = subprocess.run(
        cmd,
        cwd=str(expdir),
        capture_output=True,
        text=True,
        timeout=120,
    )
    return proc.stdout


def parse_rocotostat(expdir: Path, xml: str, db: str, lookback: int = 6) -> List[Dict[str, Any]]:
    """
    1. Run `rocotostat -w <xml> -d <db> -s` to get all activated cycles & timestamps.
    2. Select all 'Active' cycles + the last `lookback` cycles (works for both realtime & retro).
    3. Run `rocotostat -w <xml> -d <db> -c <selected_cycles>` and parse tasks.
    """
    summary_out = run_rocoto_cmd(["rocotostat", "-w", xml, "-d", db, "-s"], expdir)
    summary_map: Dict[str, Dict[str, Any]] = {}
    cycle_order: List[str] = []

    for raw_line in summary_out.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("CYCLE") or "::" in line:
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        cdate = parts[0]
        if not CYCLE_RE.match(cdate) or int(cdate) >= 210000000000:
            continue
        cstate = parts[1]
        activated = " ".join(parts[2:6])
        deactivated = None
        if len(parts) >= 10 and parts[6] != "-":
            deactivated = " ".join(parts[6:10])

        summary_map[cdate] = {
            "cdate": cdate,
            "cycle_state": cstate,
            "activated": activated,
            "deactivated": deactivated,
            "wall_time_min": compute_wall_time_min(activated, deactivated),
        }
        cycle_order.append(cdate)

    if not cycle_order:
        return []

    active_cycles = [c for c in cycle_order if summary_map[c]["cycle_state"] == "Active"]
    recent_cycles = cycle_order[-lookback:] if lookback > 0 else cycle_order
    selected_cycles = sorted(set(active_cycles + recent_cycles))
    if not selected_cycles:
        return []

    cycle_arg = ",".join(selected_cycles)
    tasks_out = run_rocoto_cmd(["rocotostat", "-w", xml, "-d", db, "-c", cycle_arg], expdir)

    cycles_tasks: Dict[str, List[Dict[str, Any]]] = {c: [] for c in selected_cycles}
    for raw_line in tasks_out.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("CYCLE") or line.startswith("=") or "::" in line:
            continue
        parts = line.split()
        if len(parts) < 7:
            continue
        cdate, task, jobid, state, exit_status, tries, duration = parts[:7]
        if not CYCLE_RE.match(cdate):
            continue

        if state == "-":
            state = "WAITING"

        task_obj = {
            "name": task,
            "state": state,
            "jobid": None if jobid == "-" else jobid,
            "exit_status": None if exit_status == "-" else int(float(exit_status)),
            "tries": None if tries == "-" else int(float(tries)),
            "duration": None if duration == "-" else float(duration),
        }
        cycles_tasks.setdefault(cdate, []).append(task_obj)

    result: List[Dict[str, Any]] = []
    for cdate in selected_cycles:
        s = summary_map.get(cdate, {})
        result.append(
            {
                "cdate": cdate,
                "cycle_state": s.get("cycle_state", "Unknown"),
                "activated": s.get("activated"),
                "deactivated": s.get("deactivated"),
                "wall_time_min": s.get("wall_time_min"),
                "tasks": cycles_tasks.get(cdate, []),
            }
        )
    return result


def build_status_dict(experiment: str, cluster: str, cycles: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {
        "total_cycles": len(cycles),
        "active_cycles": sum(1 for c in cycles if c.get("cycle_state") == "Active"),
        "done_cycles": sum(1 for c in cycles if c.get("cycle_state") == "Done"),
        "total_tasks": 0,
        "succeeded": 0,
        "running": 0,
        "queued": 0,
        "submitting": 0,
        "waiting": 0,
        "dead": 0,
        "other": 0,
    }
    for c in cycles:
        for t in c.get("tasks", []):
            counts["total_tasks"] += 1
            st = (t.get("state") or "").upper()
            if st == "SUCCEEDED":
                counts["succeeded"] += 1
            elif st == "RUNNING":
                counts["running"] += 1
            elif st == "QUEUED":
                counts["queued"] += 1
            elif st == "SUBMITTING":
                counts["submitting"] += 1
            elif st == "WAITING":
                counts["waiting"] += 1
            elif st in ("DEAD", "FAILED"):
                counts["dead"] += 1
            else:
                counts["other"] += 1

    return {
        "experiment": experiment,
        "cluster": cluster,
        "updated_at": utc_now_iso(),
        "monitor_host": socket.gethostname(),
        "cycles": cycles,
        "summary": counts,
        "alerts": {
            "dead_jobs": [],
            "stall": False,
            "stall_since": None,
            "hung_jobs": [],
        },
    }


def load_state(state_file: Path) -> Dict[str, Any]:
    if state_file.is_file():
        try:
            return json.loads(state_file.read_text())
        except Exception:
            pass
    return {
        "last_check": None,
        "dead_jobs_hash": "",
        "dead_jobs_list": [],
        "stall_since": None,
        "stall_alerted": False,
        "hung_alerted": {},
    }


def save_state(state_file: Path, state: Dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(state_file)


def parse_recipients(raw_recip: Any) -> List[str]:
    if isinstance(raw_recip, list):
        out = []
        for item in raw_recip:
            out.extend(re.split(r"[\s,]+", str(item).strip()))
        return [r for r in out if r]
    if isinstance(raw_recip, str):
        return [r for r in re.split(r"[\s,]+", raw_recip.strip()) if r]
    return []


def send_email(subject: str, body: str, recipients: List[str], dry_run: bool) -> None:
    if not recipients:
        logging.warning("No recipients configured for alert: %s", subject)
        return
    if dry_run:
        logging.info("[DRY-RUN] Would send email '%s' to %s", subject, ", ".join(recipients))
        return
    try:
        subprocess.run(
            ["mail", "-s", subject] + recipients,
            input=body,
            text=True,
            check=False,
            timeout=30,
        )
        logging.info("Sent alert email '%s' to %s", subject, ", ".join(recipients))
    except Exception as exc:
        logging.error("Failed to send email '%s': %s", subject, exc)


def check_dead_jobs(status: Dict[str, Any], state: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]]]:
    dead_list: List[Dict[str, Any]] = []
    canonical_lines: List[str] = []

    for c in status.get("cycles", []):
        cdate = c.get("cdate", "")
        for t in c.get("tasks", []):
            if (t.get("state") or "").upper() in ("DEAD", "FAILED"):
                dead_list.append(
                    {
                        "cycle": cdate,
                        "task": t.get("name"),
                        "tries": t.get("tries"),
                        "jobid": t.get("jobid"),
                        "exit_status": t.get("exit_status"),
                    }
                )
                canonical_lines.append(f"{cdate}|{t.get('name')}|{t.get('tries')}")

    status["alerts"]["dead_jobs"] = dead_list

    if not canonical_lines:
        state["dead_jobs_hash"] = ""
        state["dead_jobs_list"] = []
        return False, []

    canonical_lines.sort()
    current_hash = hashlib.md5("\n".join(canonical_lines).encode("utf-8")).hexdigest()
    saved_hash = state.get("dead_jobs_hash", "")

    if current_hash != saved_hash:
        state["dead_jobs_hash"] = current_hash
        state["dead_jobs_list"] = canonical_lines
        return True, dead_list

    return False, dead_list


def check_stall(
    status: Dict[str, Any], state: Dict[str, Any], threshold_sec: int
) -> Tuple[bool, int]:
    summary = status.get("summary", {})
    active_jobs = (
        summary.get("running", 0)
        + summary.get("queued", 0)
        + summary.get("submitting", 0)
    )
    now = int(time.time())

    if active_jobs == 0:
        stall_since = state.get("stall_since")
        if not stall_since:
            state["stall_since"] = now
            state["stall_alerted"] = False
            status["alerts"]["stall"] = False
            status["alerts"]["stall_since"] = None
            return False, 0

        duration = now - int(stall_since)
        if duration > threshold_sec:
            status["alerts"]["stall"] = True
            status["alerts"]["stall_since"] = int(stall_since)
            if not state.get("stall_alerted", False):
                state["stall_alerted"] = True
                return True, duration
        return False, duration
    else:
        state["stall_since"] = None
        state["stall_alerted"] = False
        status["alerts"]["stall"] = False
        status["alerts"]["stall_since"] = None
        return False, 0


def check_hung_jobs(
    status: Dict[str, Any],
    expdir: Path,
    hung_cfg: Dict[str, Any],
    xml: str,
    db: str,
    dry_run: bool,
) -> List[Dict[str, Any]]:
    """Check RUNNING jobs against configured log file staleness rules."""
    rules: List[Dict[str, Any]] = []
    if isinstance(hung_cfg.get("tasks"), list):
        rules = hung_cfg["tasks"]
    elif hung_cfg.get("task") and hung_cfg.get("log_pattern"):
        rules = [
            {
                "name": hung_cfg["task"],
                "log_pattern": hung_cfg["log_pattern"],
                "max_idle_sec": int(hung_cfg.get("max_idle_sec", 1200)),
            }
        ]

    if not rules:
        status["alerts"]["hung_jobs"] = []
        return []

    action = hung_cfg.get("action", "alert")
    now = int(time.time())
    hung_found: List[Dict[str, Any]] = []

    for rule in rules:
        task_name = rule.get("name", "fcst")
        pattern = rule.get("log_pattern", "")
        max_idle = int(rule.get("max_idle_sec", 1200))
        if not pattern:
            continue

        for c in status.get("cycles", []):
            cdate = c.get("cdate", "")
            pdy = cdate[:8]
            cyc = cdate[8:10]
            for t in c.get("tasks", []):
                if t.get("name") == task_name and (t.get("state") or "").upper() == "RUNNING":
                    log_path_str = (
                        pattern.replace("{workdir}", str(expdir))
                        .replace("{expdir}", str(expdir))
                        .replace("{cdate}", cdate)
                        .replace("{PDY}", pdy)
                        .replace("{cyc}", cyc)
                    )
                    log_path = Path(log_path_str)
                    if log_path.is_file():
                        mtime = int(log_path.stat().st_mtime)
                        idle_sec = now - mtime
                        if idle_sec > max_idle:
                            jobid = t.get("jobid")
                            hung_item = {
                                "cycle": cdate,
                                "task": task_name,
                                "jobid": jobid,
                                "idle_sec": idle_sec,
                            }
                            hung_found.append(hung_item)
                            if action == "cancel_and_reboot" and not dry_run and jobid:
                                logging.info("Auto-remediating hung job %s (%s %s)", jobid, cdate, task_name)
                                subprocess.run(["scancel", str(jobid)], check=False, timeout=15)
                                time.sleep(5)
                                run_rocoto_cmd(
                                    ["rocotoboot", "-w", xml, "-d", db, "-c", cdate, "-t", task_name],
                                    expdir,
                                )

    status["alerts"]["hung_jobs"] = hung_found
    return hung_found


def github_request(
    method: str, url: str, token: str, payload: Optional[Dict[str, Any]] = None
) -> Tuple[int, Optional[Dict[str, Any]]]:
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "workflow_status",
    }
    if requests is not None:
        resp = requests.request(method, url, headers=headers, json=payload, timeout=20)
        try:
            data = resp.json() if resp.text else None
        except Exception:
            data = None
        return resp.status_code, data

    req_data = json.dumps(payload).encode("utf-8") if payload is not None else None
    if req_data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except Exception:
        return 0, None


def push_combined_status_to_github(
    status: Dict[str, Any],
    repo: str,
    file_path: str,
    token: str,
    branch: str = "main",
) -> bool:
    """
    Fetch existing `status/<cluster>/<exp>.json` (if any) to get its SHA and rolling 7-day
    `history` array, append the current snapshot to `status['history']`, and PUT the single
    combined JSON file to GitHub with up to 3 retries on HTTP 409/5xx.
    """
    url = f"{GITHUB_API}/repos/{repo}/contents/{file_path}"

    done_cycles = [
        c for c in status.get("cycles", [])
        if c.get("cycle_state") == "Done" and c.get("wall_time_min") is not None
    ]
    latest_wall_min = done_cycles[-1]["wall_time_min"] if done_cycles else None
    latest_cycle = status["cycles"][-1]["cdate"] if status.get("cycles") else "unknown"

    new_entry = {
        "timestamp": status["updated_at"],
        "cycle": latest_cycle,
        "tasks_total": status["summary"]["total_tasks"],
        "succeeded": status["summary"]["succeeded"],
        "running": status["summary"]["running"],
        "dead": status["summary"]["dead"],
        "cycle_wall_time_min": latest_wall_min,
    }

    cutoff_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)
    cutoff_iso = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    for attempt in range(1, 4):
        code, get_data = github_request("GET", f"{url}?ref={branch}", token)
        sha = None
        existing_history: List[Dict[str, Any]] = []

        if code == 200 and get_data:
            sha = get_data.get("sha")
            content_b64 = get_data.get("content", "")
            if content_b64:
                try:
                    raw_json = base64.b64decode(content_b64).decode("utf-8")
                    old_doc = json.loads(raw_json)
                    existing_history = old_doc.get("history", [])
                except Exception:
                    existing_history = []

        trimmed = [
            e for e in existing_history
            if isinstance(e, dict) and e.get("timestamp", "") >= cutoff_iso
        ]
        trimmed.append(new_entry)
        status["history"] = trimmed[-1008:]

        encoded = base64.b64encode((json.dumps(status, indent=2) + "\n").encode("utf-8")).decode("ascii")
        put_payload: Dict[str, Any] = {
            "message": f"status update {file_path} {status['updated_at']}",
            "content": encoded,
            "branch": branch,
        }
        if sha:
            put_payload["sha"] = sha

        put_code, _ = github_request("PUT", url, token, put_payload)
        if put_code in (200, 201):
            return True
        if put_code in (409, 500, 502, 503, 504) and attempt < 3:
            logging.warning("GitHub PUT %s returned %d (attempt %d/3), retrying...", file_path, put_code, attempt)
            time.sleep(1.5 * attempt)
            continue

        logging.error("Failed to push %s to GitHub (HTTP %d)", file_path, put_code)
        return False

    return False


def ping_heartbeat(uuid_str: str, dry_run: bool) -> None:
    if not uuid_str or dry_run:
        return
    url = f"https://hc-ping.com/{uuid_str.strip()}"
    try:
        if requests is not None:
            requests.get(url, timeout=10)
        else:
            urllib.request.urlopen(url, timeout=10).read()
        logging.info("Sent heartbeat ping to healthchecks.io")
    except Exception as exc:
        logging.warning("Heartbeat ping failed: %s", exc)


def resolve_token(dash_cfg: Dict[str, Any], config_dir: Path) -> Optional[str]:
    token_file_str = dash_cfg.get("token_file")
    candidates: List[Path] = []
    if token_file_str:
        p = Path(os.path.expanduser(token_file_str))
        candidates.append(p if p.is_absolute() else (config_dir / p))
    candidates.append(config_dir / "github_token")

    for cand in candidates:
        if cand.is_file():
            tok = cand.read_text().strip()
            if tok:
                return tok
    return None


def process_experiment(
    config_file: Path,
    explicit_common: Optional[Path],
    dry_run: bool,
    heartbeats_to_ping: Set[str],
) -> bool:
    cfg = load_merged_config(config_file, explicit_common)
    config_dir = config_file.parent
    state_dir = config_dir / ".state"
    state_dir.mkdir(parents=True, exist_ok=True)

    exp_cfg = cfg.get("experiment", {})
    exp_name = exp_cfg.get("name")
    cluster = exp_cfg.get("cluster") or os.environ.get("MACHINE") or "unknown"
    expdir_str = exp_cfg.get("expdir")
    xml = exp_cfg.get("workflow_xml", "rrfs.xml")
    db = exp_cfg.get("workflow_db", "rrfs.db")

    if not exp_name or not expdir_str:
        logging.error("Missing required experiment fields (name, expdir) in %s", config_file.name)
        return False

    expdir = Path(expdir_str)
    logging.info("Processing experiment: %s on %s (%s)", exp_name, cluster, expdir)

    if not expdir.is_dir():
        logging.warning("Experiment directory not accessible on %s: %s — skipping", socket.gethostname(), expdir)
        return False

    cycling_cfg = cfg.get("cycling", {})
    lookback = int(cycling_cfg.get("lookback_cycles", 6))

    alerts_cfg = cfg.get("alerts", {})
    recipients = parse_recipients(alerts_cfg.get("recipients", []))
    subject_prefix = alerts_cfg.get("subject_prefix", exp_name)

    checks_cfg = cfg.get("checks", {})
    dead_cfg = checks_cfg.get("dead_jobs", {})
    stall_cfg = checks_cfg.get("stall", {})
    hung_cfg = checks_cfg.get("hung_jobs", {})

    dash_cfg = cfg.get("dashboard", {})
    github_repo = dash_cfg.get("github_repo", "guoqing-noaa/workflow_status")
    github_branch = dash_cfg.get("github_branch", "main")
    status_path = dash_cfg.get("status_path") or f"status/{cluster}/{exp_name}.json"
    github_token = resolve_token(dash_cfg, config_dir)

    hb_cfg = cfg.get("heartbeat", {})
    hc_uuid = (hb_cfg.get("healthchecks_uuid") or "").strip()
    if not hc_uuid and (config_dir / "heartbeat_uuid").is_file():
        hc_uuid = (config_dir / "heartbeat_uuid").read_text().strip()
    if hc_uuid:
        heartbeats_to_ping.add(hc_uuid)

    state_file = state_dir / f"{exp_name}_{cluster}.json"
    local_status_file = state_dir / f"{exp_name}_{cluster}_status.json"
    state = load_state(state_file)

    # 1. Parse rocotostat
    cycles = parse_rocotostat(expdir, xml, db, lookback)
    status = build_status_dict(exp_name, cluster, cycles)

    # 2. Dead job check
    if dead_cfg.get("enabled", True):
        new_dead, dead_list = check_dead_jobs(status, state)
        if new_dead:
            logging.warning("New DEAD job(s) in %s: %s", exp_name, dead_list)
            lines = [
                f"⚠️  Dead job(s) detected in {exp_name} on {cluster}",
                f"Time: {status['updated_at']}",
                "",
                "Dead jobs:",
                "──────────────────────────────────────────",
            ]
            for d in dead_list:
                lines.append(
                    f"  Cycle: {d['cycle']}  Task: {d['task']}  JobID: {d['jobid']}  Exit: {d['exit_status']}  Tries: {d['tries']}"
                )
            send_email(f"{subject_prefix}: dead job(s)", "\n".join(lines), recipients, dry_run)

    # 3. Stall check
    if stall_cfg.get("enabled", True):
        threshold_sec = int(stall_cfg.get("threshold_sec", 3600))
        new_stall, stall_dur = check_stall(status, state, threshold_sec)
        if new_stall:
            stall_min = stall_dur // 60
            logging.warning("Workflow stalled in %s for %d min", exp_name, stall_min)
            body = (
                f"⚠️  Workflow stalled: {exp_name} on {cluster}\n\n"
                f"Time: {status['updated_at']}\n"
                f"No jobs have been RUNNING, QUEUED, or SUBMITTING for {stall_min} minutes.\n"
                f"Please check the workflow and restart if needed."
            )
            send_email(f"{subject_prefix}: workflow stalled", body, recipients, dry_run)

    # 4. Hung job check
    if hung_cfg.get("enabled", False):
        hung_list = check_hung_jobs(status, expdir, hung_cfg, xml, db, dry_run)
        if hung_list:
            action = hung_cfg.get("action", "alert")
            logging.warning("Hung job(s) in %s: %s", exp_name, hung_list)
            lines = [
                f"⚠️  Hung job(s) detected in {exp_name} on {cluster}",
                f"Time: {status['updated_at']}",
                "",
                "The following RUNNING jobs have stale log files:",
                "──────────────────────────────────────────",
            ]
            for h in hung_list:
                lines.append(
                    f"  Cycle: {h['cycle']}  Task: {h['task']}  JobID: {h['jobid']}  Idle: {h['idle_sec'] // 60}m"
                )
            lines.append("")
            if action == "cancel_and_reboot":
                lines.append("Action taken: Jobs were cancelled and rebooted via rocotoboot.")
            else:
                lines.append("No automatic action taken. Please investigate.")
            send_email(f"{subject_prefix}: hung job(s)", "\n".join(lines), recipients, dry_run)

    # 5. Save state
    state["last_check"] = status["updated_at"]
    save_state(state_file, state)

    # 6. Push combined status + history JSON to GitHub
    if github_token and not dry_run:
        logging.info("Pushing status + history to GitHub: %s", status_path)
        push_combined_status_to_github(status, github_repo, status_path, github_token, github_branch)
    else:
        if not github_token:
            logging.warning("No GitHub token configured — skipping GitHub push for %s", exp_name)
        done_cycles = [
            c for c in status.get("cycles", [])
            if c.get("cycle_state") == "Done" and c.get("wall_time_min") is not None
        ]
        status["history"] = [
            {
                "timestamp": status["updated_at"],
                "cycle": status["cycles"][-1]["cdate"] if status.get("cycles") else "unknown",
                "tasks_total": status["summary"]["total_tasks"],
                "succeeded": status["summary"]["succeeded"],
                "running": status["summary"]["running"],
                "dead": status["summary"]["dead"],
                "cycle_wall_time_min": done_cycles[-1]["wall_time_min"] if done_cycles else None,
            }
        ]
        if dry_run:
            logging.info("[DRY-RUN] Would push status + history to GitHub: %s", status_path)

    local_status_file.write_text(json.dumps(status, indent=2) + "\n")
    logging.info("Saved local status JSON: %s", local_status_file)

    s = status["summary"]
    logging.info(
        "Done %s: cycles=%d (active=%d, done=%d), tasks=%d (ok=%d, run=%d, wait=%d, dead=%d)",
        exp_name,
        s["total_cycles"],
        s["active_cycles"],
        s["done_cycles"],
        s["total_tasks"],
        s["succeeded"],
        s["running"],
        s["waiting"],
        s["dead"],
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unified HPC Rocoto Workflow Monitor (inherits shared settings from common.yaml)"
    )
    parser.add_argument(
        "configs",
        nargs="+",
        help="One or more experiment YAML config files (e.g., exp1.yaml exp2.yaml)",
    )
    parser.add_argument(
        "--common",
        help="Optional path to common.yaml (defaults to common.yaml/common.yml alongside each config file)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run checks locally without sending emails or pushing to GitHub",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    config_files: List[Path] = []
    for raw in args.configs:
        p = Path(raw).resolve()
        if not p.is_file():
            print(f"ERROR: Config file not found: {raw}", file=sys.stderr)
            return 1
        config_files.append(p)

    explicit_common = Path(args.common).resolve() if args.common else None

    first_state_dir = config_files[0].parent / ".state"
    first_state_dir.mkdir(parents=True, exist_ok=True)
    log_file = first_state_dir / "monitor.log"
    if log_file.is_file() and log_file.stat().st_size > 1048576:
        log_file.replace(first_state_dir / "monitor.log.prev")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="[%(asctime)s UTC] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.Formatter.converter = time.gmtime

    logging.info(
        "=== Monitor run started (MACHINE=%s, dry_run=%s) ===",
        os.environ.get("MACHINE", "unset"),
        args.dry_run,
    )

    ok_count = 0
    heartbeats_to_ping: Set[str] = set()

    for cf in config_files:
        try:
            if process_experiment(cf, explicit_common, args.dry_run, heartbeats_to_ping):
                ok_count += 1
        except Exception as exc:
            logging.exception("Error processing %s: %s", cf.name, exc)

    if ok_count > 0:
        for uuid_str in sorted(heartbeats_to_ping):
            ping_heartbeat(uuid_str, args.dry_run)

    logging.info("=== Monitor run completed (%d/%d experiments succeeded) ===", ok_count, len(config_files))
    return 0 if ok_count > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
