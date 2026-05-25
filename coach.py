import os
import json
import time
import shutil
import difflib
import subprocess
from datetime import datetime
from pathlib import Path

# =========================
# STACKIQ AUTONOMOUS COACH
# =========================

ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "agent_state.json"
RUNS_DIR = ROOT / "agent_runs"
BACKUP_DIR = ROOT / "agent_backups"

HEALTH_URL = "http://127.0.0.1:8000/health"
BEST_PICK_URL = "http://127.0.0.1:8000/best_pick_v2"

TARGET_FILES = [
    "app.py",
    "best_pick_v2.py",
    "engine.py",
    "data_fetcher.py",
    "scoring_engine.py",
    "execution_engine.py",
    "indicator_engine.py",
    "llm_client.py",
    "llm_config.py",
    "llm_prompts.py",
    "llm_services.py",
]

REQUIRED_FIELDS = [
    "symbol",
    "entry",
    "stop",
    "take_profit",
    "confidence",
]

OPTIONAL_ALIAS_GROUPS = {
    "take_profit": ["take_profit", "target", "targets", "profit_target"],
    "confidence": ["confidence", "confidence_0_10", "ai_confidence", "conviction"],
    "entry": ["entry", "entry_price", "buy_price"],
    "stop": ["stop", "stop_loss", "risk_stop"],
    "symbol": ["symbol", "ticker"],
}

FIELD_DESCRIPTIONS = {
    "symbol": "Returned stock ticker or symbol.",
    "entry": "Suggested entry price or entry level.",
    "stop": "Suggested stop loss or invalidation level.",
    "take_profit": "Suggested take-profit target or target band.",
    "confidence": "Confidence or conviction metric.",
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dirs() -> None:
    RUNS_DIR.mkdir(exist_ok=True)
    BACKUP_DIR.mkdir(exist_ok=True)


def print_header() -> None:
    print("\n" + "=" * 44)
    print("🚀 STACKIQ AUTONOMOUS COACH v3")
    print("=" * 44)
    print(f"📁 Project: {ROOT}")
    print(f"🕒 Time:    {now_str()}")
    print("=" * 44 + "\n")


def run_cmd(cmd, timeout=10):
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=ROOT,
        )
        return {
            "ok": result.returncode == 0,
            "stdout": (result.stdout or "").strip(),
            "stderr": (result.stderr or "").strip(),
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "stdout": "",
            "stderr": f"Command timed out after {timeout}s",
            "returncode": -1,
        }
    except Exception as e:
        return {
            "ok": False,
            "stdout": "",
            "stderr": str(e),
            "returncode": -1,
        }


def curl_json(url, timeout=12):
    result = run_cmd(["curl", "-sS", url], timeout=timeout)
    raw = result["stdout"]

    if not raw:
        return {
            "ok": False,
            "raw": raw,
            "json": None,
            "error": result["stderr"] or "No response body",
        }

    try:
        return {
            "ok": True,
            "raw": raw,
            "json": json.loads(raw),
            "error": None,
        }
    except Exception as e:
        return {
            "ok": False,
            "raw": raw,
            "json": None,
            "error": f"Invalid JSON: {e}",
        }


def load_state():
    if not STATE_FILE.exists():
        return {
            "runs": 0,
            "history": [],
            "last_score": 0,
            "last_missing": [],
            "last_issues": [],
        }
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {
            "runs": 0,
            "history": [],
            "last_score": 0,
            "last_missing": [],
            "last_issues": [],
        }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def backup_file(path: Path):
    if not path.exists():
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"{path.name}.{ts}.bak"
    shutil.copy2(path, backup_path)
    return backup_path


def file_syntax_check(path: Path):
    if not path.exists():
        return {"ok": False, "error": "File does not exist"}

    result = run_cmd(["python", "-m", "py_compile", str(path)], timeout=10)
    return {
        "ok": result["ok"],
        "error": result["stderr"] if not result["ok"] else "",
    }


def scan_target_files():
    summary = []
    for rel in TARGET_FILES:
        p = ROOT / rel
        if p.exists():
            summary.append(
                {
                    "file": rel,
                    "exists": True,
                    "size": p.stat().st_size,
                    "modified": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        else:
            summary.append(
                {
                    "file": rel,
                    "exists": False,
                    "size": 0,
                    "modified": None,
                }
            )
    return summary


def read_relevant_code(max_chars=50000):
    chunks = []
    total = 0

    for rel in TARGET_FILES:
        p = ROOT / rel
        if not p.exists():
            continue

        try:
            content = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        header = f"\n\n# ===== FILE: {rel} =====\n"
        block = header + content
        if total + len(block) > max_chars:
            remaining = max_chars - total
            if remaining > 200:
                block = block[:remaining]
                chunks.append(block)
            break

        chunks.append(block)
        total += len(block)

    return "".join(chunks)


def find_field(data, canonical_name):
    aliases = OPTIONAL_ALIAS_GROUPS.get(canonical_name, [canonical_name])
    for alias in aliases:
        if alias in data:
            return alias
    return None


def analyze_best_pick_payload(data):
    present = []
    missing = []

    if not isinstance(data, dict):
        return {
            "present": [],
            "missing": REQUIRED_FIELDS.copy(),
            "issues": ["Response is not a JSON object"],
            "score": 0,
        }

    for field in REQUIRED_FIELDS:
        found_alias = find_field(data, field)
        if found_alias is not None:
            present.append(field)
        else:
            missing.append(field)

    issues = []

    for error_key in ["error", "reason", "detail", "message"]:
        if error_key in data and data[error_key]:
            issues.append(f"{error_key}: {data[error_key]}")

    if data.get("low_conviction") is True:
        issues.append("low_conviction=true")

    if data.get("request_timeout") is True:
        issues.append("request_timeout=true")

    raw_score = len(present) / len(REQUIRED_FIELDS)
    score = int(raw_score * 100)

    return {
        "present": present,
        "missing": missing,
        "issues": issues,
        "score": score,
    }


def analyze_health():
    response = curl_json(HEALTH_URL, timeout=8)

    if response["ok"] and response["json"] is not None:
        return {
            "alive": True,
            "raw": response["raw"],
            "json": response["json"],
            "error": None,
        }

    raw_fallback = run_cmd(["curl", "-sS", HEALTH_URL], timeout=8)
    if raw_fallback["stdout"]:
        return {
            "alive": True,
            "raw": raw_fallback["stdout"],
            "json": None,
            "error": None,
        }

    return {
        "alive": False,
        "raw": response["raw"],
        "json": None,
        "error": response["error"] or raw_fallback["stderr"] or "Health endpoint unreachable",
    }


def analyze_best_pick():
    response = curl_json(BEST_PICK_URL, timeout=20)

    if not response["ok"] or response["json"] is None:
        return {
            "alive": False,
            "raw": response["raw"],
            "json": response["json"],
            "analysis": {
                "present": [],
                "missing": REQUIRED_FIELDS.copy(),
                "issues": [response["error"] or "Invalid response from /best_pick_v2"],
                "score": 0,
            },
        }

    analysis = analyze_best_pick_payload(response["json"])
    return {
        "alive": True,
        "raw": response["raw"],
        "json": response["json"],
        "analysis": analysis,
    }


def print_health_report(health):
    print("🩺 HEALTH CHECK")
    print("-" * 44)
    if health["alive"]:
        print("✅ Backend reachable")
        if health["json"] is not None:
            print(f"📦 Health JSON: {json.dumps(health['json'], indent=2)}")
        else:
            print(f"📦 Health raw: {health['raw'][:300]}")
    else:
        print("❌ Backend NOT reachable")
        print(f"⚠️  Reason: {health['error']}")
        print("👉 Run: uvicorn app:app --reload")
    print("")


def print_best_pick_report(best_pick):
    print("📊 /best_pick_v2 ANALYSIS")
    print("-" * 44)

    if not best_pick["alive"]:
        print("❌ Endpoint failed")
        for issue in best_pick["analysis"]["issues"]:
            print(f"⚠️  {issue}")
        if best_pick["raw"]:
            print("\n--- RAW OUTPUT ---")
            print(best_pick["raw"][:800])
        print("")
        return

    analysis = best_pick["analysis"]

    print(f"✅ Present fields: {analysis['present']}")
    print(f"❌ Missing fields: {analysis['missing']}")
    print(f"📈 Completion score: {analysis['score']}%")

    if analysis["issues"]:
        print("\n⚠️  Issues detected:")
        for issue in analysis["issues"]:
            print(f"- {issue}")

    print("\n📦 Parsed payload preview:")
    try:
        print(json.dumps(best_pick["json"], indent=2)[:1400])
    except Exception:
        print(str(best_pick["json"])[:1400])

    print("")


def next_target(best_pick):
    analysis = best_pick["analysis"]
    if not analysis["missing"]:
        return None
    return analysis["missing"][0]


def print_next_action(health, best_pick):
    print("🎯 NEXT ACTION")
    print("-" * 44)

    if not health["alive"]:
        print("1. Start backend")
        print("2. Re-run coach.py")
        print("")
        return

    if not best_pick["alive"]:
        print("1. Fix /best_pick_v2 so it returns valid JSON")
        print("2. Re-run coach.py")
        print("")
        return

    target = next_target(best_pick)
    if target is None:
        print("🔥 All required core fields are present.")
        print("✅ Core endpoint structure is complete.")
        print("")
        return

    print(f"👉 Implement next field: {target}")
    print(f"📝 Meaning: {FIELD_DESCRIPTIONS[target]}")
    print("✅ Keep existing working logic. Only add what is missing.")
    print("")


def generate_patch_suggestion(best_pick):
    analysis = best_pick["analysis"]
    if not best_pick["alive"]:
        return (
            "Suggested direction:\n"
            "- Make sure /best_pick_v2 always returns valid JSON.\n"
            "- Catch timeouts/exceptions and still return a structured dict.\n"
            "- Never return plain text, None, or malformed output."
        )

    missing = analysis["missing"]
    issues = analysis["issues"]

    suggestions = []

    if "symbol" in missing:
        suggestions.append(
            "- Ensure the final return dict always includes 'symbol'. "
            "If your internal key is 'ticker', map it to 'symbol' before returning."
        )
    if "entry" in missing:
        suggestions.append(
            "- Add 'entry' to the final response. "
            "If you already have 'entry_price', either duplicate it into 'entry' or rename on output."
        )
    if "stop" in missing:
        suggestions.append(
            "- Add 'stop' to the final response. "
            "If you already have 'risk_stop' or 'stop_loss', map it to 'stop'."
        )
    if "take_profit" in missing:
        suggestions.append(
            "- Add 'take_profit' to the final response. "
            "If you currently return 'target' or 'targets', expose one normalized key named 'take_profit'."
        )
    if "confidence" in missing:
        suggestions.append(
            "- Add 'confidence' to the final response. "
            "If you currently use 'confidence_0_10', keep it and also expose a normalized 'confidence'."
        )

    if any("timeout" in issue.lower() for issue in issues):
        suggestions.append(
            "- You have a timeout issue. Add a timeout-safe fallback path that still returns the same response schema."
        )

    if not suggestions:
        suggestions.append("- No schema patch needed right now.")

    return "Suggested direction:\n" + "\n".join(suggestions)


def create_run_log(health, best_pick, state):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = RUNS_DIR / f"run_{ts}.json"

    payload = {
        "timestamp": now_str(),
        "health": health,
        "best_pick_alive": best_pick["alive"],
        "best_pick_analysis": best_pick["analysis"],
        "best_pick_json": best_pick["json"],
        "state_before_save": state,
        "target_files": scan_target_files(),
        "patch_suggestion": generate_patch_suggestion(best_pick),
    }

    log_path.write_text(json.dumps(payload, indent=2, default=str))
    return log_path


def print_file_summary():
    print("📂 TARGET FILE SUMMARY")
    print("-" * 44)
    for item in scan_target_files():
        exists_text = "✅" if item["exists"] else "❌"
        print(
            f"{exists_text} {item['file']}"
            + (f" | {item['size']} bytes | {item['modified']}" if item["exists"] else "")
        )
    print("")


def print_patch_hint(best_pick):
    print("🛠️ PATCH GUIDANCE")
    print("-" * 44)
    print(generate_patch_suggestion(best_pick))
    print("")


def print_syntax_summary():
    print("🧪 PYTHON SYNTAX CHECK")
    print("-" * 44)
    checked_any = False
    for rel in TARGET_FILES:
        p = ROOT / rel
        if not p.exists() or p.suffix != ".py":
            continue
        checked_any = True
        result = file_syntax_check(p)
        if result["ok"]:
            print(f"✅ {rel}")
        else:
            print(f"❌ {rel}")
            print(f"   {result['error'][:300]}")
    if not checked_any:
        print("⚠️  No Python target files found")
    print("")


def print_code_context_summary():
    print("🧠 CODE CONTEXT SNAPSHOT")
    print("-" * 44)
    code = read_relevant_code(max_chars=3500)
    if not code:
        print("⚠️  No target code found")
    else:
        lines = code.splitlines()
        preview = "\n".join(lines[:60])
        print(preview[:3500])
    print("")


def update_state(state, health, best_pick):
    state["runs"] = state.get("runs", 0) + 1
    state["last_score"] = best_pick["analysis"]["score"]
    state["last_missing"] = best_pick["analysis"]["missing"]
    state["last_issues"] = best_pick["analysis"]["issues"]
    state["last_seen"] = now_str()

    history = state.get("history", [])
    history.append(
        {
            "time": now_str(),
            "health_alive": health["alive"],
            "score": best_pick["analysis"]["score"],
            "missing": best_pick["analysis"]["missing"],
            "issues": best_pick["analysis"]["issues"],
        }
    )
    state["history"] = history[-25:]


def show_progress_delta(state):
    print("📈 PROGRESS MEMORY")
    print("-" * 44)
    print(f"Runs: {state.get('runs', 0)}")
    print(f"Last score: {state.get('last_score', 0)}%")
    print(f"Last missing: {state.get('last_missing', [])}")
    print(f"Last issues: {state.get('last_issues', [])}")
    print("")


def main():
    ensure_dirs()
    state = load_state()

    print_header()
    show_progress_delta(state)
    print_file_summary()
    print_syntax_summary()

    health = analyze_health()
    print_health_report(health)

    if not health["alive"]:
        update_state(
            state,
            health,
            {
                "alive": False,
                "json": None,
                "analysis": {
                    "present": [],
                    "missing": REQUIRED_FIELDS.copy(),
                    "issues": ["Backend not running"],
                    "score": 0,
                },
            },
        )
        log_path = create_run_log(
            health,
            {
                "alive": False,
                "json": None,
                "analysis": {
                    "present": [],
                    "missing": REQUIRED_FIELDS.copy(),
                    "issues": ["Backend not running"],
                    "score": 0,
                },
            },
            state,
        )
        save_state(state)
        print(f"📝 Run log saved: {log_path}")
        print_next_action(
            health,
            {
                "alive": False,
                "json": None,
                "analysis": {
                    "present": [],
                    "missing": REQUIRED_FIELDS.copy(),
                    "issues": ["Backend not running"],
                    "score": 0,
                },
            },
        )
        return

    best_pick = analyze_best_pick()
    print_best_pick_report(best_pick)
    print_patch_hint(best_pick)
    print_code_context_summary()

    update_state(state, health, best_pick)
    log_path = create_run_log(health, best_pick, state)
    save_state(state)

    print(f"📝 Run log saved: {log_path}\n")
    print_next_action(health, best_pick)


if __name__ == "__main__":
    main()
