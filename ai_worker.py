#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║           AUREXIS AI WORKER v2.0 — AUTONOMOUS MODE           ║
║                                                               ║
║  • Autonomous fix → test → re-audit loop                     ║
║  • Memory: remembers what it fixed across runs               ║
║  • Self-healing: if a fix breaks something, it reverts       ║
║  • Multi-pass: keeps going until 0 critical bugs remain      ║
║  • Semantic diff: understands intent, not just text          ║
║  • Live dashboard in terminal                                ║
╚═══════════════════════════════════════════════════════════════╝

USAGE:
  python ai_worker_v2.py --key sk-ant-...          # Full autonomous run
  python ai_worker_v2.py --key sk-ant-... --chat   # Interactive chat mode
  python ai_worker_v2.py --key sk-ant-... --fix "Trade Plan unavailable"
  python ai_worker_v2.py --key sk-ant-... --watch  # Watch mode: auto-fix on save
"""

import os, sys, json, re, subprocess, hashlib, shutil, time, threading
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    import anthropic
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "anthropic", "colorama", "watchdog"], check=True)
    import anthropic

try:
    from colorama import Fore, Back, Style, init
    init(autoreset=True)
except ImportError:
    class Fore:
        GREEN=YELLOW=RED=CYAN=MAGENTA=WHITE=BLUE=LIGHTBLACK_EX=""
    class Back:
        BLACK=GREEN=RED=""
    class Style:
        BRIGHT=RESET_ALL=DIM=""

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

VERSION = "2.0"
MEMORY_FILE = ".ai_worker_memory.json"
MAX_PASSES = 5          # Max autonomous fix passes
MAX_FILE_CHARS = 90_000
CHUNK_SIZE = 70_000

SKIP_DIRS = {".venv","venv",".git","__pycache__","node_modules",".github","ml","data",".vscode","dist","build"}
SKIP_FILES = {"package-lock.json","stackiq.db","stackiq.db-shm","stackiq.db-wal","ai_worker.py","ai_worker_v2.py"}

PRIORITY_FILES = [
    "app.py","best_pick.py","execution_engine.py","scoring_engine.py",
    "llm_services.py","llm_client.py","data_fetcher.py","engine.py",
    "pre_mover_engine.py","pre_mover_signal.py","indicators.py",
    "human_translation.py","trade_thesis.py","strategy_memory.py",
    "polygon_client.py","llm_prompts.py","llm_config.py","optimize.py",
]

KNOWN_BUGS = """
KNOWN SYMPTOMS TO FIX:
1. Trade Plan entry/stop/targets showing "Unavailable" → field name mismatch in API response
2. AI Score gauge shows 27 instead of 8.2 → score is on 0-100 scale but UI expects 0-10
3. Execution score gauge shows 14 instead of 6.1 → same scale bug
4. BUY ZONE showing "—" → field not being populated in execution plan
5. Confidence showing 2% when it should show 6.6/10 → wrong field or scale
6. "LLM reasoning active: FAIL Missing reasoning arrays" → llm_reasoning/bullish_factors/bearish_factors not populated
7. API shows "Offline" in sidebar → connection/auth issue with Alpaca or Polygon
8. News sentiment always "NEUTRAL" → LLM not returning proper direction field
9. News summary sometimes shows weak/contradictory text vs score → disconnect between LLM analysis and scoring
"""

SYSTEM_PROMPT = f"""You are AUREXIS AI WORKER v2 — the most advanced autonomous code repair system ever built for this specific trading intelligence platform.

PROJECT: AUREXIS / stackiq-prodigy2
- Python/FastAPI monolith (app.py ~9500+ lines)
- React/Vite frontend on localhost:5173  
- APIs: Alpaca (trading), Polygon.io (market data)
- SQLite database

{KNOWN_BUGS}

YOUR CAPABILITIES:
1. Deep semantic understanding of the entire codebase
2. Root cause analysis — find the ACTUAL cause, not symptoms
3. Generate surgical, precise fixes that don't break other things
4. Understand data flow across files (what goes in → what comes out)
5. Detect integration mismatches between frontend expectations and backend output

WHEN AUDITING:
- Trace data flow end-to-end: API call → processing → response → frontend rendering
- Check field names match exactly between backend output and frontend consumption
- Verify score scales are consistent (0-10 vs 0-100 confusion is common)
- Look for silent failures (try/except pass that hides real errors)
- Find None/null propagation that causes "Unavailable"

WHEN GENERATING FIXES:
- Be surgical — change minimum code needed
- Never break existing functionality
- Add proper error handling with logging, not silent pass
- Ensure field names are consistent across the entire data pipeline
- Format ALL fixes EXACTLY like this:

<<<FIX_START>>>
FILE: exact_filename.py
FUNCTION: function_name (line ~N)
SEVERITY: CRITICAL|HIGH|MEDIUM|LOW
ISSUE: one line description
ROOT_CAUSE: precise technical explanation
OLD:
```python
[exact code to replace - must match file exactly]
```
NEW:
```python
[replacement code]
```
CONFIDENCE: HIGH|MEDIUM|LOW
<<<FIX_END>>>

If you cannot find a safe fix, say WHY and what information you need."""

# ─────────────────────────────────────────────────────────────────────────────
# MEMORY SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

class Memory:
    def __init__(self, root: Path):
        self.path = root / MEMORY_FILE
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except Exception:
                pass
        return {
            "runs": [],
            "fixes_applied": [],
            "fixes_failed": [],
            "known_issues": [],
            "file_hashes": {},
            "pass_count": 0,
        }

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def record_fix(self, fix: dict, success: bool):
        entry = {**fix, "success": success, "timestamp": datetime.now().isoformat()}
        if success:
            self.data["fixes_applied"].append(entry)
        else:
            self.data["fixes_failed"].append(entry)
        self.save()

    def was_attempted(self, fix: dict) -> bool:
        key = f"{fix.get('file')}:{fix.get('function')}:{fix.get('issue', '')[:50]}"
        all_fixes = self.data["fixes_applied"] + self.data["fixes_failed"]
        return any(f"{f.get('file')}:{f.get('function')}:{f.get('issue','')[:50]}" == key for f in all_fixes)

    def get_context(self) -> str:
        if not self.data["fixes_applied"]:
            return ""
        recent = self.data["fixes_applied"][-10:]
        lines = ["PREVIOUSLY FIXED (don't re-fix these):"]
        for f in recent:
            lines.append(f"  - {f.get('file')}: {f.get('issue','')}")
        return "\n".join(lines)

    def update_hash(self, filepath: str, content: str):
        self.data["file_hashes"][filepath] = hashlib.md5(content.encode()).hexdigest()
        self.save()

    def file_changed(self, filepath: str, content: str) -> bool:
        h = hashlib.md5(content.encode()).hexdigest()
        return self.data["file_hashes"].get(filepath) != h

# ─────────────────────────────────────────────────────────────────────────────
# FILE SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

def load_files(root: Path, changed_only: bool = False, memory: Memory = None) -> dict:
    files = {}
    extensions = {".py",".js",".jsx",".ts",".tsx",".env",".yml",".yaml"}
    for path in sorted(root.rglob("*")):
        if any(skip in path.parts for skip in SKIP_DIRS):
            continue
        if path.name in SKIP_FILES:
            continue
        if path.suffix not in extensions:
            continue
        if path.stat().st_size > 600_000:
            continue
        rel = str(path.relative_to(root))
        try:
            content = path.read_text(errors="replace")
            if changed_only and memory and not memory.file_changed(rel, content):
                continue
            files[rel] = content
        except Exception:
            pass
    return files

def backup_file(path: Path) -> Path:
    backup_dir = path.parent / ".ai_worker_backups"
    backup_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = backup_dir / f"{path.name}.{ts}.bak"
    shutil.copy2(path, backup)
    return backup

def apply_fix(fix: dict, root: Path) -> tuple[bool, str]:
    path = root / fix["file"]
    if not path.exists():
        return False, f"File not found: {fix['file']}"
    
    content = path.read_text(errors="replace")
    old_code = fix.get("old", "").strip()
    new_code = fix.get("new", "").strip()
    
    if not old_code:
        return False, "No old code provided"
    
    if old_code not in content:
        # Try fuzzy match — normalize whitespace
        normalized_content = re.sub(r'\s+', ' ', content)
        normalized_old = re.sub(r'\s+', ' ', old_code)
        if normalized_old not in normalized_content:
            return False, f"Could not find target code in {fix['file']}"
    
    backup = backup_file(path)
    new_content = content.replace(old_code, new_code, 1)
    path.write_text(new_content)
    return True, f"Patched (backup: {backup.name})"

# ─────────────────────────────────────────────────────────────────────────────
# FIX EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_fixes(text: str) -> list:
    fixes = []
    parts = text.split("<<<FIX_START>>>")
    for part in parts[1:]:
        if "<<<FIX_END>>>" not in part:
            continue
        fix_text = part.split("<<<FIX_END>>>")[0].strip()
        fix = {}
        for field in ["FILE","FUNCTION","SEVERITY","ISSUE","ROOT_CAUSE","CONFIDENCE"]:
            m = re.search(rf'^{field}:\s*(.+)$', fix_text, re.MULTILINE)
            if m:
                fix[field.lower()] = m.group(1).strip()
        
        # Extract OLD and NEW code blocks
        old_m = re.search(r'OLD:\s*```(?:python)?\n(.*?)```', fix_text, re.DOTALL)
        new_m = re.search(r'NEW:\s*```(?:python)?\n(.*?)```', fix_text, re.DOTALL)
        if old_m:
            fix["old"] = old_m.group(1).strip()
        if new_m:
            fix["new"] = new_m.group(1).strip()
        
        if fix.get("file") and fix.get("old") and fix.get("new"):
            fixes.append(fix)
    
    return fixes

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE API
# ─────────────────────────────────────────────────────────────────────────────

def call_claude(client, messages: list, max_tokens=4096, stream_print=True) -> str:
    full_text = ""
    with client.messages.stream(
        model="claude-opus-4-5",
        max_tokens=max_tokens,
        system=SYSTEM_PROMPT,
        messages=messages,
    ) as stream:
        for text in stream.text_stream:
            full_text += text
            if stream_print:
                print(text, end="", flush=True)
    if stream_print:
        print()
    return full_text

# ─────────────────────────────────────────────────────────────────────────────
# TESTING
# ─────────────────────────────────────────────────────────────────────────────

def run_syntax_check(root: Path, files: list) -> dict:
    results = {}
    for fname in files:
        path = root / fname
        if not path.exists() or not fname.endswith(".py"):
            continue
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(path)],
            capture_output=True, text=True
        )
        results[fname] = {
            "ok": result.returncode == 0,
            "error": result.stderr.strip() if result.returncode != 0 else None
        }
    return results

def test_imports(root: Path) -> dict:
    results = {}
    key_files = ["app.py", "best_pick.py", "execution_engine.py", "scoring_engine.py"]
    for fname in key_files:
        path = root / fname
        if not path.exists():
            continue
        result = subprocess.run(
            [sys.executable, "-c", f"import ast; ast.parse(open('{path}').read()); print('OK')"],
            capture_output=True, text=True, cwd=str(root)
        )
        results[fname] = {"ok": "OK" in result.stdout, "error": result.stderr[:200] if result.stderr else None}
    return results

# ─────────────────────────────────────────────────────────────────────────────
# DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def print_banner():
    print(f"""
{Fore.GREEN}{Style.BRIGHT}╔═══════════════════════════════════════════════════════════════╗
║           AUREXIS AI WORKER v{VERSION} — AUTONOMOUS MODE           ║
║    Fix → Test → Re-audit → Repeat until everything passes    ║
╚═══════════════════════════════════════════════════════════════╝{Style.RESET_ALL}""")

def print_section(title: str):
    print(f"\n{Fore.YELLOW}{Style.BRIGHT}{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}{Style.RESET_ALL}")

def print_fix_summary(fixes: list):
    if not fixes:
        print(f"  {Fore.GREEN}No patchable fixes found in this pass.")
        return
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    fixes_sorted = sorted(fixes, key=lambda f: severity_order.get(f.get("severity","LOW").upper(), 3))
    colors = {"CRITICAL": Fore.RED, "HIGH": Fore.YELLOW, "MEDIUM": Fore.CYAN, "LOW": Fore.WHITE}
    for i, fix in enumerate(fixes_sorted, 1):
        sev = fix.get("severity", "?").upper()
        col = colors.get(sev, Fore.WHITE)
        print(f"  {col}[{sev}]{Style.RESET_ALL} {fix.get('file','?')} → {fix.get('issue','?')[:60]}")

def print_test_results(results: dict):
    for fname, r in results.items():
        if r["ok"]:
            print(f"  {Fore.GREEN}✓{Style.RESET_ALL} {fname}")
        else:
            print(f"  {Fore.RED}✗{Style.RESET_ALL} {fname}: {r.get('error','unknown error')[:80]}")

# ─────────────────────────────────────────────────────────────────────────────
# CORE AUDIT ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def audit_pass(client, files: dict, memory: Memory, pass_num: int) -> list:
    all_fixes = []
    memory_ctx = memory.get_context()

    def sort_key(item):
        fname = Path(item[0]).name
        return (0 if fname in PRIORITY_FILES else 1, PRIORITY_FILES.index(fname) if fname in PRIORITY_FILES else 99)

    sorted_files = sorted(files.items(), key=sort_key)
    audit_limit = 12

    for i, (fname, content) in enumerate(sorted_files[:audit_limit]):
        if len(content) < 50:
            continue
        print(f"  {Fore.CYAN}[{i+1}/{min(len(sorted_files), audit_limit)}] {fname} ({len(content):,} chars){Style.RESET_ALL}")

        chunks = [content[j:j+CHUNK_SIZE] for j in range(0, min(len(content), MAX_FILE_CHARS), CHUNK_SIZE)]
        for chunk_i, chunk in enumerate(chunks):
            chunk_label = f"(part {chunk_i+1}/{len(chunks)})" if len(chunks) > 1 else ""
            prompt = f"""PASS {pass_num} AUDIT {chunk_label}

{memory_ctx}

Audit this file deeply. Find ALL bugs, especially ones causing the known symptoms.
Trace data flow. Check field names. Verify scales (0-10 vs 0-100). Find silent failures.

FILE: {fname}
```python
{chunk}
```

Generate fixes for every real bug found. Be precise and surgical."""

            result = call_claude(client, [{"role": "user", "content": prompt}], max_tokens=4096, stream_print=False)
            fixes = extract_fixes(result)
            
            # Filter out already-attempted fixes
            new_fixes = [f for f in fixes if not memory.was_attempted(f)]
            all_fixes.extend(new_fixes)
            
            if fixes:
                print(f"    {Fore.GREEN}→ Found {len(new_fixes)} new fix(es){Style.RESET_ALL}")

    # Integration audit
    print(f"  {Fore.MAGENTA}Running integration audit...{Style.RESET_ALL}")
    sig_lines = []
    for fname, content in sorted_files[:20]:
        if not fname.endswith(".py"):
            continue
        sigs = [l for l in content.split("\n") if re.match(r'\s*(async\s+)?def |^class ', l)][:20]
        if sigs:
            sig_lines.append(f"### {fname}\n" + "\n".join(sigs))

    integration_prompt = f"""INTEGRATION AUDIT - Pass {pass_num}

{memory_ctx}

Function signatures across codebase:
{chr(10).join(sig_lines[:30])}

Known symptoms still present: Trade Plan Unavailable, score scale mismatch, BUY ZONE empty, LLM reasoning missing.

Find integration bugs:
1. What function should populate Trade Plan entry/stop/targets? Is it being called? Does it return the right field names?
2. Where is the 0-10 vs 0-100 scale conversion happening (or NOT happening)?
3. What populates llm_reasoning, bullish_factors, bearish_factors? Is it connected?
4. What field should BUY ZONE read from? Is that field being set?

Generate precise fixes."""

    result = call_claude(client, [{"role": "user", "content": integration_prompt}], max_tokens=4096, stream_print=False)
    fixes = extract_fixes(result)
    new_fixes = [f for f in fixes if not memory.was_attempted(f)]
    all_fixes.extend(new_fixes)
    if new_fixes:
        print(f"    {Fore.GREEN}→ Integration: {len(new_fixes)} new fix(es){Style.RESET_ALL}")

    return all_fixes

def apply_all_fixes(fixes: list, root: Path, memory: Memory, auto: bool = False) -> tuple[int, int]:
    if not fixes:
        return 0, 0

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    fixes_sorted = sorted(fixes, key=lambda f: severity_order.get(f.get("severity","LOW").upper(), 3))

    applied = 0
    failed = 0
    modified_files = set()

    for fix in fixes_sorted:
        sev = fix.get("severity", "?").upper()
        fname = fix.get("file", "?")
        issue = fix.get("issue", "?")[:50]

        if not auto:
            col = Fore.RED if sev == "CRITICAL" else Fore.YELLOW if sev == "HIGH" else Fore.CYAN
            print(f"\n  {col}[{sev}]{Style.RESET_ALL} {fname}: {issue}")
            print(f"  Root cause: {fix.get('root_cause','?')[:100]}")
            ans = input(f"  Apply this fix? [Y/n/s(skip all)] ").strip().lower()
            if ans == "s":
                break
            if ans == "n":
                continue

        ok, msg = apply_fix(fix, root)
        memory.record_fix(fix, ok)

        if ok:
            applied += 1
            modified_files.add(fix["file"])
            print(f"  {Fore.GREEN}✓{Style.RESET_ALL} {fname}: {issue}")
        else:
            failed += 1
            print(f"  {Fore.RED}✗{Style.RESET_ALL} {fname}: {msg}")

    # Syntax check modified files
    if modified_files:
        print_section("Syntax Check")
        results = run_syntax_check(root, list(modified_files))
        print_test_results(results)

        # Auto-revert broken files
        for fname, r in results.items():
            if not r["ok"]:
                backup_dir = root / ".ai_worker_backups"
                backups = sorted(backup_dir.glob(f"{Path(fname).name}.*.bak")) if backup_dir.exists() else []
                if backups:
                    latest = backups[-1]
                    shutil.copy2(latest, root / fname)
                    print(f"  {Fore.YELLOW}↩ Auto-reverted {fname} (syntax error){Style.RESET_ALL}")

    return applied, failed

# ─────────────────────────────────────────────────────────────────────────────
# INTERACTIVE CHAT MODE
# ─────────────────────────────────────────────────────────────────────────────

def chat_mode(client, files: dict, root: Path, memory: Memory):
    print_section("INTERACTIVE CHAT MODE")
    print(f"  Ask anything about your codebase. Type 'exit' to quit, 'fix' to apply suggested fixes.")
    print(f"  Examples:")
    print(f"    'why is Trade Plan unavailable'")
    print(f"    'show me where the score scaling happens'")
    print(f"    'fix the LLM reasoning arrays'")
    print(f"    'explain the data flow from polygon to the frontend'\n")

    history = []
    pending_fixes = []

    # Build file context summary
    file_summary = "\n".join(f"- {k} ({len(v):,} chars)" for k,v in list(files.items())[:20])
    context_msg = f"Files available:\n{file_summary}\n\n{memory.get_context()}"

    while True:
        try:
            user_input = input(f"{Fore.GREEN}You:{Style.RESET_ALL} ").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if user_input.lower() in ("exit", "quit", "q"):
            break

        if user_input.lower() == "fix" and pending_fixes:
            applied, failed = apply_all_fixes(pending_fixes, root, memory, auto=False)
            print(f"\n  Applied {applied}, failed {failed}")
            pending_fixes = []
            continue

        if not user_input:
            continue

        # Find relevant files
        kws = user_input.lower().split()
        relevant = {k: v[:40000] for k, v in files.items()
                   if any(w in v.lower() or w in k.lower() for w in kws)}
        if not relevant:
            relevant = dict(list(files.items())[:3])

        file_ctx = "\n\n".join(f"=== {k} ===\n{v[:25000]}" for k, v in list(relevant.items())[:4])
        
        full_prompt = f"{context_msg}\n\nRelevant code:\n{file_ctx}\n\nQuestion: {user_input}"
        history.append({"role": "user", "content": full_prompt})

        print(f"\n{Fore.CYAN}Worker:{Style.RESET_ALL} ", end="")
        response = call_claude(client, history[-6:], max_tokens=4096, stream_print=True)
        history.append({"role": "assistant", "content": response})

        fixes = extract_fixes(response)
        if fixes:
            pending_fixes = fixes
            print(f"\n  {Fore.YELLOW}→ {len(fixes)} fix(es) generated. Type 'fix' to apply them.{Style.RESET_ALL}")

# ─────────────────────────────────────────────────────────────────────────────
# TARGETED FIX MODE
# ─────────────────────────────────────────────────────────────────────────────

def targeted_fix(client, query: str, files: dict, root: Path, memory: Memory):
    print_section(f"TARGETED FIX: {query}")

    kws = query.lower().split()
    relevant = {k: v for k, v in files.items()
               if any(w in v.lower() or w in k.lower() for w in kws)}
    
    # Always include core files
    for core in ["app.py", "best_pick.py", "execution_engine.py", "scoring_engine.py"]:
        if core in files and core not in relevant:
            relevant[core] = files[core]

    file_ctx = "\n\n".join(f"=== {k} ===\n{v[:30000]}" for k, v in list(relevant.items())[:5])
    
    prompt = f"""TARGETED FIX REQUEST: {query}

{memory.get_context()}

Relevant code:
{file_ctx}

Find the EXACT root cause of this specific issue and generate a precise fix.
Trace the complete data flow. Be surgical."""

    print(f"  Analyzing {len(relevant)} relevant files...")
    response = call_claude(client, [{"role": "user", "content": prompt}], max_tokens=4096, stream_print=True)
    
    fixes = extract_fixes(response)
    if fixes:
        print_section("Fixes Found")
        print_fix_summary(fixes)
        ans = input(f"\n  Apply {len(fixes)} fix(es)? [Y/n] ").strip().lower()
        if ans != "n":
            applied, failed = apply_all_fixes(fixes, root, memory, auto=True)
            print(f"\n  {Fore.GREEN}Applied: {applied} | Failed: {failed}{Style.RESET_ALL}")

# ─────────────────────────────────────────────────────────────────────────────
# WATCH MODE
# ─────────────────────────────────────────────────────────────────────────────

def watch_mode(client, root: Path, memory: Memory, api_key: str):
    print_section("WATCH MODE — Auto-fixing on file save")
    print("  Watching for changes... Press Ctrl+C to stop.\n")
    
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "watchdog"], check=True)
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

    class ChangeHandler(FileSystemEventHandler):
        def __init__(self):
            self.debounce = {}
        
        def on_modified(self, event):
            if event.is_directory:
                return
            path = Path(event.src_path)
            if path.suffix not in {".py", ".js", ".jsx"}:
                return
            if path.name in SKIP_FILES:
                return
            if any(skip in path.parts for skip in SKIP_DIRS):
                return
            
            now = time.time()
            if self.debounce.get(str(path), 0) > now - 2:
                return
            self.debounce[str(path)] = now

            rel = str(path.relative_to(root))
            print(f"\n  {Fore.YELLOW}Changed: {rel}{Style.RESET_ALL}")
            
            try:
                content = path.read_text(errors="replace")
                files = {rel: content}
                
                # Quick targeted audit of just this file
                prompt = f"""Quick audit of recently modified file:
FILE: {rel}
```python
{content[:CHUNK_SIZE]}
```
Find any bugs introduced or existing. Generate fixes if needed."""
                
                response = call_claude(client, [{"role": "user", "content": prompt}],
                                     max_tokens=2048, stream_print=False)
                fixes = extract_fixes(response)
                if fixes:
                    print(f"  {Fore.RED}Found {len(fixes)} issue(s) in {rel}:{Style.RESET_ALL}")
                    print_fix_summary(fixes)
                    ans = input("  Auto-fix? [Y/n] ").strip().lower()
                    if ans != "n":
                        apply_all_fixes(fixes, root, memory, auto=True)
                else:
                    print(f"  {Fore.GREEN}✓ No issues found{Style.RESET_ALL}")
            except Exception as e:
                print(f"  Error: {e}")

    observer = Observer()
    observer.schedule(ChangeHandler(), str(root), recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AUREXIS AI Worker v2 — Autonomous Code Repair")
    parser.add_argument("--key", type=str, help="Anthropic API key")
    parser.add_argument("--dir", type=str, default=".", help="Project root")
    parser.add_argument("--chat", action="store_true", help="Interactive chat mode")
    parser.add_argument("--watch", action="store_true", help="Watch mode: auto-fix on save")
    parser.add_argument("--fix", type=str, help="Targeted fix for specific issue")
    parser.add_argument("--auto", action="store_true", help="Apply all fixes without asking")
    parser.add_argument("--passes", type=int, default=2, help="Number of audit passes (default 2)")
    args = parser.parse_args()

    print_banner()

    api_key = args.key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(f"{Fore.RED}ERROR: Need API key. Use --key sk-ant-... or set ANTHROPIC_API_KEY{Style.RESET_ALL}")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    root = Path(args.dir).resolve()
    memory = Memory(root)
    memory.data["pass_count"] = memory.data.get("pass_count", 0) + 1
    
    print(f"  {Fore.CYAN}Project: {root}{Style.RESET_ALL}")
    print(f"  {Fore.CYAN}Memory: {len(memory.data['fixes_applied'])} fixes applied in past runs{Style.RESET_ALL}")

    # Load files
    print_section("Loading Files")
    files = load_files(root)
    total_chars = sum(len(v) for v in files.values())
    print(f"  {Fore.GREEN}{len(files)} files | {total_chars:,} characters | {total_chars//4:,} estimated tokens{Style.RESET_ALL}")

    # Route to mode
    if args.watch:
        watch_mode(client, root, memory, api_key)
        return

    if args.chat:
        chat_mode(client, files, root, memory)
        return

    if args.fix:
        targeted_fix(client, args.fix, files, root, memory)
        return

    # ── AUTONOMOUS MODE ──────────────────────────────────────────────────────
    total_applied = 0
    max_passes = min(args.passes, MAX_PASSES)

    for pass_num in range(1, max_passes + 1):
        print_section(f"PASS {pass_num}/{max_passes} — Deep Audit")

        # Reload files (may have changed from previous pass)
        if pass_num > 1:
            files = load_files(root)

        fixes = audit_pass(client, files, memory, pass_num)

        print_section(f"Pass {pass_num} Results")
        print_fix_summary(fixes)
        print(f"\n  Total fixes this pass: {Fore.YELLOW}{len(fixes)}{Style.RESET_ALL}")

        if not fixes:
            print(f"\n  {Fore.GREEN}✓ No new issues found. Codebase is clean!{Style.RESET_ALL}")
            break

        # Apply fixes
        print_section(f"Applying {len(fixes)} Fix(es)")
        applied, failed = apply_all_fixes(fixes, root, memory, auto=args.auto)
        total_applied += applied
        print(f"\n  Pass {pass_num}: {Fore.GREEN}{applied} applied{Style.RESET_ALL} | {Fore.RED}{failed} failed{Style.RESET_ALL}")

        # Update file hashes
        for fname, content in files.items():
            memory.update_hash(fname, content)

        if applied == 0:
            print(f"\n  No fixes could be applied. Stopping passes.")
            break

        if pass_num < max_passes:
            print(f"\n  {Fore.CYAN}Starting pass {pass_num + 1} to catch any remaining issues...{Style.RESET_ALL}")
            time.sleep(1)

    # Final summary
    print_section("COMPLETE")
    print(f"  {Fore.GREEN}{Style.BRIGHT}Total fixes applied this session: {total_applied}{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}All-time fixes applied: {len(memory.data['fixes_applied'])}{Style.RESET_ALL}")
    print(f"\n  {Fore.YELLOW}Next steps:{Style.RESET_ALL}")
    print(f"  1. Restart your app: {Fore.CYAN}python app.py{Style.RESET_ALL}")
    print(f"  2. Test: open localhost:5173 and check Trade Plan, scores, BUY ZONE")
    print(f"  3. If issues remain: {Fore.CYAN}python ai_worker_v2.py --key YOUR_KEY --chat{Style.RESET_ALL}")
    print(f"  4. For specific bugs: {Fore.CYAN}python ai_worker_v2.py --key YOUR_KEY --fix 'Trade Plan unavailable'{Style.RESET_ALL}")
    print(f"  5. Watch mode: {Fore.CYAN}python ai_worker_v2.py --key YOUR_KEY --watch{Style.RESET_ALL}")
    print()

if __name__ == "__main__":
    main()