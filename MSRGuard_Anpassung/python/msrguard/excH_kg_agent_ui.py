"""
Streamlit UI fuer den MSRGuard ExcH-KG Agent (Chat + JSON Side Panels).

Start (mit optionalen Script-Args):
  streamlit run python/msrguard/excH_kg_agent_ui.py -- --event_json_path <event.json> --out_json <result.json>

Alternativ: Event JSON im UI laden (Upload oder Pfad) und dann auf "Weiter" klicken.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import streamlit as st
except Exception as e:
    raise RuntimeError(
        "streamlit ist nicht installiert. Bitte in deiner venv installieren:\n"
        "  pip install streamlit\n"
        f"\nImport-Fehler: {e}"
    ) from e


DEFAULT_CONFIG_NAME = "excH_agent_config.json"

USER_AVATAR = "👤"
BOT_AVATAR = "🤖"
SYSTEM_AVATAR = "ℹ️"


@dataclass
class PipelineConfig:
    enabled: bool = True
    dir: str = r"D:\MA_Python_Agent\Pipelines\IngestionPipeline"
    runner: str = r"D:\MA_Python_Agent\Pipelines\IngestionPipeline\run_ingestion.py"
    config: str = r"D:\MA_Python_Agent\Pipelines\IngestionPipeline\config_ingestion.json"
    timeout_sec: Optional[int] = None


@dataclass
class ChatbotConfig:
    provider: str = "groq"
    model: str = "llama-3.3-70b-versatile"
    temperature: float = 0.0
    planner_provider: Optional[str] = None
    planner_model: Optional[str] = None


@dataclass
class UiConfig:
    openai_api_key_file: str = ""
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    chatbot: ChatbotConfig = field(default_factory=ChatbotConfig)


def ensure_python_root_on_sys_path() -> None:
    """
    Erwartete Struktur:
      .../python/msrguard/excH_kg_agent_ui.py
    -> sys.path soll .../python enthalten
    """
    try:
        here = Path(__file__).resolve()
        python_root = here.parent.parent  # .../python/
        if str(python_root) not in sys.path:
            sys.path.insert(0, str(python_root))
    except Exception:
        pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_ui_config() -> UiConfig:
    env_path = os.environ.get("EXCH_AGENT_CONFIG", "").strip()
    candidates: List[Path] = []
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(Path(__file__).with_name(DEFAULT_CONFIG_NAME))
    candidates.append(Path.cwd() / DEFAULT_CONFIG_NAME)

    cfg_path = None
    for p in candidates:
        try:
            if p.exists():
                cfg_path = p
                break
        except Exception:
            continue

    if not cfg_path:
        return UiConfig()

    raw = _load_json(cfg_path)

    p_raw = raw.get("pipeline") if isinstance(raw.get("pipeline"), dict) else {}
    pipeline = PipelineConfig(
        enabled=bool(p_raw.get("enabled", True)),
        dir=str(p_raw.get("dir", PipelineConfig.dir)),
        runner=str(p_raw.get("runner", PipelineConfig.runner)),
        config=str(p_raw.get("config", PipelineConfig.config)),
        timeout_sec=p_raw.get("timeout_sec", None),
    )
    if pipeline.timeout_sec is not None:
        try:
            pipeline.timeout_sec = int(pipeline.timeout_sec)
        except Exception:
            pipeline.timeout_sec = None

    c_raw = raw.get("chatbot") if isinstance(raw.get("chatbot"), dict) else {}
    chatbot = ChatbotConfig(
        provider=str(c_raw.get("provider", ChatbotConfig.provider)),
        model=str(c_raw.get("model", ChatbotConfig.model)),
        temperature=float(c_raw.get("temperature", ChatbotConfig.temperature)),
        planner_provider=c_raw.get("planner_provider") or None,
        planner_model=c_raw.get("planner_model") or None,
    )

    return UiConfig(
        openai_api_key_file=str(raw.get("openai_api_key_file", "")),
        pipeline=pipeline,
        chatbot=chatbot,
    )


def try_set_openai_key_from_file(path_str: str, provider: str = "openai") -> Optional[str]:
    _PROVIDER_ENV = {
        "openai": "OPENAI_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
        "azure_openai": "AZURE_OPENAI_API_KEY",
        "google": "GOOGLE_API_KEY",
        "groq": "GROQ_API_KEY",
    }
    _provider = (provider or "openai").lower().strip()
    if _provider == "ollama":
        return None  # kein API-Key nötig

    env_var = _PROVIDER_ENV.get(_provider, "OPENAI_API_KEY")
    if os.environ.get(env_var):
        return None
    if not path_str:
        return f"{env_var} fehlt und api_key_file ist leer."

    p = Path(path_str).expanduser()
    if not p.exists():
        return f"api_key_file existiert nicht: {p}"

    key = p.read_text(encoding="utf-8").strip()
    if not key:
        return f"api_key_file ist leer: {p}"

    os.environ[env_var] = key
    return None


def _read_kg_final_path_from_config(config_path: str) -> str:
    p = Path(config_path).expanduser().resolve()
    if not p.exists():
        return ""
    try:
        cfg = _load_json(p)
        kgp = cfg.get("kg_final_path") or cfg.get("kg_final") or ""
        return str(kgp).strip()
    except Exception:
        return ""


def parse_args() -> argparse.Namespace:
    """
    Streamlit startet das Script selbst und übergibt ggf. zusätzliche CLI-Flags.
    Deshalb nur bekannte Argumente parsen und alles andere ignorieren.

    Übergabe in Streamlit:
      streamlit run python/msrguard/excH_kg_agent_ui.py -- --event_json_path <...> --out_json <...>
    """
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--event_json_path", required=False, default="", help="Pfad zur Event JSON (Input)")
    ap.add_argument("--out_json", required=False, default="", help="Pfad zur Result JSON (Output)")
    ap.add_argument("--server_port", required=False, type=int, default=8501, help="Streamlit Port (default: 8501)")
    ap.add_argument("--no_open_browser", action="store_false", dest="open_browser", help="Browser nicht automatisch öffnen")
    ap.set_defaults(open_browser=True)
    ns, _unknown = ap.parse_known_args()
    return ns


def read_event(event_json_path: str) -> Dict[str, Any]:
    p = Path(event_json_path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"event_json_path nicht gefunden: {p}")
    return _load_json(p)


def _json_or_str(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)


def streamlit_main() -> None:
    st.set_page_config(page_title="MSRGuard ExcH Agent", layout="wide")

    def sanitize_for_path(s: str) -> str:
        s = (s or "").strip()
        s = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)
        return s[:80] if s else "noid"

    def make_session_dir(event: Dict[str, Any]) -> Optional[Path]:
        try:
            here = Path(__file__).resolve()
            python_root = here.parent.parent  # .../python
            out_dir = python_root / "agent_results"
            out_dir.mkdir(parents=True, exist_ok=True)

            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            corr = sanitize_for_path(str(payload.get("correlationId") or payload.get("corr") or ""))
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            session_dir = out_dir / f"streamlit_{ts}_{corr}"
            session_dir.mkdir(parents=True, exist_ok=True)
            return session_dir
        except Exception:
            return None

    def ensure_state_defaults() -> None:
        if "cfg" not in st.session_state:
            st.session_state["cfg"] = load_ui_config()
        if "event" not in st.session_state:
            st.session_state["event"] = {}
        if "event_json_path" not in st.session_state:
            st.session_state["event_json_path"] = ""
        if "event_loaded_from_path" not in st.session_state:
            st.session_state["event_loaded_from_path"] = ""
        if "event_autoload_error" not in st.session_state:
            st.session_state["event_autoload_error"] = ""
        if "out_json_path" not in st.session_state:
            st.session_state["out_json_path"] = ""
        if "pipeline_enabled" not in st.session_state:
            st.session_state["pipeline_enabled"] = bool(st.session_state["cfg"].pipeline.enabled)
        if "pipeline_status" not in st.session_state:
            st.session_state["pipeline_status"] = {"ok": None, "error": "", "stdout": "", "stderr": "", "kg_path": ""}
        if "analysis_started" not in st.session_state:
            st.session_state["analysis_started"] = False
        if "analysis_done" not in st.session_state:
            st.session_state["analysis_done"] = False
        if "agent_result" not in st.session_state:
            st.session_state["agent_result"] = None
        if "chatbot_session" not in st.session_state:
            st.session_state["chatbot_session"] = None
        if "chatbot_last_error" not in st.session_state:
            st.session_state["chatbot_last_error"] = ""
        if "chat_transcript" not in st.session_state:
            st.session_state["chat_transcript"] = []
        if "ui_events" not in st.session_state:
            st.session_state["ui_events"] = []
        if "posted_initial_system_messages" not in st.session_state:
            st.session_state["posted_initial_system_messages"] = False
        if "chat_log_path" not in st.session_state:
            st.session_state["chat_log_path"] = ""
        if "chat_started_at_utc" not in st.session_state:
            st.session_state["chat_started_at_utc"] = ""
        if "last_chatbot_debug" not in st.session_state:
            st.session_state["last_chatbot_debug"] = {"plan": None, "tool_results": None}
        if "last_result_blob" not in st.session_state:
            st.session_state["last_result_blob"] = None
        if "auto_followup_enabled" not in st.session_state:
            st.session_state["auto_followup_enabled"] = True
        if "pending_auto_followup_prompt" not in st.session_state:
            st.session_state["pending_auto_followup_prompt"] = ""
        if "auto_followup_done" not in st.session_state:
            st.session_state["auto_followup_done"] = False

    def flush_chat_log() -> None:
        path = (st.session_state.get("chat_log_path") or "").strip()
        if not path:
            return
        try:
            event = st.session_state.get("event") or {}
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            meta = {
                "started_at_utc": st.session_state.get("chat_started_at_utc") or _utc_now_iso(),
                "event_type": event.get("type", ""),
                "correlationId": payload.get("correlationId") or payload.get("corr") or "",
                "processName": payload.get("processName") or payload.get("process") or payload.get("lastProcessName") or "",
                "out_json_path": st.session_state.get("out_json_path") or "",
            }
            if not st.session_state.get("chat_started_at_utc"):
                st.session_state["chat_started_at_utc"] = meta["started_at_utc"]

            blob = {
                "meta": meta,
                "transcript": st.session_state.get("chat_transcript") or [],
                "events": st.session_state.get("ui_events") or [],
            }
            Path(path).write_text(json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def log_ui_event(kind: str, data: Optional[Dict[str, Any]] = None) -> None:
        try:
            st.session_state["ui_events"].append({"ts_utc": _utc_now_iso(), "kind": kind, "data": data or {}})
            flush_chat_log()
        except Exception:
            pass

    def add_message(role: str, text: str) -> None:
        st.session_state["chat_transcript"].append({"ts_utc": _utc_now_iso(), "role": role, "text": text})
        flush_chat_log()

    def tool_results_look_empty(tool_results: Any) -> bool:
        if not isinstance(tool_results, dict) or not tool_results:
            return True
        for val in tool_results.values():
            if isinstance(val, dict) and "error" in val:
                return False
            if isinstance(val, list) and len(val) > 0:
                return False
            if val:
                return False
        return True

    def render_chat() -> None:
        for m in st.session_state.get("chat_transcript") or []:
            role = (m.get("role") or "").strip()
            text = m.get("text") or ""
            role_l = role.lower()

            if role_l in {"user", "benutzer"}:
                with st.chat_message("user", avatar=USER_AVATAR):
                    st.write(text)
                continue

            avatar = BOT_AVATAR if role_l in {"assistant", "bot"} else SYSTEM_AVATAR
            with st.chat_message("assistant", avatar=avatar):
                if role_l not in {"assistant", "bot"}:
                    st.markdown(f"**{role}**")
                st.markdown(str(text))

    def show_debug_hints(plan: Any, tool_results: Any) -> None:
        if isinstance(plan, dict):
            steps = plan.get("steps")
            if isinstance(steps, list) and len(steps) == 0:
                add_message("System", "Hinweis: Planner hat keine Tool-Schritte geplant (steps=[]).")

        if isinstance(tool_results, dict):
            err_steps = [
                k
                for k, v in tool_results.items()
                if isinstance(v, dict) and str(v.get("error", "")).strip()
            ]
            if err_steps:
                add_message("System", "Hinweis: Tool-Fehler in: " + ", ".join(err_steps))

        if tool_results_look_empty(tool_results):
            add_message(
                "System",
                "Hinweis: Keine/zu wenige Tool-Ergebnisse für eine konkrete Antwort. "
                "Der ChatBot konnte vermutlich nicht die richtigen Tools auswählen oder das KG enthält die Info nicht.",
            )

    def run_pending_auto_followup_if_needed() -> None:
        prompt = str(st.session_state.get("pending_auto_followup_prompt") or "").strip()
        if not prompt:
            return

        session = st.session_state.get("chatbot_session")
        if session is None:
            st.session_state["pending_auto_followup_prompt"] = ""
            st.session_state["auto_followup_done"] = False
            return

        with st.chat_message("assistant", avatar=BOT_AVATAR):
            with st.spinner("Automatische Folgeanalyse läuft…"):
                try:
                    res = session.ask(prompt, debug=True)
                    plan = res.get("plan") if isinstance(res, dict) else None
                    tool_results = res.get("tool_results") if isinstance(res, dict) else None
                    st.session_state["last_chatbot_debug"] = {"plan": plan, "tool_results": tool_results}
                    log_ui_event(
                        "chatbot_auto_followup_debug",
                        {"has_plan": bool(plan), "has_tool_results": bool(tool_results)},
                    )

                    answer = res.get("answer") if isinstance(res, dict) else None
                    out_text = answer or _json_or_str(res)
                    add_message(
                        "Assistant",
                        "Automatische Folgeanalyse (Pfad-Erklärung + Vermeidungsmaßnahmen):\n\n" + out_text,
                    )
                    show_debug_hints(plan, tool_results)
                    st.session_state["auto_followup_done"] = True
                    log_ui_event("chatbot_auto_followup_done", {"ok": True})
                except Exception as e:
                    add_message("System", f"Auto-Folgeanalyse Fehler: {e}")
                    st.session_state["auto_followup_done"] = False
                    log_ui_event("chatbot_auto_followup_done", {"ok": False, "error": str(e)})

        st.session_state["pending_auto_followup_prompt"] = ""
        st.rerun()

    def import_handle_event():
        ensure_python_root_on_sys_path()
        try:
            from msrguard.excH_kg_agent_core import handle_event  # type: ignore

            return handle_event
        except Exception:
            from excH_kg_agent_core import handle_event  # type: ignore

            return handle_event

    def import_chatbot_pieces():
        ensure_python_root_on_sys_path()
        try:
            from msrguard.excH_chatbot import IncidentContext, ExcHChatBotSession, run_initial_analysis  # type: ignore
        except Exception:
            from excH_chatbot import IncidentContext, ExcHChatBotSession, run_initial_analysis  # type: ignore

        try:
            from msrguard.chatbot_core import build_bot  # type: ignore
        except Exception:
            from chatbot_core import build_bot  # type: ignore

        return IncidentContext, ExcHChatBotSession, run_initial_analysis, build_bot

    def run_pipeline(cfg: PipelineConfig, event: Dict[str, Any]) -> Dict[str, Any]:
        runner = Path(cfg.runner).expanduser().resolve()
        if not runner.exists():
            raise FileNotFoundError(f"Pipeline runner nicht gefunden: {runner}")

        cwd = Path(cfg.dir).expanduser().resolve()
        if not cwd.exists():
            raise FileNotFoundError(f"Pipeline dir nicht gefunden: {cwd}")

        cmd = [sys.executable, str(runner)]
        kw: Dict[str, Any] = dict(cwd=str(cwd), capture_output=True, text=True)
        if cfg.timeout_sec is not None:
            kw["timeout"] = int(cfg.timeout_sec)

        proc = subprocess.run(cmd, **kw)  # type: ignore[arg-type]
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""

        if proc.returncode != 0:
            raise RuntimeError(
                f"Pipeline returncode={proc.returncode}\n"
                f"STDOUT:\n{stdout[-2000:]}\n"
                f"STDERR:\n{stderr[-2000:]}"
            )

        kg_path = _read_kg_final_path_from_config(cfg.config)
        if not kg_path:
            raise RuntimeError(f"kg_final_path fehlt in {cfg.config}")

        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        payload = dict(payload)
        payload["kg_ttl_path"] = kg_path
        event["payload"] = payload
        os.environ["MSRGUARD_KG_TTL"] = kg_path

        return {"ok": True, "kg_path": kg_path, "stdout": stdout, "stderr": stderr}

    def init_chat_log_if_needed(event: Dict[str, Any]) -> None:
        if (st.session_state.get("chat_log_path") or "").strip():
            return
        session_dir = make_session_dir(event)
        if not session_dir:
            return
        st.session_state["chat_log_path"] = str(session_dir / "chatBot_verlauf.json")
        flush_chat_log()

    def post_initial_system_messages(event: Dict[str, Any], pipeline_enabled: bool) -> None:
        if st.session_state.get("posted_initial_system_messages"):
            return

        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        last_skill = payload.get("lastSkill") or payload.get("lastExecutedSkill") or payload.get("interruptedSkill") or ""
        proc = payload.get("processName") or payload.get("lastExecutedProcess") or ""
        summary = payload.get("summary") or ""

        msg1 = "Unbekannter Fehler wurde erkannt."
        if last_skill:
            msg1 += f" lastSkill={last_skill}"
        if proc:
            msg1 += f" process={proc}"
        if summary:
            msg1 += f" | {summary}"
        add_message("System", msg1)

        if pipeline_enabled:
            add_message("System", "Pipeline wird vor der Analyse ausgeführt. Klicke 'Weiter' um zu starten oder 'Abbrechen'.")
        else:
            add_message("System", "Klicke 'Weiter' um Analyse zu starten oder 'Abbrechen'.")

        st.session_state["posted_initial_system_messages"] = True

    def autoload_event_from_path_if_needed() -> None:
        raw_path = (st.session_state.get("event_json_path") or "").strip()
        if not raw_path:
            return

        try:
            resolved = str(Path(raw_path).expanduser().resolve())
        except Exception:
            resolved = raw_path

        loaded_from = str(st.session_state.get("event_loaded_from_path") or "")
        has_event = bool(st.session_state.get("event"))
        if has_event and loaded_from == "__manual__":
            return
        if has_event and loaded_from == resolved:
            return

        try:
            event_obj = read_event(resolved)
        except Exception as e:
            st.session_state["event_autoload_error"] = str(e)
            return

        st.session_state["event"] = event_obj
        st.session_state["event_json_path"] = resolved
        st.session_state["event_loaded_from_path"] = resolved
        st.session_state["event_autoload_error"] = ""
        init_chat_log_if_needed(event_obj)
        post_initial_system_messages(event_obj, bool(st.session_state.get("pipeline_enabled")))
        log_ui_event("event_autoloaded_path", {"path": resolved})

    def build_result_blob(*, continue_flag: bool, reason: str) -> Dict[str, Any]:
        cfg: UiConfig = st.session_state["cfg"]
        pipeline_status = st.session_state.get("pipeline_status") or {}
        return {
            "continue": bool(continue_flag),
            "reason": reason,
            "agent_result": st.session_state.get("agent_result"),
            "chatbot": {
                "ok": st.session_state.get("chatbot_session") is not None,
                "error": st.session_state.get("chatbot_last_error") or "",
            },
            "chatbot_transcript": st.session_state.get("chat_transcript") or [],
            "pipeline": {
                "enabled": bool(st.session_state.get("pipeline_enabled")),
                "ok": pipeline_status.get("ok"),
                "error": pipeline_status.get("error", ""),
                "stdout_tail": (pipeline_status.get("stdout") or "")[-4000:],
                "stderr_tail": (pipeline_status.get("stderr") or "")[-4000:],
            },
            "event": st.session_state.get("event"),
            "ui": {
                "config": {
                    "openai_api_key_file": cfg.openai_api_key_file,
                    "pipeline": {
                        "enabled": cfg.pipeline.enabled,
                        "dir": cfg.pipeline.dir,
                        "runner": cfg.pipeline.runner,
                        "config": cfg.pipeline.config,
                        "timeout_sec": cfg.pipeline.timeout_sec,
                    },
                    "chatbot": {"model": cfg.chatbot.model, "temperature": cfg.chatbot.temperature},
                }
            },
        }

    def write_out_json(blob: Dict[str, Any]) -> Optional[str]:
        out_json_path = (st.session_state.get("out_json_path") or "").strip()
        if not out_json_path:
            return None
        outp = Path(out_json_path).expanduser().resolve()
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(outp)

    def start_analysis(event: Dict[str, Any]) -> None:
        cfg: UiConfig = st.session_state["cfg"]
        st.session_state["analysis_started"] = True
        log_ui_event("analysis_started", {"pipeline_enabled": bool(st.session_state.get("pipeline_enabled"))})

        err = try_set_openai_key_from_file(cfg.openai_api_key_file, provider=cfg.chatbot.provider)
        if err:
            add_message("System", f"ChatBot Key Hinweis: {err}")

        if st.session_state.get("pipeline_enabled"):
            with st.chat_message("assistant", avatar=SYSTEM_AVATAR):
                with st.spinner("Pipeline läuft…"):
                    try:
                        status = run_pipeline(cfg.pipeline, event)
                        st.session_state["pipeline_status"] = status
                        add_message("System", f"Pipeline OK. KG: {status.get('kg_path')}")
                        log_ui_event("pipeline_done", {"ok": True, "kg_path": status.get("kg_path")})
                    except Exception as e:
                        st.session_state["pipeline_status"] = {
                            "ok": False,
                            "error": str(e),
                            "stdout": "",
                            "stderr": "",
                            "kg_path": "",
                        }
                        add_message("System", f"Pipeline FEHLER: {e}")
                        log_ui_event("pipeline_done", {"ok": False, "error": str(e)})

        with st.chat_message("assistant", avatar=BOT_AVATAR):
            with st.spinner("Agent-Analyse läuft…"):
                try:
                    handle_event = import_handle_event()
                    st.session_state["agent_result"] = handle_event(event)
                    log_ui_event("agent_core_done", {"ok": True})
                except Exception as e:
                    st.session_state["agent_result"] = {"status": "error", "error": str(e)}
                    add_message("System", f"Agent Fehler: {e}")
                    log_ui_event("agent_core_done", {"ok": False, "error": str(e)})

        with st.chat_message("assistant", avatar=BOT_AVATAR):
            with st.spinner("ChatBot initialisiert und denkt…"):
                try:
                    IncidentContext, ExcHChatBotSession, run_initial_analysis, build_bot = import_chatbot_pieces()
                    ctx = IncidentContext.from_event(event)

                    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                    kg_path = payload.get("kg_ttl_path") or os.environ.get("MSRGUARD_KG_TTL", "")

                    bot = build_bot(
                        kg_ttl_path=str(kg_path),
                        provider=cfg.chatbot.provider,
                        openai_model=cfg.chatbot.model,
                        openai_temperature=cfg.chatbot.temperature,
                        planner_provider=cfg.chatbot.planner_provider,
                        planner_model=cfg.chatbot.planner_model,
                        plc_snapshot=ctx.plcSnapshot,
                    )
                    session = ExcHChatBotSession(bot=bot, ctx=ctx)
                    st.session_state["chatbot_session"] = session

                    res = run_initial_analysis(session, debug=True)
                    plan = res.get("plan") if isinstance(res, dict) else None
                    tool_results = res.get("tool_results") if isinstance(res, dict) else None
                    st.session_state["last_chatbot_debug"] = {"plan": plan, "tool_results": tool_results}
                    log_ui_event("chatbot_initial_debug", {"has_plan": bool(plan), "has_tool_results": bool(tool_results)})

                    answer = res.get("answer") if isinstance(res, dict) else None
                    add_message("Assistant", answer or _json_or_str(res))
                    show_debug_hints(plan, tool_results)

                    if st.session_state.get("auto_followup_enabled"):
                        st.session_state["pending_auto_followup_prompt"] = (
                            "Bitte erklaere jetzt den Fehlerpfad aus der Initialanalyse im Detail. "
                            "Fokus: Welche Rolle spielt `PeriodicFaultPeriod` in `FB_Automatikbetrieb_F1`, "
                            "wie fuehrt der Pfad bis `OPCUA.TriggerD2`, und ob eine periodische Ausloesung "
                            "aus den vorhandenen Trace-Belegen abgeleitet werden kann (ohne Annahmen ohne Evidenz). "
                            "Nutze Code + Variablendeklaration von `FB_Automatikbetrieb_F1`, die bereits im KG liegen. "
                            "Danach nenne konkrete Vermeidungsmaßnahmen, priorisiert, und begruende sie nur mit "
                            "der vorhandenen softwareseitigen Evidenz des Triggerpfads."
                        )
                        st.session_state["auto_followup_done"] = False
                        add_message(
                            "System",
                            "Automatische Folgeanalyse wird gestartet (Pfad-Erklärung + Vermeidungsmaßnahmen).",
                        )
                        log_ui_event("chatbot_auto_followup_queued", {"enabled": True})
                except Exception as e:
                    st.session_state["chatbot_session"] = None
                    st.session_state["chatbot_last_error"] = str(e)
                    add_message("System", f"ChatBot init Fehler: {e}")
                    log_ui_event("chatbot_init_failed", {"error": str(e)})

        st.session_state["analysis_done"] = True
        log_ui_event("analysis_done", {})

    def reset_session() -> None:
        keep_cfg = st.session_state.get("cfg")
        st.session_state.clear()
        st.session_state["cfg"] = keep_cfg or load_ui_config()
        ensure_state_defaults()

    ensure_state_defaults()

    args = parse_args()
    if args.event_json_path and not st.session_state.get("event_json_path"):
        st.session_state["event_json_path"] = str(Path(args.event_json_path).expanduser().resolve())
    if args.out_json and not st.session_state.get("out_json_path"):
        st.session_state["out_json_path"] = args.out_json
    autoload_event_from_path_if_needed()

    cfg: UiConfig = st.session_state["cfg"]

    st.title("MSRGuard ExcH Agent")

    with st.sidebar:
        st.subheader("Einstellungen")
        st.checkbox("Pipeline vor Analyse ausführen", key="pipeline_enabled")
        st.checkbox(
            "Automatische Folgeanalyse (Pfad-Erklärung + Maßnahmen)",
            key="auto_followup_enabled",
        )
        st.text_input("Result JSON (out_json)", key="out_json_path", placeholder="optional: Pfad zur Result JSON")

        with st.expander("UI Config (excH_agent_config.json)"):
            st.json(
                {
                    "openai_api_key_file": cfg.openai_api_key_file,
                    "pipeline": {
                        "enabled": cfg.pipeline.enabled,
                        "dir": cfg.pipeline.dir,
                        "runner": cfg.pipeline.runner,
                        "config": cfg.pipeline.config,
                        "timeout_sec": cfg.pipeline.timeout_sec,
                    },
                    "chatbot": {"model": cfg.chatbot.model, "temperature": cfg.chatbot.temperature},
                }
            )

        st.subheader("Event laden")
        st.text_input("event_json_path", key="event_json_path", placeholder="Pfad zur Event JSON")
        if (st.session_state.get("event_autoload_error") or "").strip():
            st.warning(f"Auto-Load Fehler: {st.session_state.get('event_autoload_error')}")
        uploaded = st.file_uploader("oder Upload (Event JSON)", type=["json"])
        col_a, col_b = st.columns(2)
        load_clicked = col_a.button("Laden", use_container_width=True)
        reset_clicked = col_b.button("Reset", use_container_width=True)

        if reset_clicked:
            reset_session()
            st.rerun()

        if uploaded is not None:
            try:
                st.session_state["event"] = json.loads(uploaded.getvalue().decode("utf-8"))
                st.session_state["event_loaded_from_path"] = "__manual__"
                st.session_state["event_autoload_error"] = ""
                init_chat_log_if_needed(st.session_state["event"])
                post_initial_system_messages(st.session_state["event"], bool(st.session_state.get("pipeline_enabled")))
                log_ui_event("event_loaded_upload", {"name": uploaded.name})
            except Exception as e:
                st.error(f"Upload JSON konnte nicht gelesen werden: {e}")

        if load_clicked and (st.session_state.get("event_json_path") or "").strip():
            try:
                resolved = str(Path(st.session_state["event_json_path"]).expanduser().resolve())
                st.session_state["event"] = read_event(resolved)
                st.session_state["event_json_path"] = resolved
                st.session_state["event_loaded_from_path"] = resolved
                st.session_state["event_autoload_error"] = ""
                init_chat_log_if_needed(st.session_state["event"])
                post_initial_system_messages(st.session_state["event"], bool(st.session_state.get("pipeline_enabled")))
                log_ui_event("event_loaded_path", {"path": st.session_state["event_json_path"]})
                st.success("Event geladen.")
            except Exception as e:
                st.error(str(e))

    event: Dict[str, Any] = st.session_state.get("event") or {}

    left, mid, right = st.columns([1.2, 2.6, 1.2], gap="large")

    with left:
        st.subheader("Event (Input)")
        if event:
            st.json(event)
        else:
            st.info("Noch kein Event geladen.")

        with st.expander("Event JSON bearbeiten"):
            raw = st.text_area("Event JSON", value=json.dumps(event or {}, ensure_ascii=False, indent=2), height=260)
            if st.button("Übernehmen", use_container_width=True):
                try:
                    st.session_state["event"] = json.loads(raw)
                    st.session_state["event_loaded_from_path"] = "__manual__"
                    st.session_state["event_autoload_error"] = ""
                    init_chat_log_if_needed(st.session_state["event"])
                    post_initial_system_messages(st.session_state["event"], bool(st.session_state.get("pipeline_enabled")))
                    log_ui_event("event_edited", {})
                    st.success("Event aktualisiert.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Ungültiges JSON: {e}")

    with mid:
        st.subheader("ChatBot")

        btn_c1, btn_c2, btn_c3 = st.columns([1, 1, 1])
        if not st.session_state.get("analysis_started"):
            start_label = "Weiter (Analyse starten)"
        elif st.session_state.get("analysis_done"):
            start_label = "Weiter (Result schreiben)"
        else:
            start_label = "Analyse läuft…"

        start_clicked = btn_c1.button(start_label, use_container_width=True)
        abort_clicked = btn_c2.button("Abbrechen", use_container_width=True)
        download_clicked = btn_c3.button("Result herunterladen", use_container_width=True)

        if abort_clicked:
            blob = build_result_blob(continue_flag=False, reason="user_abort")
            wrote = write_out_json(blob)
            if wrote:
                st.info(f"Result geschrieben: {wrote}")
            st.session_state["last_result_blob"] = blob
            log_ui_event("user_abort", {"wrote": bool(wrote)})

        if start_clicked:
            if not event:
                st.error("Kein Event geladen.")
            elif not st.session_state.get("analysis_started"):
                init_chat_log_if_needed(event)
                post_initial_system_messages(event, bool(st.session_state.get("pipeline_enabled")))
                start_analysis(event)
                st.rerun()
            elif st.session_state.get("analysis_done"):
                blob = build_result_blob(continue_flag=True, reason="user_continue_after_analysis")
                wrote = write_out_json(blob)
                if wrote:
                    st.success(f"Result geschrieben: {wrote}")
                st.session_state["last_result_blob"] = blob
                log_ui_event("user_continue", {"wrote": bool(wrote)})

        if download_clicked:
            blob = st.session_state.get("last_result_blob") or build_result_blob(
                continue_flag=bool(st.session_state.get("analysis_done")),
                reason="download_snapshot",
            )
            st.download_button(
                "Download result.json",
                data=json.dumps(blob, indent=2, ensure_ascii=False).encode("utf-8"),
                file_name="exch_agent_result.json",
                mime="application/json",
                use_container_width=True,
            )

        render_chat()
        run_pending_auto_followup_if_needed()

        chatbot_ready = st.session_state.get("chatbot_session") is not None
        user_msg = st.chat_input("Frage an den ChatBot…", disabled=not chatbot_ready)
        if user_msg:
            add_message("User", user_msg)
            session = st.session_state.get("chatbot_session")
            if session is None:
                add_message("System", "ChatBot ist nicht initialisiert. Bitte zuerst 'Weiter' drücken oder Key/Config prüfen.")
                st.rerun()

            with st.chat_message("assistant", avatar=BOT_AVATAR):
                with st.spinner("Denke…"):
                    try:
                        res = session.ask(user_msg, debug=True)
                        plan = res.get("plan") if isinstance(res, dict) else None
                        tool_results = res.get("tool_results") if isinstance(res, dict) else None
                        st.session_state["last_chatbot_debug"] = {"plan": plan, "tool_results": tool_results}
                        log_ui_event(
                            "chatbot_message_debug",
                            {"has_plan": bool(plan), "has_tool_results": bool(tool_results)},
                        )

                        answer = res.get("answer") if isinstance(res, dict) else None
                        add_message("Assistant", answer or _json_or_str(res))
                        show_debug_hints(plan, tool_results)
                    except Exception as e:
                        add_message("System", f"ChatBot Fehler: {e}")
            st.rerun()

    with right:
        st.subheader("Agent Output")
        if st.session_state.get("agent_result") is not None:
            st.json(st.session_state.get("agent_result"))
        else:
            st.info("Noch keine Analyse ausgeführt.")

        with st.expander("Pipeline"):
            st.json(st.session_state.get("pipeline_status") or {})

        with st.expander("ChatBot Debug (Plan/Tool Results)"):
            dbg = st.session_state.get("last_chatbot_debug") or {}
            if dbg.get("plan") is not None:
                st.markdown("**Plan**")
                st.json(dbg.get("plan"))
            if dbg.get("tool_results") is not None:
                st.markdown("**Tool Results**")
                st.json(dbg.get("tool_results"))

        with st.expander("UI Events"):
            st.json(st.session_state.get("ui_events") or [])


def main() -> None:
    ensure_python_root_on_sys_path()
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore

        in_runtime = get_script_run_ctx() is not None
    except Exception:
        in_runtime = False

    if not in_runtime:
        args = parse_args()

        if not (args.event_json_path or "").strip():
            print(
                "Direktstart ohne Streamlit-Context erkannt, aber --event_json_path fehlt.\n"
                "Bitte starten mit:\n"
                "  streamlit run python/msrguard/excH_kg_agent_ui.py -- --event_json_path <event.json> --out_json <result.json>\n"
            )
            return

        out_json_path = (args.out_json or "").strip()
        out_json_file = Path(out_json_path).expanduser().resolve() if out_json_path else None
        if out_json_file:
            try:
                out_json_file.unlink(missing_ok=True)  # ensure we do not consume stale output
            except Exception:
                pass

        script_path = Path(__file__).resolve()
        server_port = int(getattr(args, "server_port", 8501) or 8501)
        ui_url = f"http://localhost:{server_port}"
        cmd: List[str] = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(script_path),
            "--server.headless=false",
            "--server.address=localhost",
            f"--server.port={server_port}",
            "--",
            "--event_json_path",
            str(Path(args.event_json_path).expanduser().resolve()),
        ]
        if out_json_path:
            cmd.extend(["--out_json", str(Path(out_json_path).expanduser().resolve())])
        cmd.extend(["--server_port", str(server_port)])
        if not bool(getattr(args, "open_browser", True)):
            cmd.append("--no_open_browser")

        print("[excH_kg_agent_ui] Bare-mode detected, starting Streamlit runner...")
        proc = subprocess.Popen(cmd)
        if bool(getattr(args, "open_browser", True)):
            try:
                webbrowser.open_new_tab(ui_url)
                print(f"[excH_kg_agent_ui] UI geöffnet: {ui_url}")
            except Exception as e:
                print(f"[excH_kg_agent_ui] Konnte Browser nicht automatisch öffnen ({ui_url}): {e}")
        wrote_out_json = False
        try:
            if out_json_file is None:
                # If no out_json is requested, behave like a regular streamlit run.
                rc = proc.wait()
                if rc != 0:
                    sys.exit(rc)
                return

            import time

            while True:
                if proc.poll() is not None:
                    # Streamlit exited before writing out_json.
                    break
                if out_json_file.exists() and out_json_file.stat().st_size > 0:
                    # UI finished and wrote the expected handshake file.
                    wrote_out_json = True
                    break
                time.sleep(0.2)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except Exception:
                    proc.kill()
                    proc.wait(timeout=3)
        if out_json_file is not None and not wrote_out_json:
            child_rc = proc.returncode if proc.returncode is not None else 1
            print(
                f"[excH_kg_agent_ui] Streamlit exited without writing out_json: {out_json_file} (rc={child_rc})",
                file=sys.stderr,
            )
            sys.exit(child_rc if child_rc != 0 else 2)
        return

    streamlit_main()


if __name__ == "__main__":
    main()
