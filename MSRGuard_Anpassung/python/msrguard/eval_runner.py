"""eval_runner.py — Headless Evaluation Harness (kein Streamlit-UI)

Führt einen Testfall gegen ExcH-KG-Agent oder ExcH-RAG-Agent aus und
schreibt eine strukturierte EvalResult-JSON in Evaluation/results/.

─── Start via JSON-Config (empfohlen) ────────────────────────────────────
    python -m msrguard.eval_runner --from-config Evaluation/configs/TC-001_kg.json

─── Direkte CLI-Argumente (alternativ) ───────────────────────────────────
    python -m msrguard.eval_runner \\
        --agent kg \\
        --event-json MSRGuard_Anpassung/python/agent_results/evD2-xxx_event.json \\
        --question "Warum wurde TriggerD2 gesetzt?" \\
        --provider groq --model llama-3.3-70b-versatile \\
        --kg-ttl Pfad/zum/kg_final.ttl

─── Als Bibliothek ───────────────────────────────────────────────────────
    from msrguard.eval_runner import run_eval, EvalConfig, ModelConfig
    result = run_eval(EvalConfig(agent="kg", event_json_path="...", question="..."))
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

# Standard-Ausgabeverzeichnis (relativ zum Arbeitsverzeichnis)
DEFAULT_RESULTS_DIR = "Evaluation/results"


# ---------------------------------------------------------------------------
# Datenstrukturen
# ---------------------------------------------------------------------------

@dataclass
class ModelConfig:
    """LLM-Konfiguration für Synthese und (optional) Planner."""
    provider: str = "groq"
    model: str = "llama-3.3-70b-versatile"
    temperature: float = 0.0
    planner_provider: Optional[str] = None   # None = gleich wie provider
    planner_model: Optional[str] = None       # None = gleich wie model
    judge_provider: Optional[str] = None      # None = gleich wie provider
    judge_model: Optional[str] = None         # None = gleich wie model
    judge_temperature: Optional[float] = None # None = 0.0


@dataclass
class EvalConfig:
    """Vollständige Konfiguration für einen einzelnen Eval-Lauf.

    Kann aus einer JSON-Datei geladen werden (EvalConfig.from_json_file)
    oder programmatisch erzeugt werden.
    """
    agent: str                              # "kg" oder "rag"
    event_json_path: str                    # Pfad zur Event-JSON
    question: str                           # Frage an den Agenten

    model: ModelConfig = field(default_factory=ModelConfig)

    # Pfade
    kg_ttl_path: str = ""                  # KG-Agent: TTL-Datei
    xml_path: str = ""                     # RAG-Agent: PLCOpenXML-Datei
    api_key_file: str = ""                 # Pfad zur API-Key-Datei
    pipeline_config: str = ""              # Für XML-Autoerkennung (RAG)

    # Test-Metadaten
    test_id: str = ""
    ground_truth: Optional[str] = None     # Erwartete Root Cause (Freitext)

    # Pricing: {"input_per_1k": 0.00, "output_per_1k": 0.00} in USD
    pricing: Optional[Dict[str, float]] = None
    planner_pricing: Optional[Dict[str, float]] = None
    judge_pricing: Optional[Dict[str, float]] = None

    top_k: int = 6                         # Nur RAG: Anzahl Retrieval-Chunks

    # Ausgabe
    results_dir: str = DEFAULT_RESULTS_DIR  # Verzeichnis für Ergebnisse
    out_json: str = ""                      # Expliziter Pfad; leer = auto

    @classmethod
    def from_json_file(cls, path: str) -> "EvalConfig":
        """Lädt eine EvalConfig aus einer JSON-Datei."""
        raw = _load_json(path)

        model_raw = raw.get("model", {}) if isinstance(raw.get("model"), dict) else {}
        model = ModelConfig(
            provider=str(model_raw.get("provider", "groq")),
            model=str(model_raw.get("model", "llama-3.3-70b-versatile")),
            temperature=float(model_raw.get("temperature", 0.0)),
            planner_provider=model_raw.get("planner_provider") or None,
            planner_model=model_raw.get("planner_model") or None,
            judge_provider=model_raw.get("judge_provider") or None,
            judge_model=model_raw.get("judge_model") or None,
            judge_temperature=(
                float(model_raw["judge_temperature"])
                if model_raw.get("judge_temperature", None) is not None
                else None
            ),
        )

        pricing_raw = raw.get("pricing")
        planner_pricing_raw = raw.get("planner_pricing")
        judge_pricing_raw = raw.get("judge_pricing")

        return cls(
            agent=str(raw.get("agent", "kg")),
            event_json_path=str(raw.get("event_json_path", "")),
            question=str(raw.get("question", "")),
            model=model,
            kg_ttl_path=str(raw.get("kg_ttl_path", "")),
            xml_path=str(raw.get("xml_path", "")),
            api_key_file=str(raw.get("api_key_file", "")),
            pipeline_config=str(raw.get("pipeline_config", "")),
            test_id=str(raw.get("test_id", "")),
            ground_truth=raw.get("ground_truth") or None,
            pricing=pricing_raw if isinstance(pricing_raw, dict) else None,
            planner_pricing=planner_pricing_raw if isinstance(planner_pricing_raw, dict) else None,
            judge_pricing=judge_pricing_raw if isinstance(judge_pricing_raw, dict) else None,
            top_k=int(raw.get("top_k", 6)),
            results_dir=str(raw.get("results_dir", DEFAULT_RESULTS_DIR)),
            out_json=str(raw.get("out_json", "")),
        )

    def resolve_out_json(self) -> str:
        """Gibt den vollständigen Ausgabepfad zurück (auto-generiert wenn leer)."""
        if self.out_json:
            return self.out_json
        model_slug = self.model.model.replace(":", "-").replace("/", "-")
        filename = f"{self.test_id or 'run'}_{self.agent}_{self.model.provider}_{model_slug}.json"
        return str(Path(self.results_dir) / filename)


@dataclass
class EvalResult:
    """Ergebnis eines einzelnen Eval-Laufs."""
    test_id: str
    agent: str
    provider: str
    model: str
    planner_provider: Optional[str]
    planner_model: Optional[str]

    question: str
    answer: str
    ground_truth: Optional[str]

    # Zeitmessungen (Sekunden)
    t_initial_analysis_s: float     # Initiale deterministische Analyse
    t_question_s: float             # Automatische Folgeanalyse / zweiter Chat-Schritt
    t_total_s: float                # Summe

    # Token- und Kostentracking
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cost_usd: float

    # Nach dem Lauf manuell (oder per LLM-Judge) befüllen
    root_cause_found: Optional[bool] = None   # True/False/None
    notes: str = ""
    requested_question_ignored: bool = False
    effective_question: str = ""
    answer_initial: str = ""
    answer_auto_followup: str = ""
    answer_stages: List[Dict[str, str]] = field(default_factory=list)
    judge_provider: Optional[str] = None
    judge_model: Optional[str] = None
    judge_prompt_tokens: int = 0
    judge_completion_tokens: int = 0
    judge_total_tokens: int = 0
    judge_cost_usd: float = 0.0
    judge_verdict: str = "not_run"
    judge_summary: str = ""
    judge_interpretation: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def save(self, path: str) -> None:
        p = Path(path).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.as_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[eval_runner] Ergebnis gespeichert: {p}")


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _load_json(path: str) -> Dict[str, Any]:
    p = Path(path).expanduser().resolve()
    return json.loads(p.read_text(encoding="utf-8"))


def _set_api_key(api_key_file: str, provider: str) -> None:
    """Liest API-Key aus Datei und setzt die passende Umgebungsvariable."""
    _ENV = {
        "openai": "OPENAI_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
        "azure_openai": "AZURE_OPENAI_API_KEY",
        "google": "GOOGLE_API_KEY",
        "groq": "GROQ_API_KEY",
        "together": "TOGETHER_API_KEY",
    }
    _provider = (provider or "openai").lower().strip()
    if _provider == "ollama":
        return
    env_var = _ENV.get(_provider, "OPENAI_API_KEY")
    if os.environ.get(env_var):
        return
    if not api_key_file:
        return
    p = Path(api_key_file).expanduser()
    if not p.exists():
        print(f"[eval_runner] WARNUNG: api_key_file nicht gefunden: {p}")
        return
    key = p.read_text(encoding="utf-8").strip()
    if key:
        os.environ[env_var] = key


def _ensure_python_root() -> None:
    here = Path(__file__).resolve().parent.parent  # .../python/
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


def _extract_json_dict(raw_text: str) -> Dict[str, Any]:
    _ensure_python_root()
    from msrguard.chatbot_core import strip_code_fences

    text = strip_code_fences(str(raw_text or ""))
    if not text:
        raise ValueError("Leere Judge-Antwort")

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("Kein JSON-Objekt in der Judge-Antwort gefunden")

    data = json.loads(m.group(0))
    if not isinstance(data, dict):
        raise ValueError("Judge-Antwort ist kein JSON-Objekt")
    return data


def _judge_verdict_from_text(value: Any) -> str:
    verdict = str(value or "").strip().lower()
    allowed = {
        "correct",
        "partially_correct",
        "incorrect",
        "not_evaluable",
        "skipped_no_ground_truth",
        "judge_error",
    }
    return verdict if verdict in allowed else "not_evaluable"


def _append_note(existing: str, new_text: str) -> str:
    left = str(existing or "").strip()
    right = str(new_text or "").strip()
    if not left:
        return right
    if not right:
        return left
    return left + " | " + right


def _run_llm_judge(cfg: EvalConfig, result: EvalResult) -> EvalResult:
    if not cfg.ground_truth:
        result.judge_verdict = "skipped_no_ground_truth"
        result.judge_summary = "Keine automatische LLM-Bewertung durchgeführt, da keine ground_truth hinterlegt ist."
        result.judge_interpretation = "Die Antwort wurde gespeichert, aber nicht gegen einen Sollzustand verglichen."
        return result

    _ensure_python_root()
    from msrguard.chatbot_core import get_llm_invoke, LLMUsage

    judge_provider = cfg.model.judge_provider or cfg.model.provider
    judge_model = cfg.model.judge_model or cfg.model.model
    judge_temperature = cfg.model.judge_temperature if cfg.model.judge_temperature is not None else 0.0
    judge_pricing = cfg.judge_pricing if cfg.judge_pricing is not None else cfg.pricing

    _set_api_key(cfg.api_key_file, judge_provider)

    usage = LLMUsage()
    result.judge_provider = judge_provider
    result.judge_model = judge_model

    llm = get_llm_invoke(
        model=judge_model,
        temperature=judge_temperature,
        provider=judge_provider,
        usage_accumulator=usage,
        pricing=judge_pricing,
    )
    effective_question = (
        str(result.effective_question or "").strip()
        if result.requested_question_ignored and str(result.effective_question or "").strip()
        else str(cfg.question or "").strip()
    )
    ignored_question_note = ""
    if result.requested_question_ignored:
        configured_question = str(cfg.question or "").strip()
        actual_question = str(result.effective_question or "").strip()
        if configured_question and actual_question and configured_question != actual_question:
            ignored_question_note = (
                "Hinweis: Die konfigurierte Eval-Frage wurde in diesem Lauf bewusst ignoriert. "
                "Bewertet wird stattdessen die tatsächlich verwendete Agentenfrage.\n\n"
                f"Ignorierte Config-Frage:\n{configured_question}\n\n"
            )

    system_prompt = (
        "Du bist ein strenger Evaluations-Judge für technische Root-Cause-Antworten. "
        "Bewerte ausschließlich anhand von Frage, Ground Truth und Modellantwort. "
        "Antworte nur als JSON-Objekt. "
        "Erlaubte verdict-Werte: correct, partially_correct, incorrect, not_evaluable. "
        "Setze root_cause_found nur dann auf true, wenn die wesentliche Ursache aus der Ground Truth "
        "inhaltlich getroffen wurde. "
        "summary soll in 1-2 Sätzen knapp die Bewertung zusammenfassen. "
        "interpretation soll kurz benennen, was richtig und was falsch oder fehlend ist."
    )
    user_prompt = (
        "Bitte bewerte die Modellantwort.\n\n"
        + ignored_question_note
        + f"Frage:\n{effective_question}\n\n"
        f"Ground Truth:\n{cfg.ground_truth}\n\n"
        f"Modellantwort:\n{result.answer_initial if result.agent == 'rag' and result.answer_initial else result.answer}\n\n"
        "Gib nur JSON in genau diesem Format zurück:\n"
        "{\n"
        '  "root_cause_found": true,\n'
        '  "verdict": "correct",\n'
        '  "summary": "kurze Zusammenfassung",\n'
        '  "interpretation": "kurze Interpretation",\n'
        '  "notes": "optionale kurze Notiz"\n'
        "}\n"
    )

    try:
        raw = llm(system_prompt, user_prompt)
        data = _extract_json_dict(raw)

        root_cause_found = data.get("root_cause_found")
        if isinstance(root_cause_found, bool):
            result.root_cause_found = root_cause_found
        else:
            result.root_cause_found = None

        result.judge_verdict = _judge_verdict_from_text(data.get("verdict"))
        result.judge_summary = str(data.get("summary", "") or "").strip()
        result.judge_interpretation = str(data.get("interpretation", "") or "").strip()
        result.notes = _append_note(result.notes, str(data.get("notes", "") or "").strip())
    except Exception as e:
        result.judge_verdict = "judge_error"
        result.judge_summary = f"LLM-Judge fehlgeschlagen: {e}"
        result.judge_interpretation = "Die Modellantwort wurde erzeugt, aber die automatische Interpretation konnte nicht erstellt werden."
        result.notes = _append_note(result.notes, f"judge_error={e}")
    finally:
        result.judge_prompt_tokens = usage.prompt_tokens
        result.judge_completion_tokens = usage.completion_tokens
        result.judge_total_tokens = usage.total_tokens
        result.judge_cost_usd = round(usage.cost_usd, 6)

    return result


def _kg_auto_followup_prompt() -> str:
    return (
        "Bitte erklaere jetzt den Fehlerpfad aus der Initialanalyse im Detail. "
        "Fokus: Welche Rolle spielt `PeriodicFaultPeriod` in `FB_Automatikbetrieb_F1`, "
        "wie fuehrt der Pfad bis `OPCUA.TriggerD2`, und ob eine periodische Ausloesung "
        "aus den vorhandenen Trace-Belegen abgeleitet werden kann (ohne Annahmen ohne Evidenz). "
        "Nutze Code + Variablendeklaration von `FB_Automatikbetrieb_F1`, die bereits im KG liegen. "
        "Danach nenne konkrete VermeidungsmaÃŸnahmen, priorisiert, und begruende sie nur mit "
        "der vorhandenen softwareseitigen Evidenz des Triggerpfads."
    )


def _build_kg_ui_emulation_answer(initial_answer: str, auto_followup_answer: str) -> str:
    parts: List[str] = []
    first = str(initial_answer or "").strip()
    second = str(auto_followup_answer or "").strip()

    if first:
        parts.append(first)
    if second:
        parts.append("System\n\nAutomatische Folgeanalyse wird gestartet (Pfad-Erklärung + Vermeidungsmaßnahmen).")
        parts.append(
            "Automatische Folgeanalyse (Pfad-Erklärung + Vermeidungsmaßnahmen):\n\n"
            + second
        )
    return "\n\n".join(parts).strip()


def _build_rag_ui_emulation_answer(default_question: str, initial_answer: str) -> str:
    parts: List[str] = []
    question = str(default_question or "").strip()
    answer = str(initial_answer or "").strip()

    if question:
        parts.append("User\n\n" + question)
    if answer:
        parts.append("Assistant\n\n" + answer)
    return "\n\n".join(parts).strip()


# ---------------------------------------------------------------------------
# KG-Agent Eval
# ---------------------------------------------------------------------------

def _run_kg_eval(cfg: EvalConfig) -> EvalResult:
    _ensure_python_root()
    from msrguard.chatbot_core import build_bot, LLMUsage
    from msrguard.excH_chatbot import IncidentContext, ExcHChatBotSession, run_initial_analysis

    event = _load_json(cfg.event_json_path)
    ctx = IncidentContext.from_event(event)

    payload = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}
    kg_path = cfg.kg_ttl_path or payload.get("kg_ttl_path") or os.environ.get("MSRGUARD_KG_TTL", "")
    if not kg_path:
        raise RuntimeError(
            "KG-Pfad fehlt. Setze 'kg_ttl_path' in der Config, "
            "payload.kg_ttl_path im Event oder ENV MSRGUARD_KG_TTL."
        )

    usage = LLMUsage()

    bot = build_bot(
        kg_ttl_path=str(kg_path),
        provider=cfg.model.provider,
        openai_model=cfg.model.model,
        openai_temperature=cfg.model.temperature,
        planner_provider=cfg.model.planner_provider,
        planner_model=cfg.model.planner_model,
        usage_accumulator=usage,
        pricing=cfg.pricing,
        planner_pricing=cfg.planner_pricing,
        plc_snapshot=ctx.plcSnapshot,
    )
    session = ExcHChatBotSession(bot=bot, ctx=ctx)

    # UI-Ã¤quivalenter KG-Ablauf:
    # 1) deterministische Initialanalyse direkt ausgeben
    # 2) automatische Folgeanalyse mit festem Prompt ausfÃ¼hren
    # Die konfigurierte Frage wird im KG-Modus dafÃ¼r bewusst ignoriert.
    t0 = time.monotonic()
    initial_resp = run_initial_analysis(session, debug=False)
    t_initial = time.monotonic() - t0
    initial_answer = initial_resp.get("answer", "") if isinstance(initial_resp, dict) else str(initial_resp)

    t1 = time.monotonic()
    auto_followup_prompt = _kg_auto_followup_prompt()
    resp = session.ask(auto_followup_prompt, debug=False)
    t_question = time.monotonic() - t1

    auto_followup_answer = resp.get("answer", "") if isinstance(resp, dict) else str(resp)
    answer = _build_kg_ui_emulation_answer(initial_answer, auto_followup_answer)
    answer_stages: List[Dict[str, str]] = []
    if initial_answer.strip():
        answer_stages.append(
            {
                "stage": "initial_analysis",
                "role": "assistant",
                "label": "Initiale deterministische Analyse",
                "text": initial_answer.strip(),
            }
        )
    answer_stages.append(
        {
            "stage": "auto_followup_notice",
            "role": "system",
            "label": "System",
            "text": "Automatische Folgeanalyse wird gestartet (Pfad-Erklärung + Vermeidungsmaßnahmen).",
        }
    )
    if auto_followup_answer.strip():
        answer_stages.append(
            {
                "stage": "auto_followup",
                "role": "assistant",
                "label": "Automatische Folgeanalyse (Pfad-Erklärung + Vermeidungsmaßnahmen)",
                "text": auto_followup_answer.strip(),
            }
        )

    return EvalResult(
        test_id=cfg.test_id,
        agent="kg",
        provider=cfg.model.provider,
        model=cfg.model.model,
        planner_provider=cfg.model.planner_provider,
        planner_model=cfg.model.planner_model,
        question=cfg.question,
        answer=answer,
        ground_truth=cfg.ground_truth,
        t_initial_analysis_s=round(t_initial, 3),
        t_question_s=round(t_question, 3),
        t_total_s=round(t_initial + t_question, 3),
        prompt_tokens=usage.prompt_tokens,
        completion_tokens=usage.completion_tokens,
        total_tokens=usage.total_tokens,
        cost_usd=round(usage.cost_usd, 6),
        requested_question_ignored=True,
        effective_question=auto_followup_prompt,
        answer_initial=initial_answer,
        answer_auto_followup=auto_followup_answer,
        answer_stages=answer_stages,
        notes=_append_note(
            "",
            "KG-UI-Emulation aktiv: konfigurierte Frage wurde ignoriert; stattdessen Initialanalyse + automatische Folgeanalyse ausgeführt.",
        ),
    )


# ---------------------------------------------------------------------------
# RAG-Agent Eval
# ---------------------------------------------------------------------------

def _run_rag_eval(cfg: EvalConfig) -> EvalResult:
    _ensure_python_root()
    from msrguard.chatbot_core import LLMUsage
    from msrguard.simple_rag_agent import build_session_from_input

    event = _load_json(cfg.event_json_path)
    usage = LLMUsage()

    t0 = time.monotonic()
    session = build_session_from_input(
        event,
        xml_path=cfg.xml_path,
        pipeline_config_path=cfg.pipeline_config,
        provider=cfg.model.provider,
        openai_model=cfg.model.model,
        openai_temperature=cfg.model.temperature,
        top_k=cfg.top_k,
        usage_accumulator=usage,
        pricing=cfg.pricing,
    )
    init_res = session.run_initial_analysis(debug=False)
    t_initial = time.monotonic() - t0

    default_question = init_res.get("default_question", "") if isinstance(init_res, dict) else ""
    initial_answer = init_res.get("answer", "") if isinstance(init_res, dict) else str(init_res)
    answer = _build_rag_ui_emulation_answer(default_question, initial_answer)

    answer_stages: List[Dict[str, str]] = []
    if str(default_question or "").strip():
        answer_stages.append(
            {
                "stage": "initial_user_prompt",
                "role": "user",
                "label": "Initiale Analysefrage",
                "text": str(default_question).strip(),
            }
        )
    if str(initial_answer or "").strip():
        answer_stages.append(
            {
                "stage": "initial_analysis",
                "role": "assistant",
                "label": "Initiale Analyse",
                "text": str(initial_answer).strip(),
            }
        )

    return EvalResult(
        test_id=cfg.test_id,
        agent="rag",
        provider=cfg.model.provider,
        model=cfg.model.model,
        planner_provider=None,
        planner_model=None,
        question=cfg.question,
        answer=answer,
        ground_truth=cfg.ground_truth,
        t_initial_analysis_s=round(t_initial, 3),
        t_question_s=0.0,
        t_total_s=round(t_initial, 3),
        prompt_tokens=usage.prompt_tokens,
        completion_tokens=usage.completion_tokens,
        total_tokens=usage.total_tokens,
        cost_usd=round(usage.cost_usd, 6),
        requested_question_ignored=True,
        effective_question=str(default_question or ""),
        answer_initial=str(initial_answer or ""),
        answer_stages=answer_stages,
        notes=_append_note(
            "",
            "RAG-UI-Emulation aktiv: konfigurierte Frage wurde ignoriert; stattdessen nur die automatische Initialanalyse des RAG-Agenten ausgeführt.",
        ),
    )


# ---------------------------------------------------------------------------
# Hauptfunktion (Bibliotheks-API)
# ---------------------------------------------------------------------------

def run_eval(cfg: EvalConfig) -> EvalResult:
    """Führt einen Testfall aus und gibt das EvalResult zurück."""
    _set_api_key(cfg.api_key_file, cfg.model.provider)
    if cfg.model.planner_provider:
        _set_api_key(cfg.api_key_file, cfg.model.planner_provider)
    if cfg.model.judge_provider:
        _set_api_key(cfg.api_key_file, cfg.model.judge_provider)

    agent = (cfg.agent or "kg").lower().strip()
    if agent == "kg":
        result = _run_kg_eval(cfg)
    elif agent == "rag":
        result = _run_rag_eval(cfg)
    else:
        raise ValueError(f"Unbekannter Agent: '{agent}'. Erlaubt: kg, rag")

    return _run_llm_judge(cfg, result)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Headless Eval-Runner für ExcH-KG-Agent und ExcH-RAG-Agent.\n\n"
            "Empfohlener Start: --from-config Evaluation/configs/TC-001_kg.json\n"
            "Alternativ: alle Parameter einzeln angeben."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── Hauptmodus: JSON-Config ──────────────────────────────────────────────
    p.add_argument(
        "--from-config", dest="from_config", metavar="JSON_DATEI",
        help="Lädt alle Parameter aus einer JSON-Config-Datei (empfohlen).",
    )

    # ── Fallback: Einzelne CLI-Argumente ─────────────────────────────────────
    p.add_argument("--agent", choices=["kg", "rag"], help="Welcher Agent (kg oder rag)")
    p.add_argument("--event-json", dest="event_json", help="Pfad zur Event-JSON")
    p.add_argument("--question", help="Frage an den Agenten")
    p.add_argument("--provider", default="groq",
                   help="LLM-Provider: openai | anthropic | groq | ollama | google | azure_openai")
    p.add_argument("--model", default="llama-3.3-70b-versatile", help="Modellname")
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--planner-provider", dest="planner_provider", default=None)
    p.add_argument("--planner-model", dest="planner_model", default=None)
    p.add_argument("--judge-provider", dest="judge_provider", default=None)
    p.add_argument("--judge-model", dest="judge_model", default=None)
    p.add_argument("--judge-temperature", dest="judge_temperature", type=float, default=None)
    p.add_argument("--kg-ttl", dest="kg_ttl", default="", help="KG-TTL-Pfad (KG-Agent)")
    p.add_argument("--xml-path", dest="xml_path", default="", help="PLCOpenXML-Pfad (RAG-Agent)")
    p.add_argument("--pipeline-config", dest="pipeline_config", default="")
    p.add_argument("--api-key-file", dest="api_key_file", default="")
    p.add_argument("--test-id", dest="test_id", default="")
    p.add_argument("--ground-truth", dest="ground_truth", default=None)
    p.add_argument("--pricing-json", dest="pricing_json", default=None,
                   help='JSON-Datei mit {"input_per_1k": ..., "output_per_1k": ...}')
    p.add_argument("--judge-pricing-json", dest="judge_pricing_json", default=None,
                   help='JSON-Datei mit {"input_per_1k": ..., "output_per_1k": ...} für den Judge')
    p.add_argument("--results-dir", dest="results_dir", default=DEFAULT_RESULTS_DIR,
                   help=f"Ausgabeverzeichnis (Standard: {DEFAULT_RESULTS_DIR})")
    p.add_argument("--out-json", dest="out_json", default=None,
                   help="Expliziter Ausgabepfad; leer = auto-generiert in results_dir")
    p.add_argument("--top-k", dest="top_k", type=int, default=6)
    return p


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    # ── Modus 1: Aus JSON-Config laden ──────────────────────────────────────
    if args.from_config:
        cfg = EvalConfig.from_json_file(args.from_config)
        # CLI-Flags überschreiben Config-Werte wenn explizit angegeben
        if args.out_json:
            cfg.out_json = args.out_json
        if args.results_dir != DEFAULT_RESULTS_DIR:
            cfg.results_dir = args.results_dir
        if args.judge_provider is not None:
            cfg.model.judge_provider = args.judge_provider
        if args.judge_model is not None:
            cfg.model.judge_model = args.judge_model
        if args.judge_temperature is not None:
            cfg.model.judge_temperature = args.judge_temperature
        if args.judge_pricing_json:
            cfg.judge_pricing = _load_json(args.judge_pricing_json)

    # ── Modus 2: Direkte CLI-Argumente ───────────────────────────────────────
    else:
        if not args.agent:
            parser.error("--agent ist erforderlich wenn --from-config nicht angegeben ist.")
        if not args.event_json:
            parser.error("--event-json ist erforderlich wenn --from-config nicht angegeben ist.")
        if not args.question:
            parser.error("--question ist erforderlich wenn --from-config nicht angegeben ist.")

        pricing = _load_json(args.pricing_json) if args.pricing_json else None
        judge_pricing = _load_json(args.judge_pricing_json) if args.judge_pricing_json else None

        cfg = EvalConfig(
            agent=args.agent,
            event_json_path=args.event_json,
            question=args.question,
            model=ModelConfig(
                provider=args.provider,
                model=args.model,
                temperature=args.temperature,
                planner_provider=args.planner_provider,
                planner_model=args.planner_model,
                judge_provider=args.judge_provider,
                judge_model=args.judge_model,
                judge_temperature=args.judge_temperature,
            ),
            kg_ttl_path=args.kg_ttl,
            xml_path=args.xml_path,
            pipeline_config=args.pipeline_config,
            api_key_file=args.api_key_file,
            test_id=args.test_id,
            ground_truth=args.ground_truth,
            pricing=pricing,
            judge_pricing=judge_pricing,
            top_k=args.top_k,
            results_dir=args.results_dir,
            out_json=args.out_json or "",
        )

    # ── Eval ausführen ───────────────────────────────────────────────────────
    print(
        f"[eval_runner] Starte:\n"
        f"  test_id  = {cfg.test_id or '(keiner)'}\n"
        f"  agent    = {cfg.agent}\n"
        f"  provider = {cfg.model.provider}  model = {cfg.model.model}\n"
        f"  frage    = {cfg.question[:80]}{'...' if len(cfg.question) > 80 else ''}"
    )

    result = run_eval(cfg)

    # ── Ausgabe ──────────────────────────────────────────────────────────────
    out_path = cfg.resolve_out_json()
    result.save(out_path)

    print("\n--- EVAL RESULT (Zusammenfassung) ---")
    summary = {
        k: v
        for k, v in result.as_dict().items()
        if k not in {"answer", "answer_initial", "answer_auto_followup", "answer_stages"}
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nAntwort ({len(result.answer)} Zeichen):")
    print(result.answer[:500] + ("..." if len(result.answer) > 500 else ""))


if __name__ == "__main__":
    main()
