from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

@dataclass
class AgentResult:
    status: str
    finished_at_utc: str
    diagnosis: str
    next_steps: List[str]
    proposed_actions: List[Dict[str, Any]]  # später: Aktionen, die C++ in CommandForces übersetzen kann
    debug: Dict[str, Any]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def handle_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Zentrale Entry-Funktion fürs Core-Modul.
    Erwartet ein Event-Dict, z.B.:
      {
        "type": "evUnknownFM",
        "ts_ticks": 123456789,
        "payload": { "correlationId": "...", "processName": "...", "summary": "..." }
      }
    Gibt ein AgentResult als dict zurück.
    """
    ev_type = str(event.get("type", "unknown"))
    payload = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}

    if ev_type in ("evUnknownFM", "evAgentStart"):
        return _handle_unknown_fm(event, payload)

    # Default-Fallback (falls du später weitere Events erlaubst)
    res = AgentResult(
        status="ok",
        finished_at_utc=_utc_now_iso(),
        diagnosis=f"Event type not implemented: {ev_type}",
        next_steps=[
            "This event type is not supported by the agent yet.",
            "Extend excH_kg_agent_core.handle_event(...) with a handler for this type.",
        ],
        proposed_actions=[],
        debug={"event": event},
    )
    return asdict(res)


def _handle_unknown_fm(event: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    corr = str(payload.get("correlationId", ""))
    process = str(payload.get("processName", ""))
    summary = str(payload.get("summary", ""))

    # MVP: “so tun als wäre Agent gelaufen” -> strukturierte Ausgabe
    # Später kannst du hier:
    # - KG_Interface abfragen
    # - Heuristiken anwenden
    # - Actions erzeugen, die C++ dann ausführt
    diagnosis = (
        "No FailureMode candidates found in KG for the interrupted skill. "
        "Manual user guidance required."
    )

    next_steps = [
        "Check whether the interruptedSkill is present in the KG with correct IRI.",
        "Validate that preventsFunction links exist for FailureModes to that Function.",
        "If the KG is incomplete: create/update FailureMode + MonitoringAction + SystemReaction entries.",
        "Optionally store this occurrence as UnknownFailure in the KG (ingestion step).",
    ]

    proposed_actions = [
        # Das ist absichtlich generisch: C++ kann später daraus CommandForces bauen
        {
            "kind": "noop",
            "label": "No automatic PLC action in MVP",
            "details": "MVP mode: user confirms to continue.",
        }
    ]

    res = AgentResult(
        status="ok",
        finished_at_utc=_utc_now_iso(),
        diagnosis=diagnosis,
        next_steps=next_steps,
        proposed_actions=proposed_actions,
        debug={
            "correlationId": corr,
            "processName": process,
            "summary": summary,
            "event": event,
        },
    )
    return asdict(res)
