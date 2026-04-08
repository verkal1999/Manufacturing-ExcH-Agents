from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from collections import Counter
from typing import Any, Dict, Optional, List, Set, Tuple


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _normalize_event_input(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Akzeptiert:
      A) direkt ein Event: {"type": ..., "payload": {...}}
      B) result_json: {"continue": ..., "event": {...}, "agent_result": ...}
    Gibt immer das Event-Dict zurÃ¼ck.
    """
    if "event" in obj and isinstance(obj["event"], dict):
        return obj["event"]
    return obj


def _get_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _pick(payload: Dict[str, Any], keys: list[str]) -> str:
    for k in keys:
        v = payload.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _snapshot_get_var(plc_snapshot: Any, name_candidates: list[str]) -> Optional[Any]:
    """Liest eine Variable aus plcSnapshot.vars.

    plcSnapshot.vars ist bei dir eine Liste wie:
      {"id":"OPCUA.lastExecutedSkill","t":"string","v":"TestSkill2"}

    Match: exakter Name oder Suffix.
    """
    snap = plc_snapshot if isinstance(plc_snapshot, dict) else {}
    vars_list = snap.get("vars")
    if not isinstance(vars_list, list):
        return None

    for item in vars_list:
        if not isinstance(item, dict):
            continue
        _id = str(item.get("id", ""))
        for cand in name_candidates:
            if _id == cand or _id.endswith(cand):
                return item.get("v")
    return None


@dataclass
class IncidentContext:
    correlationId: str = ""
    processName: str = ""
    summary: str = ""
    triggerEvent: str = ""  # z.B. evD2

    lastSkill: str = ""  # z.B. TestSkill3
    plcSnapshot: Any = None

    lastGEMMAStateBeforeFailure: str = ""  # z.B. F1

    kg_ttl_path: str = ""
    project_root: str = ""
    agent_result: Any = None

    triggerD2: Optional[bool] = None

    @staticmethod
    def from_input(obj: Dict[str, Any]) -> "IncidentContext":
        event = _normalize_event_input(obj)
        payload = _get_payload(event)

        plc_snapshot = payload.get("plcSnapshot") or payload.get("snapShot") or payload.get("snapshot")

        corr = _pick(payload, ["correlationId", "corr", "correlation_id"])
        proc = _pick(payload, ["processName", "process", "lastProcessName"])
        summ = _pick(payload, ["summary", "summary_text", "message", "error"])
        trig_evt = _pick(payload, ["triggerEvent", "event", "trigger"])

        last = _pick(payload, ["lastSkill", "lastSkillName", "interruptedSkill", "interruptedSkillName"])

        if not last:
            v = _snapshot_get_var(plc_snapshot, ["lastExecutedSkill", "OPCUA.lastExecutedSkill"])
            if v is not None:
                last = str(v)

        if not proc:
            v = _snapshot_get_var(plc_snapshot, ["lastExecutedProcess", "OPCUA.lastExecutedProcess"])
            if v is not None:
                proc = str(v)

        trigger_d2_val: Optional[bool] = None
        vtd2 = _snapshot_get_var(plc_snapshot, ["TriggerD2", "OPCUA.TriggerD2"])
        if isinstance(vtd2, bool):
            trigger_d2_val = vtd2
        elif vtd2 is not None:
            s = str(vtd2).strip().lower()
            if s in {"true", "1"}:
                trigger_d2_val = True
            elif s in {"false", "0"}:
                trigger_d2_val = False

        last_gemma = ""
        vgemma = _snapshot_get_var(
            plc_snapshot,
            [
                "LastGEMMAStateBeforeFailure",
                "OPCUA.LastGEMMAStateBeforeFailure",
                "LastGemmaStateBeforeFailure",
                "OPCUA.LastGemmaStateBeforeFailure",
            ],
        )
        if vgemma is not None:
            last_gemma = str(vgemma).strip()

        return IncidentContext(
            correlationId=corr,
            processName=proc,
            summary=summ,
            triggerEvent=trig_evt,
            lastSkill=last,
            plcSnapshot=plc_snapshot,
            lastGEMMAStateBeforeFailure=last_gemma,
            kg_ttl_path=_pick(payload, ["kg_ttl_path", "kgTtlPath", "kg_path", "kgPath", "ttl_path", "ttlPath"]),
            project_root=_pick(payload, ["project_root", "projectRoot"]),
            agent_result=_as_dict(obj).get("agent_result"),
            triggerD2=trigger_d2_val,
        )

    @staticmethod
    def from_event(event: Dict[str, Any]) -> "IncidentContext":
        """Alias fÃ¼r UI-KompatibilitÃ¤t."""
        return IncidentContext.from_input(event)


def _shorten_json(x: Any, max_chars: int = 2200) -> str:
    if x is None:
        return ""
    try:
        return json.dumps(x, ensure_ascii=False)[:max_chars]
    except Exception:
        return str(x)[:max_chars]


def _safe_tool_exec(session: "ExcHChatBotSession", tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        res = session.bot.registry.execute(tool_name, args)
    except Exception as e:
        return {"error": f"{tool_name}: {e}"}
    if isinstance(res, dict):
        return res
    return {"result": res}


def _pick_primary_trigger_setter(core: Dict[str, Any]) -> Dict[str, Any]:
    setters = core.get("trigger_setters") if isinstance(core.get("trigger_setters"), list) else []
    if not setters:
        return {}
    with_true = [s for s in setters if isinstance(s, dict) and s.get("snips_TRUE")]
    chosen = with_true[0] if with_true else setters[0]
    snip = ""
    if isinstance(chosen, dict):
        st = chosen.get("snips_TRUE")
        if isinstance(st, list) and st:
            snip = str(st[0].get("snippet", ""))[:500]
    return {
        "pou_name": chosen.get("pou_name", "") if isinstance(chosen, dict) else "",
        "snippet_true": snip,
    }


def _compact_requirement_paths(req: Dict[str, Any], max_paths: int = 2, max_rows: int = 14) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    paths = req.get("paths") if isinstance(req.get("paths"), list) else []
    for p in paths[:max_paths]:
        if not isinstance(p, dict):
            continue
        rows = p.get("rows") if isinstance(p.get("rows"), list) else []
        compact_rows = []
        for r in rows[:max_rows]:
            if not isinstance(r, dict):
                continue
            compact_rows.append(
                {
                    "token": r.get("Port/Variable", ""),
                    "value": r.get("Wert", ""),
                    "pou": r.get("POU", ""),
                    "reason": r.get("Herleitung", ""),
                }
            )
        out.append(
            {
                "path_id": p.get("path_id", ""),
                "path_expr": p.get("path_expr", ""),
                "rows": compact_rows,
            }
        )
    return out


def _build_evd2_compact_bundle(
    *,
    ctx: "IncidentContext",
    core: Dict[str, Any],
    unified: Dict[str, Any],
    req: Dict[str, Any],
    path_trace: Dict[str, Any],
) -> Dict[str, Any]:
    primary_setter = _pick_primary_trigger_setter(core)
    uctx = unified.get("context", {}) if isinstance(unified.get("context"), dict) else {}
    dominant_if = unified.get("dominant_inner_if", {}) if isinstance(unified.get("dominant_inner_if"), dict) else {}
    dominant_assign = (
        unified.get("dominant_true_assignment", {})
        if isinstance(unified.get("dominant_true_assignment"), dict)
        else {}
    )
    origin = unified.get("origin", {}) if isinstance(unified.get("origin"), dict) else {}

    missing: List[str] = []
    for name, data in {
        "evd2_diagnoseplan": core,
        "evd2_unified_trace": unified,
        "evd2_requirement_paths": req,
        "evd2_path_trace": path_trace,
    }.items():
        if isinstance(data, dict) and data.get("error"):
            missing.append(name)

    status = "ok"
    if len(missing) == 4:
        status = "error"
    elif missing:
        status = "partial"

    return {
        "schema_version": "evd2_compact_v1",
        "status": status,
        "missing_tools": missing,
        "context": {
            "trigger_event": ctx.triggerEvent or "evD2",
            "last_skill": ctx.lastSkill,
            "last_gemma_state_before_failure": ctx.lastGEMMAStateBeforeFailure,
            "trigger_d2_snapshot": ctx.triggerD2,
        },
        "trigger_chain": {
            "trigger_var": "OPCUA.TriggerD2",
            "primary_setter_pou": primary_setter.get("pou_name", ""),
            "primary_setter_snippet_true": primary_setter.get("snippet_true", ""),
        },
        "state_chain": {
            "target_state": "D2",
            "target_var": uctx.get("target_var", ""),
            "target_pou": uctx.get("pou_name", ""),
            "dominant_true_assignment_line": dominant_assign.get("line_no"),
            "dominant_if_expr": dominant_if.get("expr", ""),
            "truth_path_count": len(unified.get("path_reports", []) if isinstance(unified.get("path_reports"), list) else []),
        },
        "bridge_chain": {
            "suspected_input_for_d2": core.get("inferred_d2_driver_signal", ""),
            "resolved_caller_pou": origin.get("caller_pou_name", ""),
            "resolved_caller_port": origin.get("caller_port_name", ""),
        },
        "requirements_preview": _compact_requirement_paths(req),
        "path_trace_preview": {
            "selected_state_path": str(_as_dict(path_trace.get("point_3_path_trace")).get("selected_state_path", "")),
            "state_fit_best_paths": _as_dict(path_trace.get("point_3_path_trace")).get("state_fit", {}).get("best_paths", [])
            if isinstance(_as_dict(path_trace.get("point_3_path_trace")).get("state_fit"), dict)
            else [],
            "skill_expr": _as_dict(_as_dict(path_trace.get("point_3_path_trace")).get("skill_trace")).get("skill_expr", ""),
        },
    }


def _sparql_escape_text(text: str) -> str:
    return str(text or "").replace("\\", "\\\\").replace('"', '\\"')


def _extract_var_declaration_block(header_text: str, max_chars: int = 3500) -> str:
    header = str(header_text or "")
    if not header:
        return ""

    lines = header.splitlines()
    blocks: List[str] = []
    in_var_block = False

    for raw in lines:
        line = raw.rstrip()
        upper = line.strip().upper()
        if upper.startswith("VAR"):
            in_var_block = True
            blocks.append(line)
            continue
        if in_var_block:
            blocks.append(line)
            if upper.startswith("END_VAR"):
                in_var_block = False

    if not blocks:
        fallback = [ln.rstrip() for ln in lines if ":" in ln and ";" in ln]
        blocks = fallback[:80]

    text = "\n".join(blocks).strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "\n... (gekuerzt)"
    return text


def _pick_focus_pou_for_solution(point1: Dict[str, Any], point3: Dict[str, Any]) -> str:
    counts: Counter[str] = Counter()

    selected_rows = point3.get("selected_state_path_rows")
    if isinstance(selected_rows, list):
        for row in selected_rows:
            if not isinstance(row, dict):
                continue
            pou = str(row.get("pou", "")).strip()
            if pou and pou != "-":
                counts[pou] += 2

    trigger_chain = point1.get("condition_chain")
    if isinstance(trigger_chain, dict):
        paths = trigger_chain.get("paths")
        if isinstance(paths, list):
            for p in paths:
                if not isinstance(p, dict):
                    continue
                rows = p.get("rows")
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    pou = str(row.get("pou", "")).strip()
                    if pou and pou != "-":
                        counts[pou] += 1

    preferred = "FB_Automatikbetrieb_F1"
    # Fuer die evD2-Ursachenkette priorisieren wir bewusst den Upstream-FB.
    if preferred:
        return preferred

    setter_pou = str(point1.get("pou_name", "")).strip()
    if counts:
        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        for pou, _ in ranked:
            if pou != setter_pou:
                return pou
        return ranked[0][0]

    return preferred


def _fetch_pou_code_context_from_kg(pou_name: str) -> Dict[str, Any]:
    try:
        from msrguard.chatbot_core import sparql_select_raw  # type: ignore
    except Exception:
        from chatbot_core import sparql_select_raw  # type: ignore

    name = str(pou_name or "").strip()
    if not name:
        return {"error": "POU-Name fuer SPARQL ist leer."}

    esc = _sparql_escape_text(name)
    q1 = f"""
    SELECT ?gesuchtePOU ?gesuchterCode ?HeaderzumCode WHERE {{
      ?gesuchtePOU a ag:class_POU ;
                   dp:hasPOUName "{esc}" ;
                   dp:hasPOUCode ?gesuchterCode .
      OPTIONAL {{ ?gesuchtePOU dp:hasPOUDeclarationHeader ?HeaderzumCode . }}
    }}
    """
    rows = sparql_select_raw(q1, max_rows=5)
    query_used = q1

    if not rows:
        q2 = f"""
        SELECT ?gesuchtePOU ?gesuchterCode ?HeaderzumCode WHERE {{
          ?gesuchtePOU a ag:class_POU ;
                       ag:dp_hasPOUName "{esc}" ;
                       ag:dp_hasPOUCode ?gesuchterCode .
          OPTIONAL {{ ?gesuchtePOU ag:dp_hasPOUDeclarationHeader ?HeaderzumCode . }}
        }}
        """
        rows = sparql_select_raw(q2, max_rows=5)
        query_used = q2

    if not rows:
        return {
            "error": f"Kein SPARQL-Treffer fuer POU '{name}'.",
            "query": query_used,
            "pou_name": name,
        }

    row = rows[0] if isinstance(rows[0], dict) else {}
    code = str(row.get("gesuchterCode", "") or row.get("code", "") or "")
    header = str(row.get("HeaderzumCode", "") or row.get("header", "") or "")
    return {
        "pou_name": name,
        "pou_uri": str(row.get("gesuchtePOU", "") or row.get("pou", "") or ""),
        "code": code,
        "header": header,
        "query": query_used,
        "rows": len(rows),
    }


def _question_wants_prevention_suggestions(user_text: str) -> bool:
    q = str(user_text or "").lower()
    keys = [
        "maßnahme",
        "maßnahmen",
        "massnahme",
        "massnahmen",
        "lösung",
        "loesung",
        "vorschlag",
        "verhindern",
        "prävent",
        "praevent",
        "fix",
        "beheben",
        "abstellen",
    ]
    return any(k in q for k in keys)


def _collect_trigger_path_rows(point1: Dict[str, Any], max_rows_per_path: int = 80) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    trigger_chain = _as_dict(point1.get("condition_chain"))
    paths = trigger_chain.get("paths") if isinstance(trigger_chain.get("paths"), list) else []
    for p_idx, p in enumerate(paths, start=1):
        if not isinstance(p, dict):
            continue
        rows = p.get("rows") if isinstance(p.get("rows"), list) else []
        for r_idx, row in enumerate(rows[:max_rows_per_path], start=1):
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["_path_idx"] = p_idx
            item["_row_idx"] = r_idx
            out.append(item)
    return out


def _extract_root_anchor_row(point1: Dict[str, Any], point3: Dict[str, Any]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = _collect_trigger_path_rows(point1)

    selected_rows = point3.get("selected_state_path_rows")
    if isinstance(selected_rows, list):
        for idx, row in enumerate(selected_rows, start=1):
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["_path_idx"] = 99
            item["_row_idx"] = idx
            rows.append(item)

    if not rows:
        return {}

    def score(row: Dict[str, Any]) -> int:
        token = str(row.get("token", "")).strip().lower()
        pou = str(row.get("pou", "")).strip().lower()
        value = str(row.get("value", "")).strip().lower()
        assignment = str(row.get("assignment", "")).strip().lower()
        source = str(row.get("source", "")).strip().lower()
        reason = str(row.get("reason", "")).strip().lower()
        row_idx = int(row.get("_row_idx", 0) or 0)

        s = 0
        if "default" in assignment or "default" in source or "default" in reason:
            s += 60
        if "fb_automatikbetrieb_f1" in pou:
            s += 45
        if token in {"periodicfaultperiod", "faultpulse"}:
            s += 30
        if token == "periodicfaultperiod" and "t#60" in value:
            s += 80
        s += min(20, row_idx)
        return s

    ranked = sorted(rows, key=score, reverse=True)
    return ranked[0] if ranked else {}


def _extract_assignment_bridges_from_rows(rows: List[Dict[str, Any]], max_items: int = 10) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for row in rows:
        assignment = str(row.get("assignment", "")).strip()
        if not assignment:
            continue
        if "<=" not in assignment and "=>" not in assignment:
            continue

        normalized = assignment.split("[", 1)[0].strip()
        pou = str(row.get("pou", "")).strip() or "-"
        key = (pou, normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append({"pou": pou, "assignment": normalized})
        if len(out) >= max_items:
            break
    return out


def _build_causal_sequence_summary(point1: Dict[str, Any], point3: Dict[str, Any]) -> Dict[str, Any]:
    rows = _collect_trigger_path_rows(point1)
    anchor = _extract_root_anchor_row(point1, point3)
    bridges = _extract_assignment_bridges_from_rows(rows, max_items=8)

    present = {str(r.get("token", "")).strip().lower() for r in rows if isinstance(r, dict)}
    ordered = [
        "periodicfaultperiod",
        "tper.pt",
        "tper.q",
        "rper.q",
        "pper.q",
        "stoerung_erkannt",
        "auto_stoerung",
        "d2",
        "diagnose_gefordert",
        "rtreq.q",
    ]

    chain: List[str] = []
    anchor_token = str(anchor.get("token", "")).strip()
    if anchor_token:
        chain.append(anchor_token)
    for tk in ordered:
        if tk in present:
            pretty = next((str(r.get("token", "")).strip() for r in rows if str(r.get("token", "")).strip().lower() == tk), tk)
            if pretty and pretty not in chain:
                chain.append(pretty)
    if "OPCUA.TriggerD2" not in chain:
        chain.append("OPCUA.TriggerD2")

    return {"anchor": anchor, "bridges": bridges, "chain": chain}


def _extract_periodicity_evidence(rows: List[Dict[str, Any]], max_items: int = 12) -> List[str]:
    keep_tokens = {
        "periodicfaultperiod",
        "tper.pt",
        "tper.in",
        "tper.q",
        "rper.q",
        "pper.q",
    }
    out: List[str] = []
    seen: Set[str] = set()

    for row in rows:
        token = str(row.get("token", "")).strip()
        value = str(row.get("value", "")).strip()
        pou = str(row.get("pou", "")).strip() or "-"
        phase = str(row.get("phase", "")).strip()
        assignment = str(row.get("assignment", "")).strip()
        source = str(row.get("source", "")).strip()
        reason = str(row.get("reason", "")).strip()

        token_l = token.lower()
        assignment_l = assignment.lower()
        source_l = source.lower()
        reason_l = reason.lower()

        keep = False
        if token_l in keep_tokens:
            keep = True
        if "tper(" in assignment_l or "tper(" in source_l:
            keep = True
        if "not tper.q" in assignment_l or "not tper.q" in source_l:
            keep = True
        if "default" in assignment_l or "default" in source_l or "default" in reason_l:
            keep = keep or (token_l in {"periodicfaultperiod", "faultpulse"})
        if not keep:
            continue

        parts: List[str] = []
        if phase:
            parts.append(f"phase={phase}")
        parts.append(f"token={token or '?'}")
        if value:
            parts.append(f"value={value}")
        parts.append(f"pou={pou}")
        if assignment:
            parts.append(f"assignment={assignment}")
        if source:
            parts.append(f"source={source}")
        line = " | ".join(parts)
        if line in seen:
            continue
        seen.add(line)
        out.append(line)
        if len(out) >= max_items:
            break
    return out


def _build_llm_prevention_suggestions(
    session: "ExcHChatBotSession",
    diagnoseplan: Dict[str, Any],
    analysis_text: str,
) -> Dict[str, Any]:
    path_trace = _as_dict(diagnoseplan.get("path_trace"))
    point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
    point3 = _as_dict(path_trace.get("point_3_path_trace"))

    focus_candidates: List[str] = []
    focus_candidates.append(_pick_focus_pou_for_solution(point1, point3))
    setter_pou = str(point1.get("pou_name", "")).strip()
    if setter_pou:
        focus_candidates.append(setter_pou)
    focus_candidates.append("FB_Diagnose_D2")

    seen: Set[str] = set()
    dedup_focus: List[str] = []
    for cand in focus_candidates:
        c = str(cand or "").strip()
        if not c or c in seen:
            continue
        seen.add(c)
        dedup_focus.append(c)

    pou_ctx: Dict[str, Any] = {"error": "Keine gueltige POU fuer Loesungsvorschlaege."}
    focus_pou = dedup_focus[0] if dedup_focus else ""
    for cand in dedup_focus:
        res = _fetch_pou_code_context_from_kg(cand)
        if not res.get("error"):
            pou_ctx = res
            focus_pou = cand
            break
        pou_ctx = res
    if pou_ctx.get("error"):
        return pou_ctx

    code = str(pou_ctx.get("code", "") or "")
    header = str(pou_ctx.get("header", "") or "")
    var_decl = _extract_var_declaration_block(header)
    causal = _build_causal_sequence_summary(point1, point3)
    anchor = _as_dict(causal.get("anchor"))
    bridges = causal.get("bridges") if isinstance(causal.get("bridges"), list) else []
    chain = causal.get("chain") if isinstance(causal.get("chain"), list) else []
    rows = _collect_trigger_path_rows(point1, max_rows_per_path=120)
    periodicity_evidence = _extract_periodicity_evidence(rows, max_items=12)
    chain_summary = _as_dict(_as_dict(point1.get("condition_chain")).get("summary"))
    software_likely = chain_summary.get("software_likely")
    has_hw = chain_summary.get("has_hardware_address")

    analysis_short = analysis_text[:12000]
    header_short = header[:5000]
    var_decl_short = var_decl[:3500]
    code_short = code[:10000]

    system_prompt = (
        "Du bist ein erfahrener SPS-Engineer fuer Root-Cause-Praevention. "
        "Antworte nur auf Deutsch, technisch praezise, ohne Halluzinationen."
    )
    user_prompt = (
        "Ziel:\n"
        "Erarbeite konkrete Loesungsvorschlaege, wie der in der Analyse gefundene Pfad "
        "zu `OPCUA.TriggerD2 := TRUE` zukuenftig verhindert werden kann.\n\n"
        "WICHTIG zur Struktur:\n"
        "1) ZUERST die Fehlerfolge erklaeren (Upstream -> Trigger),\n"
        "2) DANACH Massnahmen vorschlagen.\n\n"
        "Upstream-Anker aus dem Trace:\n"
        f"- Token: {anchor.get('token', '-')}\n"
        f"- Wert: {anchor.get('value', '-')}\n"
        f"- POU: {anchor.get('pou', '-')}\n"
        f"- Assignment/Quelle: {anchor.get('assignment', '') or anchor.get('source', '') or '-'}\n\n"
        "Kompakte Fehlerfolge (aus Trace):\n"
        f"- {' -> '.join(str(x) for x in chain)}\n\n"
        "POU-/Programm-Wechsel ueber Assignments (aus Triggerpfad):\n"
        + ("\n".join(f"- ({b.get('pou','-')}) {b.get('assignment','')}" for b in bridges) if bridges else "- keine")
        + "\n\n"
        "Periodizitaets-Hinweise aus dem Trace (rohe Evidenz):\n"
        + ("\n".join(f"- {x}" for x in periodicity_evidence) if periodicity_evidence else "- keine")
        + "\n\n"
        f"Pfad-Bewertung: software_likely={software_likely}, has_hardware_address={has_hw}\n\n"
        "Analyse (Auszug):\n"
        f"{analysis_short}\n\n"
        "Zusatzkontext aus dem KG (SPARQL bei bekanntem FB-Namen):\n"
        "```sparql\n"
        f"{str(pou_ctx.get('query', '')).strip()}\n"
        "```\n\n"
        f"POU: {focus_pou}\n\n"
        "POU Declaration Header:\n"
        "```pascal\n"
        f"{header_short}\n"
        "```\n\n"
        "Extrahierte Variablendeklaration:\n"
        "```pascal\n"
        f"{var_decl_short}\n"
        "```\n\n"
        "POU Code:\n"
        "```pascal\n"
        f"{code_short}\n"
        "```\n\n"
        "Bitte liefere:\n"
        "0) Eine kurze Fehlerfolge-Erklaerung in 4-8 Schritten (ohne Massnahmen).\n"
        "1) Eine explizite Periodizitaetsbewertung:\n"
        "   - Leite aus der Evidenz ab, ob eine periodische Ausloesung plausibel ist.\n"
        "   - Falls plausibel: nenne abgeleitete Periode (z.B. aus PT-Wert) und Unschaerfen.\n"
        "   - Falls nicht plausibel: benenne fehlende Evidenz.\n"
        "2) Drei umsetzbare Gegenmassnahmen mit Prioritaet (hoch/mittel/niedrig).\n"
        "3) Fuer jede Massnahme: konkrete ST-Aenderung (Patch-Style), warum sie wirkt, Nebenwirkungen.\n"
        "4) Eine bevorzugte Empfehlung mit kurzer Begruendung.\n"
        "5) Eine Watchlist mit 6-10 Signalen fuer Online-Validierung nach dem Fix.\n"
        "Regel: Keine Hardware-/OPCUA-Massnahmen vorschlagen, wenn der Triggerpfad software_likely=true und has_hardware_address=false.\n"
    )

    try:
        llm_answer = session.bot.llm(system_prompt, user_prompt)
    except Exception as e:
        return {
            "error": f"LLM konnte keine Loesungsvorschlaege erzeugen: {e}",
            "focus_pou": focus_pou,
            "sparql_query": pou_ctx.get("query", ""),
        }

    return {
        "focus_pou": focus_pou,
        "sparql_query": pou_ctx.get("query", ""),
        "declaration_header": header_short,
        "variable_declaration": var_decl_short,
        "code_excerpt": code_short,
        "llm_answer": llm_answer,
    }


def build_initial_prompt(ctx: IncidentContext, diagnoseplan: Optional[Dict[str, Any]] = None) -> str:
    parts = ["Unbekannter Fehler ist aufgetreten (Unknown Failure Mode)."]

    if ctx.triggerEvent:
        parts.append(f"triggerEvent: {ctx.triggerEvent}")
    if ctx.triggerD2 is not None:
        parts.append(f"snapshot.OPCUA.TriggerD2: {ctx.triggerD2}")
    if ctx.lastGEMMAStateBeforeFailure:
        parts.append(f"snapshot.OPCUA.LastGEMMAStateBeforeFailure: {ctx.lastGEMMAStateBeforeFailure}")
        if (ctx.triggerEvent or "").strip().lower() == "evd2" or ctx.triggerD2 is True:
            parts.append("GEMMA: letzter stabiler Zustand vor Fehler war oben; aktueller Fehlerzustand ist D2 (evD2).")

    if ctx.lastSkill:
        parts.append(f"lastSkill (lastExecutedSkill): {ctx.lastSkill}")
    if ctx.processName:
        parts.append(f"process: {ctx.processName}")
    if ctx.correlationId:
        parts.append(f"correlationId: {ctx.correlationId}")
    if ctx.summary:
        parts.append(f"summary: {ctx.summary}")

    snap_short = _shorten_json(ctx.plcSnapshot, 2000)
    agent_short = _shorten_json(ctx.agent_result, 1800)

    prompt = (
        "Kontext:\n"
        + "\n".join(f"- {p}" for p in parts)
        + (f"\n- plcSnapshot (gekÃ¼rzt): {snap_short}" if snap_short else "")
        + (f"\n- agent_result (gekÃ¼rzt): {agent_short}" if agent_short else "")
    )

    if diagnoseplan is not None:
        prompt_payload = diagnoseplan.get("compact") if isinstance(diagnoseplan, dict) and diagnoseplan.get("compact") else diagnoseplan
        plan_short = json.dumps(prompt_payload, ensure_ascii=False, indent=2)
        if len(plan_short) > 8000:
            plan_short = plan_short[:8000] + "\n... (gekÃ¼rzt)"
        prompt += "\n\nAutomatisch erzeugte Diagnose-Fakten (evD2, compact):\n" + plan_short + "\n"

    prompt += (
        "\n\nAufgabe (wichtig):\n"
        "0) Nutze ausschlieÃŸlich die gelieferten Diagnose-Fakten (compact JSON). Keine erfundenen Zwischenschritte.\n"
        "1) Du musst erklÃ¤ren, WARUM evD2 ausgelÃ¶st wurde. Entscheidend ist, warum OPCUA.TriggerD2 TRUE ging.\n"
        "2) Nutze lastSkill, um zuerst zu finden, in welcher POU / FBInstanz dieser Skill gesetzt wurde (OPCUA.lastExecutedSkill := '...').\n"
        "3) Finde die GEMMA-State-Machine (dp:isGEMMAStateMachine true), identifiziere den Output-Port D2 und wie D2 in MAIN verdrahtet ist.\n"
        "4) Analysiere die D2-Logik im GEMMA-Layer (FBD) als RS-Flipflop: 1. Argument = Set, 2. Argument = Reset.\n"
        "5) Suche danach im Code, wo die EinflussgrÃ¶ÃŸen der Set/Reset-Bedingung gesetzt werden und ob diese durch den lastSkill-Pfad beeinflusst werden.\n\n"
        "GEMMA Hinweis:\n"
        "- Im GEMMA ist typischerweise immer genau 1 Zustand gleichzeitig aktiv (one-hot). "
        "Wenn snapshot.OPCUA.LastGEMMAStateBeforeFailure gesetzt ist (z.B. F1), nutze das als starken Hinweis, "
        "welcher Zweig einer ODER-Set-Bedingung realistisch aktiv war.\n\n"
        "Liefer bitte: konkrete POU-Namen, relevante Code-Snippets und klare nÃ¤chste Debug-Schritte.\n"
    )
    return prompt


class ExcHChatBotSession:
    def __init__(self, bot: Any, ctx: IncidentContext):
        self.bot = bot
        self.ctx = ctx
        self.bootstrap_evd2_plan: Optional[Dict[str, Any]] = None

    def ensure_bootstrap_plan(self) -> Dict[str, Any]:
        """
        FÃ¼hrt immer zuerst die deterministische evD2-Composite-Analyse aus und cached das Ergebnis.
        Fehler werden als {"error": "..."} zurÃ¼ckgegeben, damit der Chat trotzdem weiterlÃ¤uft.
        """
        if self.bootstrap_evd2_plan is not None:
            return self.bootstrap_evd2_plan

        try:
            self.bootstrap_evd2_plan = build_evd2_diagnoseplan(self)
        except Exception as e:
            self.bootstrap_evd2_plan = {"error": str(e)}
        return self.bootstrap_evd2_plan

    def ask(self, user_msg: str, debug: bool = True, *, include_bootstrap: bool = False) -> Dict[str, Any]:
        if _should_build_evd2_plan(self.ctx) and _question_wants_prevention_suggestions(user_msg):
            diagnoseplan = self.ensure_bootstrap_plan()
            if isinstance(diagnoseplan, dict):
                base_analysis = _render_initial_evd2_answer(self.ctx, diagnoseplan)
                suggestion_block = _build_llm_prevention_suggestions(self, diagnoseplan, base_analysis)

                llm_text = str(suggestion_block.get("llm_answer", "") or "").strip()
                llm_err = str(suggestion_block.get("error", "") or "").strip()

                path_trace = _as_dict(diagnoseplan.get("path_trace"))
                point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
                point3 = _as_dict(path_trace.get("point_3_path_trace"))
                causal = _build_causal_sequence_summary(point1, point3)
                chain = causal.get("chain") if isinstance(causal.get("chain"), list) else []
                anchor = _as_dict(causal.get("anchor"))

                lines: List[str] = []
                lines.append("Zuerst die Fehlerfolge (ohne Massnahmen):")
                if anchor:
                    lines.append(
                        f"- Upstream-Anker: `{anchor.get('token', '?')} = {anchor.get('value', '?')}` "
                        f"in `{anchor.get('pou', '?')}`."
                    )
                if chain:
                    lines.append(f"- Kette: {' -> '.join(str(x) for x in chain)}")

                if llm_text:
                    lines.append("")
                    lines.append("LLM-basierte Loesungsvorschlaege zur Vermeidung des Pfads:")
                    lines.append(llm_text)
                else:
                    lines.append("")
                    lines.append(
                        "LLM-basierte Loesungsvorschlaege konnten nicht erzeugt werden: "
                        + (llm_err or "unbekannter Fehler")
                    )

                response: Dict[str, Any] = {"answer": "\n".join(lines).strip()}
                if debug:
                    response["plan"] = {
                        "steps": [
                            {
                                "tool": "deterministic_path_search",
                                "args": _evd2_bootstrap_tool_args(self.ctx),
                            },
                            {
                                "tool": "llm_prevention_suggestions",
                                "args": {
                                    "focus_pou": str(suggestion_block.get("focus_pou", "") or ""),
                                },
                            },
                        ]
                    }
                    response["tool_results"] = {
                        "step_1:deterministic_path_search": diagnoseplan.get("path_trace", {}),
                        "step_2:derived_core": diagnoseplan.get("core", {}),
                        "step_3:derived_requirement_paths": diagnoseplan.get("requirement_paths", {}),
                        "step_4:llm_prevention_suggestions": suggestion_block,
                    }
                return response

        ctx_blob = asdict(self.ctx)
        wrapped = (
            "Incident Kontext (JSON):\n"
            + json.dumps(ctx_blob, ensure_ascii=False, indent=2)
        )

        if include_bootstrap:
            bootstrap = self.ensure_bootstrap_plan()
            wrapped += (
                "\n\nEvD2DiagnosisTool Ergebnis (JSON):\n"
                + json.dumps(bootstrap, ensure_ascii=False, indent=2)
            )

        wrapped += "\n\nUser Frage:\n" + user_msg
        return self.bot.chat(wrapped, debug=debug)


def build_session_from_input(obj: Dict[str, Any]) -> ExcHChatBotSession:
    ctx = IncidentContext.from_input(obj)
    kg_path = ctx.kg_ttl_path or os.environ.get("MSRGUARD_KG_TTL", "")

    if not kg_path:
        raise RuntimeError("Kein KG TTL Pfad gefunden. Setze payload.kg_ttl_path ODER ENV MSRGUARD_KG_TTL.")

    from msrguard.chatbot_core import build_bot
    bot = build_bot(kg_ttl_path=kg_path)
    return ExcHChatBotSession(bot=bot, ctx=ctx)


def _should_build_evd2_plan(ctx: IncidentContext) -> bool:
    if (ctx.triggerEvent or "").strip().lower() == "evd2":
        return True
    if ctx.triggerD2 is True:
        return True
    return False


def _evd2_bootstrap_tool_args(ctx: IncidentContext) -> Dict[str, Any]:
    return {
        "preset": "evd2_bootstrap",
        "last_skill": ctx.lastSkill or "",
        "last_gemma_state": ctx.lastGEMMAStateBeforeFailure or "",
        "process_name": ctx.processName or "",
        "trigger_var": "OPCUA.TriggerD2",
        "state_name": "D2",
        "skill_case": ctx.lastSkill or "TestSkill3",
        "max_depth": 18,
        "assumed_false_states": "D1,D2,D3",
    }


def build_evd2_diagnoseplan(session: ExcHChatBotSession) -> Dict[str, Any]:
    """Deterministische Composite-Analyse fÃ¼r evD2 (ohne LLM)."""
    bootstrap_args = _evd2_bootstrap_tool_args(session.ctx)
    path_trace = _safe_tool_exec(session, "deterministic_path_search", bootstrap_args)
    raw = _as_dict(path_trace.get("raw"))
    core = _as_dict(raw.get("core"))
    unified = _as_dict(raw.get("unified_trace"))
    req = _as_dict(raw.get("requirement_paths"))

    compact = _build_evd2_compact_bundle(
        ctx=session.ctx,
        core=core,
        unified=unified,
        req=req,
        path_trace=path_trace,
    )

    return {
        "pipeline": "evd2_composite_v1",
        "bootstrap_tool": {"tool": "deterministic_path_search", "args": bootstrap_args},
        "core": core,
        "unified_trace": unified,
        "requirement_paths": req,
        "path_trace": path_trace,
        "compact": compact,
    }


def _render_initial_evd2_answer(ctx: IncidentContext, diagnoseplan: Dict[str, Any]) -> str:
    path_trace = _as_dict(diagnoseplan.get("path_trace"))
    point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
    point2 = _as_dict(path_trace.get("point_2_last_skill"))
    point3 = _as_dict(path_trace.get("point_3_path_trace"))

    if not point1 and isinstance(diagnoseplan.get("core"), dict):
        fallback = _pick_primary_trigger_setter(diagnoseplan.get("core") or {})
        point1 = {
            "trigger_var": "OPCUA.TriggerD2",
            "pou_name": fallback.get("pou_name", ""),
            "snippet_true": fallback.get("snippet_true", ""),
            "condition_expr": "",
        }

    trigger_var = str(point1.get("trigger_var", "OPCUA.TriggerD2") or "OPCUA.TriggerD2")
    setter_pou = str(point1.get("pou_name", "") or "-")
    snippet = str(point1.get("snippet_true", "") or "").strip()
    cond_expr = str(point1.get("condition_expr", "") or "").strip()
    trigger_chain = _as_dict(point1.get("condition_chain"))
    trigger_chain_paths = trigger_chain.get("paths") if isinstance(trigger_chain.get("paths"), list) else []
    trigger_chain_summary = _as_dict(trigger_chain.get("summary"))

    last_skill = str(point2.get("last_skill", "") or ctx.lastSkill or "-")
    process_name = str(point2.get("process_name", "") or ctx.processName or "-")
    abort_statement = str(point2.get("abort_statement", "") or "").strip()
    skill_identity = _as_dict(point2.get("skill_identity"))
    skill_interpretation = str(skill_identity.get("interpretation", "") or "").strip()
    class_hits = skill_identity.get("class_skill_iris")
    class_hit_text = ", ".join(class_hits[:3]) if isinstance(class_hits, list) and class_hits else "-"

    skill_trace = _as_dict(point3.get("skill_trace"))
    skill_expr = str(skill_trace.get("skill_expr", "") or "").strip()
    fit = _as_dict(point3.get("state_fit"))
    selected_path = str(point3.get("selected_state_path", "") or "")
    selected_rows = point3.get("selected_state_path_rows") if isinstance(point3.get("selected_state_path_rows"), list) else []
    best_paths = fit.get("best_paths") if isinstance(fit.get("best_paths"), list) else []
    evaluations = fit.get("path_evaluations") if isinstance(fit.get("path_evaluations"), list) else []

    lines: List[str] = []
    causal = _build_causal_sequence_summary(point1, point3)
    anchor = _as_dict(causal.get("anchor"))
    bridges = causal.get("bridges") if isinstance(causal.get("bridges"), list) else []
    chain = causal.get("chain") if isinstance(causal.get("chain"), list) else []

    if anchor:
        lines.append("0) Warum die Fehlerfolge entsteht (Upstream -> Trigger):")
        anchor_token = str(anchor.get("token", "")).strip() or "?"
        anchor_value = str(anchor.get("value", "")).strip() or "?"
        anchor_pou = str(anchor.get("pou", "")).strip() or "?"
        anchor_asg = str(anchor.get("assignment", "") or anchor.get("source", "") or "").strip()
        lines.append(f"- Upstream-Anker: `{anchor_token} = {anchor_value}` in `{anchor_pou}`.")
        if anchor_asg:
            lines.append(f"- Herleitung am Anker: `{anchor_asg}`")
    if chain:
        lines.append(f"- Fehlerfolge kompakt: {' -> '.join(str(x) for x in chain)}")
    if bridges:
        lines.append("- Programm-/POU-Wechsel ueber Assignments:")
        for idx, b in enumerate(bridges[:8], start=1):
            b_pou = str(b.get("pou", "-") or "-")
            b_asg = str(b.get("assignment", "") or "").strip()
            lines.append(f"  - 0.{idx}) ({b_pou}) `{b_asg}`")
    if anchor or chain or bridges:
        lines.append("")

    lines.append(f"1) `{trigger_var} := TRUE` wird in der POU `{setter_pou}` gesetzt.")
    if snippet:
        lines.append("Der relevante Code-Snippet ist:")
        lines.append("```pascal")
        lines.append(snippet)
        lines.append("```")
    if cond_expr:
        lines.append(f"Bedingung: `{cond_expr}`.")

    if trigger_chain_paths:
        lines.append("")
        lines.append("Upstream-Trace der Bedingung (alle Pfade bis Terminator):")
        for p_idx, p in enumerate(trigger_chain_paths, start=1):
            if not isinstance(p, dict):
                continue
            p_expr = str(p.get("path_expr", "") or "")
            lines.append(f"- Pfad 1.{p_idx}: `{p_expr}`")
            for r_idx, row in enumerate((p.get("rows") or [])[:60], start=1):
                if not isinstance(row, dict):
                    continue
                token = str(row.get("token", "") or "")
                value = str(row.get("value", "") or "")
                pou = str(row.get("pou", "") or "-")
                assignment = str(row.get("assignment", "") or "").strip()
                fb_type = str(row.get("fb_type", "") or "").strip()
                fb_desc = str(row.get("fb_type_description", "") or "").strip()

                extras: List[str] = []
                if assignment:
                    extras.append(f"Assignment: `{assignment}`")
                if fb_type:
                    extras.append(f"FB-Typ: `{fb_type}`")
                if fb_desc:
                    extras.append(f"Beschreibung: {fb_desc}")

                if extras:
                    lines.append(f"  - 1.{p_idx}.{r_idx} `{token}` = `{value}` ({pou}) | " + " | ".join(extras))
                else:
                    lines.append(f"  - 1.{p_idx}.{r_idx} `{token}` = `{value}` ({pou})")
            lines.append("")

        origin_assessment = str(trigger_chain_summary.get("origin_assessment", "") or "")
        if origin_assessment:
            lines.append(f"Einschaetzung Trigger-Ursprung: {origin_assessment}")
        if trigger_chain_summary.get("software_likely") is True:
            lines.append("Bewertung: Kein Hardware-Endpunkt im Triggerpfad gefunden -> Ursache wahrscheinlich softwareseitig.")
        elif trigger_chain_summary.get("has_hardware_address") is True:
            lines.append("Bewertung: Triggerpfad enthaelt Hardware-Endpunkt(e).")

    lines.append("")
    lines.append(
        "2) Letzter ausgefuehrter Skill war "
        f"`{last_skill}`; der Prozess `{process_name}` wurde danach durch `evD2` unterbrochen."
    )
    if abort_statement:
        lines.append(abort_statement)
    if skill_interpretation:
        lines.append(f"KG-Einordnung von `{last_skill}`: {skill_interpretation}")
    lines.append(f"`ag:class_Skill` Treffer: {class_hit_text}")

    lines.append("")
    lines.append("3) Rekonstruierter Signal-/Logikpfad (rueckwaerts):")
    if skill_expr:
        lines.append(f"- 3.1) Skill-Path: `{skill_expr}`")
    if best_paths:
        lines.append(f"- 3.2) Konsistente State-Paths: {', '.join(best_paths)}")
    if selected_path:
        lines.append(f"- 3.3) Gewaehlter State-Path: `{selected_path}`")

    for idx, ev in enumerate(evaluations[:3], start=1):
        if not isinstance(ev, dict):
            continue
        p = ev.get("state_path", "?")
        possible = "ja" if bool(ev.get("possible")) else "nein"
        conflicts = ev.get("conflicts") if isinstance(ev.get("conflicts"), list) else []
        hw = ev.get("hardware_addresses") if isinstance(ev.get("hardware_addresses"), list) else []
        hw_text = ", ".join(str(x) for x in hw[:3]) if hw else "-"
        default_flag = "ja" if bool(ev.get("has_default_terminator")) else "nein"
        if conflicts:
            lines.append(f"- 3.4.{idx}) {p}: moeglich={possible}, Konflikt: {conflicts[0]}, HW={hw_text}, Default={default_flag}")
        else:
            lines.append(f"- 3.4.{idx}) {p}: moeglich={possible}, score={ev.get('score', 0)}, HW={hw_text}, Default={default_flag}")

    if selected_rows:
        def _phase_rank_local(phase: str) -> int:
            text = str(phase or "").strip().lower()
            if text == "t0":
                return 0
            if text.startswith("t-"):
                try:
                    return int(text[2:])
                except Exception:
                    return 999
            return 999

        lines.append("- 3.5) Schluesselwerte im gewaehlten Pfad (mit Zeitphase):")
        rows_time = [r for r in selected_rows if isinstance(r, dict)]
        rows_time.sort(key=lambda r: (_phase_rank_local(str(r.get("phase", "t0"))), str(r.get("token", ""))))

        for idx, row in enumerate(rows_time[:14], start=1):
            tok = row.get("token", "")
            val = row.get("value", "")
            pou = row.get("pou", "")
            phase = str(row.get("phase", "t0") or "t0")
            assignment = str(row.get("assignment", "") or "").strip()
            fb_type = str(row.get("fb_type", "") or "").strip()
            fb_desc = str(row.get("fb_type_description", "") or "").strip()
            source = str(row.get("source", "") or "").strip()
            reason = str(row.get("reason", "") or "").strip()

            extras: List[str] = []
            if assignment:
                extras.append(f"Assignment: `{assignment}`")
            if source:
                extras.append(f"Quelle: `{source}`")
            if fb_type:
                extras.append(f"FB-Typ: `{fb_type}`")
            if fb_desc:
                extras.append(f"Beschreibung: {fb_desc}")
            if "default" in source.lower() or "default" in reason.lower():
                extras.append("Default-Wert verwendet")

            if extras:
                lines.append(f"  - 3.5.{idx}) ({phase}) `{tok}` = `{val}` ({pou}) | " + " | ".join(extras))
            else:
                lines.append(f"  - 3.5.{idx}) ({phase}) `{tok}` = `{val}` ({pou})")

    return "\n".join(lines).strip()


def run_initial_analysis(session: ExcHChatBotSession, debug: bool = True) -> Dict[str, Any]:
    diagnoseplan = session.ensure_bootstrap_plan()
    if _should_build_evd2_plan(session.ctx) and isinstance(diagnoseplan, dict):
        answer = _render_initial_evd2_answer(session.ctx, diagnoseplan)
        if answer:
            answer = (
                answer
                + "\n\n4) LLM-basierte Loesungsvorschlaege zur Vermeidung des Pfads:\n"
                + "Auf Nachfrage. Frage z.B.: `Bitte gib Massnahmen zur Vermeidung dieses Pfads.`"
            )

            result: Dict[str, Any] = {"answer": answer}
            if debug:
                result["plan"] = {
                    "steps": [
                        {
                            "tool": "deterministic_path_search",
                            "args": _evd2_bootstrap_tool_args(session.ctx),
                        },
                    ]
                }
                result["tool_results"] = {
                    "step_1:deterministic_path_search": diagnoseplan.get("path_trace", {}),
                    "step_2:derived_core": diagnoseplan.get("core", {}),
                    "step_3:derived_requirement_paths": diagnoseplan.get("requirement_paths", {}),
                }
            return result

    prompt = build_initial_prompt(session.ctx, diagnoseplan=diagnoseplan)
    return session.ask(prompt, debug=debug, include_bootstrap=False)
