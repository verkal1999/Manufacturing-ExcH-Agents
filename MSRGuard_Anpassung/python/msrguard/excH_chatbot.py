from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
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
    lastFinishedSkill: str = ""
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

        last_finished = ""
        vfinished = _snapshot_get_var(plc_snapshot, ["lastFinishedSkill", "OPCUA.lastFinishedSkill"])
        if vfinished is not None:
            last_finished = str(vfinished).strip()

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
            lastFinishedSkill=last_finished,
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


def _sparql_select(query: str, max_rows: int = 20) -> List[Dict[str, Any]]:
    try:
        from msrguard.chatbot_core import sparql_select_raw  # type: ignore
    except Exception:
        from chatbot_core import sparql_select_raw  # type: ignore

    rows = sparql_select_raw(query, max_rows=max_rows)
    return rows if isinstance(rows, list) else []


def _unique_nonempty(values: List[Any]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _snapshot_candidates_for_token(token: str) -> List[str]:
    text = str(token or "").strip()
    if not text:
        return []
    out = [text]
    if "." in text:
        out.append(text.rsplit(".", 1)[-1].strip())
    return _unique_nonempty(out)


def _extract_code_snippets(code: str, terms: List[str], *, radius: int = 2, max_snips: int = 6) -> List[str]:
    src = str(code or "")
    if not src:
        return []

    needles = [str(t or "").strip() for t in terms if str(t or "").strip()]
    if not needles:
        return []

    lines = src.splitlines()
    seen: Set[Tuple[int, int]] = set()
    snippets: List[str] = []

    for idx, line in enumerate(lines):
        lower = line.lower()
        if not any(needle.lower() in lower for needle in needles):
            continue
        start = max(0, idx - radius)
        end = min(len(lines), idx + radius + 1)
        key = (start, end)
        if key in seen:
            continue
        seen.add(key)
        snippet = "\n".join(lines[start:end]).strip()
        if snippet and snippet not in snippets:
            snippets.append(snippet[:1200])
        if len(snippets) >= max_snips:
            break

    return snippets


def _counterpart_sensor_short_name(token: str) -> str:
    short = str(token or "").strip()
    if "." in short:
        short = short.rsplit(".", 1)[-1].strip()
    if re.search(r"_aussen$", short, flags=re.I):
        return re.sub(r"_aussen$", "_innen", short, flags=re.I)
    if re.search(r"_innen$", short, flags=re.I):
        return re.sub(r"_innen$", "_aussen", short, flags=re.I)
    return ""


def _fetch_variable_context_from_kg(var_name: str) -> Dict[str, Any]:
    name = str(var_name or "").strip()
    if not name:
        return {"error": "Variablenname ist leer.", "variable_name": name}

    candidates = _snapshot_candidates_for_token(name)
    var_uri = ""
    find_query = ""

    for cand in candidates:
        esc = _sparql_escape_text(cand)
        q_find = f"""
        SELECT DISTINCT ?var WHERE {{
          ?var a ag:class_Variable ;
               dp:hasVariableName "{esc}" .
        }}
        """
        rows = _sparql_select(q_find, max_rows=5)
        find_query = q_find
        if rows:
            row = rows[0] if isinstance(rows[0], dict) else {}
            var_uri = str(row.get("var", "") or "").strip()
            if var_uri:
                break

    if not var_uri:
        return {
            "error": f"Kein KG-Treffer fuer Variable '{name}'.",
            "variable_name": name,
            "query": find_query,
        }

    q_meta = f"""
    SELECT ?var_name ?var_type ?hw ?opcua_da ?opcua_wa ?ioxml WHERE {{
      OPTIONAL {{ <{var_uri}> dp:hasVariableName ?var_name . }}
      OPTIONAL {{ <{var_uri}> dp:hasVariableType ?var_type . }}
      OPTIONAL {{ <{var_uri}> dp:hasHardwareAddress ?hw . }}
      OPTIONAL {{ <{var_uri}> dp:hasOPCUADataAccess ?opcua_da . }}
      OPTIONAL {{ <{var_uri}> dp:hasOPCUAWriteAccess ?opcua_wa . }}
      OPTIONAL {{ <{var_uri}> dp:ioRawXml ?ioxml . }}
    }}
    """
    meta_rows = _sparql_select(q_meta, max_rows=40)

    q_incoming = f"""
    SELECT ?src_name ?src_expr WHERE {{
      ?assign op:assignsToVariable <{var_uri}> ;
              op:assignsFrom ?src .
      OPTIONAL {{ ?src dp:hasVariableName ?src_name . }}
      OPTIONAL {{ ?src dp:hasExpressionText ?src_expr . }}
    }}
    """
    incoming_rows = _sparql_select(q_incoming, max_rows=40)

    names = _unique_nonempty([r.get("var_name", "") for r in meta_rows if isinstance(r, dict)])
    var_types = _unique_nonempty([r.get("var_type", "") for r in meta_rows if isinstance(r, dict)])
    hardware_addresses = _unique_nonempty([r.get("hw", "") for r in meta_rows if isinstance(r, dict)])
    incoming_sources = _unique_nonempty(
        [
            r.get("src_name", "") or r.get("src_expr", "")
            for r in incoming_rows
            if isinstance(r, dict)
        ]
    )

    return {
        "variable_name": name,
        "variable_uri": var_uri,
        "names": names,
        "variable_types": var_types,
        "hardware_addresses": hardware_addresses,
        "opcua_data_access": _unique_nonempty([r.get("opcua_da", "") for r in meta_rows if isinstance(r, dict)]),
        "opcua_write_access": _unique_nonempty([r.get("opcua_wa", "") for r in meta_rows if isinstance(r, dict)]),
        "incoming_sources": incoming_sources,
        "query_find": find_query,
        "query_meta": q_meta,
        "query_incoming": q_incoming,
    }


def _classify_variable_context(var_ctx: Dict[str, Any]) -> Dict[str, Any]:
    hw_list = [str(x).strip() for x in (var_ctx.get("hardware_addresses") or []) if str(x).strip()]
    type_list = [str(x).strip().upper() for x in (var_ctx.get("variable_types") or []) if str(x).strip()]
    incoming_sources = [str(x).strip() for x in (var_ctx.get("incoming_sources") or []) if str(x).strip()]

    def _has_input_hw(text: str) -> bool:
        t = str(text or "").strip().upper()
        return t.startswith("%I") or bool(re.search(r"(^|\\s)I\\s*\\d", t))

    def _has_output_hw(text: str) -> bool:
        t = str(text or "").strip().upper()
        return t.startswith("%Q") or bool(re.search(r"(^|\\s)Q\\s*\\d", t))

    is_bool = "BOOL" in type_list
    is_input = any(_has_input_hw(x) for x in hw_list)
    is_output = any(_has_output_hw(x) for x in hw_list)
    sim_sources = [src for src in incoming_sources if "_SIM" in src.upper()]

    return {
        "is_bool": is_bool,
        "is_input": is_input,
        "is_output": is_output,
        "is_input_bool_sensor": bool(is_input and is_bool),
        "has_sim_mapping": bool(sim_sources),
        "sim_sources": sim_sources,
    }


def _fetch_pou_input_wiring_from_kg(pou_name: str) -> Dict[str, Any]:
    name = str(pou_name or "").strip()
    if not name:
        return {"error": "POU-Name fuer Wiring-Abfrage ist leer.", "pou_name": name}

    esc = _sparql_escape_text(name)
    q = f"""
    SELECT ?port_name ?src_name ?src_expr WHERE {{
      ?pou a ag:class_POU ;
           dp:hasPOUName "{esc}" ;
           op:hasPort ?port .
      ?port dp:hasPortName ?port_name .
      ?assign a ag:class_ParameterAssignment ;
              op:assignsToPort ?port ;
              op:assignsFrom ?src .
      OPTIONAL {{ ?src dp:hasVariableName ?src_name . }}
      OPTIONAL {{ ?src dp:hasExpressionText ?src_expr . }}
    }}
    ORDER BY ?port_name ?src_name
    """
    rows = _sparql_select(q, max_rows=200)
    wiring: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str]] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        port_name = str(row.get("port_name", "") or "").strip()
        source_name = str(row.get("src_name", "") or row.get("src_expr", "") or "").strip()
        key = (port_name.lower(), source_name.lower(), name.lower())
        if key in seen:
            continue
        seen.add(key)
        wiring.append({"port_name": port_name, "source_name": source_name})

    return {"pou_name": name, "query": q, "wiring": wiring}


def _fetch_method_contexts_for_owner_pou(owner_name: str) -> Dict[str, Any]:
    name = str(owner_name or "").strip()
    if not name:
        return {"error": "Owner-POU-Name fuer Methodenabfrage ist leer.", "owner_name": name}

    esc = _sparql_escape_text(name)
    q = f"""
    SELECT ?method_type ?code ?header WHERE {{
      ?owner a ag:class_POU ;
             dp:hasPOUName "{esc}" .
      ?method a ag:class_Method ;
              op:isMethodOf ?owner ;
              dp:hasMethodType ?method_type ;
              dp:hasPOUCode ?code .
      OPTIONAL {{ ?method dp:hasPOUDeclarationHeader ?header . }}
    }}
    ORDER BY ?method_type
    """
    rows = _sparql_select(q, max_rows=30)
    methods: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        methods.append(
            {
                "method_type": str(row.get("method_type", "") or "").strip(),
                "code": str(row.get("code", "") or ""),
                "header": str(row.get("header", "") or ""),
            }
        )
    return {"owner_name": name, "query": q, "methods": methods}


def _build_last_finished_skill_context(last_finished_skill: str, anchor_token: str) -> Dict[str, Any]:
    skill_name = str(last_finished_skill or "").strip()
    anchor_name = str(anchor_token or "").strip()
    if not skill_name:
        return {"last_finished_skill": "", "available": False}

    pou_ctx = _fetch_pou_code_context_from_kg(skill_name)
    if pou_ctx.get("error"):
        return {
            "last_finished_skill": skill_name,
            "available": False,
            "error": str(pou_ctx.get("error", "") or ""),
        }

    wiring_ctx = _fetch_pou_input_wiring_from_kg(skill_name)
    methods_ctx = _fetch_method_contexts_for_owner_pou(skill_name)

    anchor_short = anchor_name.rsplit(".", 1)[-1].strip() if "." in anchor_name else anchor_name
    counterpart_short = _counterpart_sensor_short_name(anchor_short)

    wiring = wiring_ctx.get("wiring") if isinstance(wiring_ctx.get("wiring"), list) else []
    relevant_wiring: List[Dict[str, str]] = []
    for row in wiring:
        if not isinstance(row, dict):
            continue
        port_name = str(row.get("port_name", "") or "").strip()
        source_name = str(row.get("source_name", "") or "").strip()
        combined = f"{port_name} {source_name}".upper()
        if "LS_" in combined or (anchor_short and anchor_short.upper() in combined) or (
            counterpart_short and counterpart_short.upper() in combined
        ):
            relevant_wiring.append({"port_name": port_name, "source_name": source_name})

    code = str(pou_ctx.get("code", "") or "")
    evidence_terms = _unique_nonempty(
        [
            anchor_short,
            counterpart_short,
            "fbConveyorForward(",
            "cmdConveyorForward",
            "REQ_CB_forwards",
            "jobFinished",
        ]
    )
    body_snippets = _extract_code_snippets(code, evidence_terms, radius=2, max_snips=8)

    method_snippets: List[Dict[str, Any]] = []
    methods = methods_ctx.get("methods") if isinstance(methods_ctx.get("methods"), list) else []
    for method in methods:
        if not isinstance(method, dict):
            continue
        method_type = str(method.get("method_type", "") or "").strip()
        if method_type not in {"Start", "CheckState", "Abort"}:
            continue
        terms = evidence_terms + ["OPCUA.lastExecutedSkill", "OPCUA.lastExecutedProcess", "MethodCall"]
        snippets = _extract_code_snippets(str(method.get("code", "") or ""), terms, radius=2, max_snips=4)
        if snippets:
            method_snippets.append({"method_type": method_type, "snippets": snippets})

    reads_anchor_sensor = any(
        anchor_short
        and (
            str(row.get("port_name", "") or "").strip().lower() == anchor_short.lower()
            or str(row.get("source_name", "") or "").strip().lower().endswith(anchor_short.lower())
        )
        for row in relevant_wiring
        if isinstance(row, dict)
    )
    reads_counterpart_sensor = any(
        counterpart_short
        and (
            str(row.get("port_name", "") or "").strip().lower() == counterpart_short.lower()
            or str(row.get("source_name", "") or "").strip().lower().endswith(counterpart_short.lower())
        )
        for row in relevant_wiring
        if isinstance(row, dict)
    )
    actuation_path_detected = any(
        term.lower() in code.lower() for term in ["fbconveyorforward(", "cmdconveyorforward", "req_cb_forwards"]
    )

    return {
        "last_finished_skill": skill_name,
        "available": True,
        "pou_name": skill_name,
        "sensor_wiring": relevant_wiring,
        "body_snippets": body_snippets,
        "method_snippets": method_snippets,
        "reads_anchor_sensor": reads_anchor_sensor,
        "reads_counterpart_sensor": reads_counterpart_sensor,
        "counterpart_sensor_short": counterpart_short,
        "actuation_path_detected": actuation_path_detected,
    }


def _build_hardware_followup_context(
    ctx: IncidentContext,
    point1: Dict[str, Any],
    point3: Dict[str, Any],
) -> Dict[str, Any]:
    causal = _build_causal_sequence_summary(point1, point3)
    anchor = _as_dict(causal.get("anchor"))
    chain_summary = _as_dict(_as_dict(point1.get("condition_chain")).get("summary"))
    anchor_token = str(anchor.get("token", "") or "").strip()

    snapshot_value = None
    if anchor_token:
        snapshot_value = _snapshot_get_var(ctx.plcSnapshot, _snapshot_candidates_for_token(anchor_token))

    var_ctx = _fetch_variable_context_from_kg(anchor_token) if anchor_token else {}
    var_cls = _classify_variable_context(var_ctx if isinstance(var_ctx, dict) else {})

    last_finished = str(
        ctx.lastFinishedSkill
        or _snapshot_get_var(ctx.plcSnapshot, ["lastFinishedSkill", "OPCUA.lastFinishedSkill"])
        or ""
    ).strip()
    last_finished_ctx = _build_last_finished_skill_context(last_finished, anchor_token)

    mode = "software_prevention"
    if chain_summary.get("has_hardware_address") is True:
        mode = "hardware_diagnosis"

    manual_checks: List[str] = []
    if var_cls.get("is_input_bool_sensor"):
        manual_checks = [
            "Sensor-LED bzw. Eingangskanal an der Klemme prüfen.",
            "Werkstückposition direkt am Sensor prüfen.",
            "Sensor auf Verschmutzung, Verstellung oder Beschädigung prüfen.",
            "Stecker, Leitung und Klemmenkontakt prüfen.",
            "Online-Wert in der SPS mit der realen physischen Situation vergleichen.",
        ]
        if var_cls.get("has_sim_mapping"):
            sim_src = ", ".join(var_cls.get("sim_sources") or [])
            manual_checks.append(f"Zusätzlich die Simulationsquelle pruefen: {sim_src}.")

    return {
        "mode": mode,
        "anchor": anchor,
        "snapshot_value": snapshot_value,
        "variable_context": var_ctx,
        "variable_classification": var_cls,
        "last_finished_skill": last_finished,
        "last_finished_skill_context": last_finished_ctx,
        "manual_checks": manual_checks,
        "origin_assessment": str(chain_summary.get("origin_assessment", "") or ""),
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
        "hardware",
        "sensor",
        "diagnose",
        "pruefen",
        "prüfen",
        "behandlung",
        "werkst",
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
        kind = str(row.get("kind", "")).strip().lower()
        row_idx = int(row.get("_row_idx", 0) or 0)
        text = " | ".join([token, pou, value, assignment, source, reason])

        s = 0
        has_hw_hint = any(
            marker in text
            for marker in [
                "hardware_address",
                "dp_hashardwareaddress",
                "%ix",
                "%iw",
                "%id",
                "%qx",
                "%qw",
                "%qd",
                "%mx",
                "%mw",
                "%md",
            ]
        )
        if has_hw_hint:
            s += 180
        if source in {"global", "metadata"} and has_hw_hint:
            s += 50
        if source == "wiring":
            s += 70
        if kind in {"global_variable", "terminal"}:
            s += 25
        if "default" in assignment or "default" in source or "default" in reason:
            s += 30
        if token.startswith("gvl_") or token.startswith("opcua."):
            s += 8

        if source == "job_method" or kind == "job_method_input" or "job-method input" in reason:
            s -= 220
        if "methodcall" in token:
            s -= 120
        if token in {"hdl", "start", "abort", "checkstate"}:
            s -= 80
        if "jobrunning" in token:
            s -= 40

        s += min(10, row_idx)
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
    hardware_ctx = _build_hardware_followup_context(session.ctx, point1, point3)
    followup_mode = str(hardware_ctx.get("mode", "") or "software_prevention")

    hw_anchor = _as_dict(hardware_ctx.get("anchor"))
    hw_var_ctx = _as_dict(hardware_ctx.get("variable_context"))
    hw_var_cls = _as_dict(hardware_ctx.get("variable_classification"))
    last_finished_skill = str(hardware_ctx.get("last_finished_skill", "") or "")
    last_finished_ctx = _as_dict(hardware_ctx.get("last_finished_skill_context"))
    hw_snapshot_value = hardware_ctx.get("snapshot_value")
    manual_checks = hardware_ctx.get("manual_checks") if isinstance(hardware_ctx.get("manual_checks"), list) else []

    analysis_short = analysis_text[:12000]
    header_short = header[:5000]
    var_decl_short = var_decl[:3500]
    code_short = code[:10000]

    system_prompt = (
        "Du bist ein erfahrener SPS-Engineer fuer Root-Cause-Praevention. "
        "Antworte nur auf Deutsch, technisch praezise, ohne Halluzinationen."
    )
    if followup_mode == "hardware_diagnosis":
        sensor_wiring = last_finished_ctx.get("sensor_wiring") if isinstance(last_finished_ctx.get("sensor_wiring"), list) else []
        wiring_text = (
            "\n".join(
                f"- {str(w.get('port_name', '')).strip()} <= {str(w.get('source_name', '')).strip()}"
                for w in sensor_wiring
                if isinstance(w, dict)
            )
            if sensor_wiring
            else "- keine sensorbezogene Verdrahtung gefunden"
        )
        body_snippets = last_finished_ctx.get("body_snippets") if isinstance(last_finished_ctx.get("body_snippets"), list) else []
        body_snippets_text = (
            "\n\n".join(f"```pascal\n{str(s).strip()}\n```" for s in body_snippets if str(s).strip())
            if body_snippets
            else "- keine relevanten Body-Snippets gefunden"
        )
        method_snippets = last_finished_ctx.get("method_snippets") if isinstance(last_finished_ctx.get("method_snippets"), list) else []
        method_snippets_text = []
        for item in method_snippets:
            if not isinstance(item, dict):
                continue
            method_type = str(item.get("method_type", "") or "").strip()
            snippets = item.get("snippets") if isinstance(item.get("snippets"), list) else []
            if not snippets:
                continue
            block = [f"{method_type}:"]
            block.extend(f"```pascal\n{str(sn).strip()}\n```" for sn in snippets if str(sn).strip())
            method_snippets_text.append("\n".join(block))

        hw_types = ", ".join(str(x) for x in (hw_var_ctx.get("variable_types") or []) if str(x).strip()) or "-"
        hw_addresses = ", ".join(str(x) for x in (hw_var_ctx.get("hardware_addresses") or []) if str(x).strip()) or "-"
        sim_sources = ", ".join(str(x) for x in (hw_var_cls.get("sim_sources") or []) if str(x).strip()) or "-"
        incoming_sources = ", ".join(str(x) for x in (hw_var_ctx.get("incoming_sources") or []) if str(x).strip()) or "-"
        manual_checks_text = "\n".join(f"- {x}" for x in manual_checks) if manual_checks else "- keine vordefinierten Checks"

        user_prompt = (
            "Ziel:\n"
            "Fuehre eine hardware-orientierte Folgeanalyse fuer einen bereits ermittelten Hardware-Endpunkt im Fehlerpfad "
            "zu `OPCUA.TriggerD2 := TRUE` durch.\n\n"
            "WICHTIG zur Struktur:\n"
            "1) ZUERST die Fehlerfolge erklaeren (Upstream -> Trigger),\n"
            "2) DANACH Hardware-Diagnose und Behandlungsweise fuer den Nutzer an der Anlage liefern,\n"
            "3) den Bezug von `lastFinishedSkill` zum Sensor / Werkstueck sauber bewerten.\n\n"
            "Upstream-Anker aus dem Trace:\n"
            f"- Token: {hw_anchor.get('token', anchor.get('token', '-'))}\n"
            f"- Wert: {hw_anchor.get('value', anchor.get('value', '-'))}\n"
            f"- Snapshot-Wert: {hw_snapshot_value!r}\n"
            f"- POU: {hw_anchor.get('pou', anchor.get('pou', '-'))}\n"
            f"- Assignment/Quelle: {hw_anchor.get('assignment', '') or hw_anchor.get('source', '') or '-'}\n\n"
            "Kompakte Fehlerfolge (aus Trace):\n"
            f"- {' -> '.join(str(x) for x in chain)}\n\n"
            "Hardware-/Variablenkontext aus dem KG:\n"
            f"- Typ: {hw_types}\n"
            f"- Hardware-Adressen: {hw_addresses}\n"
            f"- Eingang/BOOL-Sensor: {bool(hw_var_cls.get('is_input_bool_sensor'))}\n"
            f"- Eingehende Quellen/Mappings: {incoming_sources}\n"
            f"- Simulations-Mapping erkannt: {bool(hw_var_cls.get('has_sim_mapping'))}\n"
            f"- Simulationsquellen: {sim_sources}\n\n"
            "POU-/Programm-Wechsel ueber Assignments (aus Triggerpfad):\n"
            + ("\n".join(f"- ({b.get('pou','-')}) {b.get('assignment','')}" for b in bridges) if bridges else "- keine")
            + "\n\n"
            f"lastFinishedSkill aus Snapshot: {last_finished_skill or '-'}\n"
            f"- Liest Anker-Sensor: {bool(last_finished_ctx.get('reads_anchor_sensor'))}\n"
            f"- Liest Gegen-/Nachbarsensor: {bool(last_finished_ctx.get('reads_counterpart_sensor'))}\n"
            f"- Gegen-/Nachbarsensor (kurz): {str(last_finished_ctx.get('counterpart_sensor_short', '') or '-')}\n"
            f"- Aktorik-/Foerderpfad erkannt: {bool(last_finished_ctx.get('actuation_path_detected'))}\n\n"
            "Sensorbezogene Verdrahtung des lastFinishedSkill:\n"
            f"{wiring_text}\n\n"
            "Relevante Body-Snippets des lastFinishedSkill:\n"
            f"{body_snippets_text}\n\n"
            "Relevante Methoden-Snippets des lastFinishedSkill:\n"
            + ("\n\n".join(method_snippets_text) if method_snippets_text else "- keine relevanten Methoden-Snippets gefunden")
            + "\n\n"
            "Empfohlene manuelle Vor-Ort-Checks (deterministisch vorbereitet):\n"
            f"{manual_checks_text}\n\n"
            "Analyse (Auszug):\n"
            f"{analysis_short}\n\n"
            f"POU mit Fehlerbedingung: {focus_pou}\n\n"
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
            "1) Eine Hardware-Diagnose vor Ort: 4-8 konkrete Pruefschritte fuer Nutzer/Instandhaltung.\n"
            "2) Plausible Ursachen, priorisiert, mit Evidenz aus KG/Snapshot/Code. Bewerte mindestens:\n"
            "   - Sensorausfall / Sensor verstellt / verschmutzt / Verkabelung,\n"
            "   - Werkstueck physisch oder manuell entfernt / verrutscht,\n"
            "   - Simulations- bzw. Spiegelungsproblem ueber GVL_*_Sim, falls relevant.\n"
            "3) Eine Erklaerung des Bezugs von `lastFinishedSkill` zum Sensorsignal und ggf. zum Nachbarsensor.\n"
            "   - Erklaere, ob der Skill das Werkstueck bzw. den Materialfluss so beeinflusst, dass der Sensor danach plausibel TRUE gewesen sein koennte.\n"
            "   - Bewerte daraus, ob eine zwischenzeitliche physische/manuelle Aenderung plausibel ist.\n"
            "4) Eine bevorzugte Arbeitshypothese mit kurzer Begruendung.\n"
            "5) Eine Watchlist mit 6-10 Signalen fuer Online-Validierung.\n"
            "Regeln:\n"
            "- KEINE Periodizitaetsbewertung.\n"
            "- KEINE ST-Code-Aenderungen als primaere Massnahme, solange die Hardware-Ursache plausibel ist.\n"
            "- Wenn ein Simulations-Mapping existiert, benenne explizit, welche reale Variable und welche Simulationsquelle zu pruefen sind.\n"
        )
    else:
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
            "mode": followup_mode,
        }

    return {
        "mode": followup_mode,
        "focus_pou": focus_pou,
        "sparql_query": pou_ctx.get("query", ""),
        "declaration_header": header_short,
        "variable_declaration": var_decl_short,
        "code_excerpt": code_short,
        "hardware_context": hardware_ctx,
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
    if ctx.lastFinishedSkill:
        parts.append(f"lastFinishedSkill: {ctx.lastFinishedSkill}")
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


def _build_chat_context_summary(ctx: IncidentContext) -> str:
    summary: Dict[str, Any] = {
        "correlationId": ctx.correlationId,
        "processName": ctx.processName,
        "summary": ctx.summary,
        "triggerEvent": ctx.triggerEvent,
        "lastSkill": ctx.lastSkill,
        "lastFinishedSkill": ctx.lastFinishedSkill,
        "lastGEMMAStateBeforeFailure": ctx.lastGEMMAStateBeforeFailure,
        "triggerD2": ctx.triggerD2,
        "has_plcSnapshot": bool(ctx.plcSnapshot),
    }
    return json.dumps(summary, ensure_ascii=False, indent=2)


def _short_json_text(value: Any, max_chars: int = 700) -> str:
    text = "" if value is None else str(value).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + " ..."


def _summarize_state_fit_evaluations(point3: Dict[str, Any], max_items: int = 3) -> List[Dict[str, Any]]:
    fit = _as_dict(point3.get("state_fit"))
    evaluations = fit.get("path_evaluations") if isinstance(fit.get("path_evaluations"), list) else []
    out: List[Dict[str, Any]] = []

    for ev in evaluations[:max_items]:
        if not isinstance(ev, dict):
            continue
        conflicts_raw = ev.get("conflicts") if isinstance(ev.get("conflicts"), list) else []
        conflicts = [str(x).strip() for x in conflicts_raw if str(x).strip()]
        hw_raw = ev.get("hardware_addresses") if isinstance(ev.get("hardware_addresses"), list) else []
        hardware_addresses = [str(x).strip() for x in hw_raw if str(x).strip()]

        out.append(
            {
                "state_path": str(ev.get("state_path", "") or ""),
                "possible": bool(ev.get("possible")),
                "score": ev.get("score", 0),
                "has_default_terminator": bool(ev.get("has_default_terminator")),
                "hardware_addresses": hardware_addresses[:3],
                "conflicts": conflicts[:2],
            }
        )
    return out


def _build_sticky_planner_context(
    ctx: IncidentContext,
    diagnoseplan: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    incident: Dict[str, Any] = {
        "correlationId": ctx.correlationId,
        "processName": ctx.processName,
        "summary": ctx.summary,
        "triggerEvent": ctx.triggerEvent,
        "lastSkill": ctx.lastSkill,
        "lastFinishedSkill": ctx.lastFinishedSkill,
        "lastGEMMAStateBeforeFailure": ctx.lastGEMMAStateBeforeFailure,
        "triggerD2": ctx.triggerD2,
        "has_plcSnapshot": bool(ctx.plcSnapshot),
    }

    sticky: Dict[str, Any] = {
        "schema_version": "planner_sticky_context_v1",
        "incident": incident,
        "summary_for_planner": [
            f"Trigger-Event: {ctx.triggerEvent or '-'}",
            f"Prozess: {ctx.processName or '-'}",
            f"Letzter Skill: {ctx.lastSkill or '-'}",
            f"Letzter abgeschlossener Skill: {ctx.lastFinishedSkill or '-'}",
            f"Letzter GEMMA-State: {ctx.lastGEMMAStateBeforeFailure or '-'}",
        ],
    }

    if not isinstance(diagnoseplan, dict):
        return sticky

    if diagnoseplan.get("error"):
        sticky["bootstrap"] = {"status": "error", "error": str(diagnoseplan.get("error", "") or "")}
        sticky["summary_for_planner"].append(
            f"Bootstrap-Analyse fehlgeschlagen: {str(diagnoseplan.get('error', '') or '').strip() or '-'}"
        )
        return sticky

    compact = _as_dict(diagnoseplan.get("compact"))
    path_trace = _as_dict(diagnoseplan.get("path_trace"))
    point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
    point3 = _as_dict(path_trace.get("point_3_path_trace"))
    causal = _build_causal_sequence_summary(point1, point3)
    anchor = _as_dict(causal.get("anchor"))
    chain = causal.get("chain") if isinstance(causal.get("chain"), list) else []
    bridges = causal.get("bridges") if isinstance(causal.get("bridges"), list) else []
    rows = _collect_trigger_path_rows(point1, max_rows_per_path=120)
    periodicity_evidence = _extract_periodicity_evidence(rows, max_items=8)
    chain_summary = _as_dict(_as_dict(point1.get("condition_chain")).get("summary"))
    fit = _as_dict(point3.get("state_fit"))
    best_paths = fit.get("best_paths") if isinstance(fit.get("best_paths"), list) else []
    summarized_evaluations = _summarize_state_fit_evaluations(point3, max_items=3)

    sticky["bootstrap"] = {
        "pipeline": str(diagnoseplan.get("pipeline", "") or ""),
        "status": str(compact.get("status", "") or "ok"),
        "missing_tools": compact.get("missing_tools", []) if isinstance(compact.get("missing_tools"), list) else [],
        "compact": compact,
    }
    sticky["channels"] = {
        "upstream_anchor": {
            "token": str(anchor.get("token", "") or ""),
            "value": str(anchor.get("value", "") or ""),
            "pou": str(anchor.get("pou", "") or ""),
            "assignment": str(anchor.get("assignment", "") or anchor.get("source", "") or ""),
            "reason": str(anchor.get("reason", "") or ""),
        },
        "causal_chain": {
            "ordered_tokens": [str(x) for x in chain if str(x).strip()],
            "assignment_bridges": bridges[:8],
        },
        "trigger": {
            "trigger_var": str(point1.get("trigger_var", "") or "OPCUA.TriggerD2"),
            "setter_pou": str(point1.get("pou_name", "") or ""),
            "condition_expr": str(point1.get("condition_expr", "") or ""),
            "snippet_true": _short_json_text(point1.get("snippet_true", ""), max_chars=900),
        },
        "state_path": {
            "selected_state_path": str(point3.get("selected_state_path", "") or ""),
            "best_paths": [str(x) for x in best_paths[:3]],
            "skill_expr": str(_as_dict(point3.get("skill_trace")).get("skill_expr", "") or ""),
            "top_path_evaluations": summarized_evaluations,
        },
        "periodicity": {
            "evidence": periodicity_evidence,
            "software_likely": chain_summary.get("software_likely"),
            "has_hardware_address": chain_summary.get("has_hardware_address"),
            "origin_assessment": str(chain_summary.get("origin_assessment", "") or ""),
        },
    }

    summary_lines = sticky.get("summary_for_planner")
    if isinstance(summary_lines, list):
        if anchor:
            summary_lines.append(
                "Upstream-Anker: "
                f"{anchor.get('token', '?')}={anchor.get('value', '?')} in {anchor.get('pou', '?')}"
            )
        if chain:
            summary_lines.append("Fehlerkette: " + " -> ".join(str(x) for x in chain if str(x).strip()))
        setter_pou = str(point1.get("pou_name", "") or "")
        condition_expr = str(point1.get("condition_expr", "") or "")
        if setter_pou:
            line = f"Trigger-Setter: OPCUA.TriggerD2 in {setter_pou}"
            if condition_expr:
                line += f" bei Bedingung {condition_expr}"
            summary_lines.append(line)
        if summarized_evaluations:
            top = summarized_evaluations[0]
            top_path = str(top.get("state_path", "") or "")
            if top_path:
                summary_lines.append(
                    f"Top-State-Path: {top_path} | possible={bool(top.get('possible'))} | score={top.get('score', 0)}"
                )
        if periodicity_evidence:
            summary_lines.append(f"Periodizitaets-Hinweise vorhanden: {len(periodicity_evidence)}")

    return sticky


class ExcHChatBotSession:
    def __init__(self, bot: Any, ctx: IncidentContext):
        self.bot = bot
        self.ctx = ctx
        self.bootstrap_evd2_plan: Optional[Dict[str, Any]] = None
        self.sticky_planner_context: Dict[str, Any] = {}
        self._refresh_sticky_planner_context()

    def _refresh_sticky_planner_context(self, diagnoseplan: Optional[Dict[str, Any]] = None) -> None:
        self.sticky_planner_context = _build_sticky_planner_context(self.ctx, diagnoseplan=diagnoseplan)
        if hasattr(self.bot, "set_sticky_context"):
            try:
                self.bot.set_sticky_context(self.sticky_planner_context)
            except Exception:
                pass

    def _build_session_extra_context(self, *, include_bootstrap: bool = False) -> str:
        sections: List[str] = []
        if not hasattr(self.bot, "set_sticky_context"):
            sections.append(_build_chat_context_summary(self.ctx))
            if self.sticky_planner_context:
                sections.append(
                    "Persistenter Analysekontext (JSON):\n"
                    + json.dumps(self.sticky_planner_context, ensure_ascii=False, indent=2)[:12000]
                )
        if include_bootstrap:
            bootstrap = self.ensure_bootstrap_plan()
            sections.append(
                "EvD2DiagnosisTool Ergebnis (JSON):\n"
                + json.dumps(bootstrap, ensure_ascii=False, indent=2)
            )
        return "\n\n".join(s for s in sections if s.strip())

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
        self._refresh_sticky_planner_context(self.bootstrap_evd2_plan)
        return self.bootstrap_evd2_plan

    def ask(self, user_msg: str, debug: bool = True, *, include_bootstrap: bool = False) -> Dict[str, Any]:
        can_use_cached_prevention_path = (
            _should_build_evd2_plan(self.ctx)
            and _question_wants_prevention_suggestions(user_msg)
            and (self.bootstrap_evd2_plan is not None or not self.bot.history)
        )
        if can_use_cached_prevention_path:
            diagnoseplan = self.ensure_bootstrap_plan()
            if isinstance(diagnoseplan, dict):
                base_analysis = _render_initial_evd2_answer(self.ctx, diagnoseplan)
                suggestion_block = _build_llm_prevention_suggestions(self, diagnoseplan, base_analysis)

                llm_text = str(suggestion_block.get("llm_answer", "") or "").strip()
                llm_err = str(suggestion_block.get("error", "") or "").strip()
                followup_mode = str(suggestion_block.get("mode", "") or "software_prevention")
                followup_title = (
                    "LLM-basierte Hardware-Diagnose und Behandlungsweise:"
                    if followup_mode == "hardware_diagnosis"
                    else "LLM-basierte Loesungsvorschlaege zur Vermeidung des Pfads:"
                )
                followup_error_title = (
                    "LLM-basierte Hardware-Diagnose konnte nicht erzeugt werden: "
                    if followup_mode == "hardware_diagnosis"
                    else "LLM-basierte Loesungsvorschlaege konnten nicht erzeugt werden: "
                )

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
                    lines.append(followup_title)
                    lines.append(llm_text)
                else:
                    lines.append("")
                    lines.append(followup_error_title + (llm_err or "unbekannter Fehler"))

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
                self.bot.remember_turn(user_msg, response)
                return response

        extra_context = self._build_session_extra_context(include_bootstrap=include_bootstrap)
        return self.bot.chat(user_msg, debug=debug, extra_context=extra_context)


def build_session_from_input(obj: Dict[str, Any]) -> ExcHChatBotSession:
    ctx = IncidentContext.from_input(obj)
    kg_path = ctx.kg_ttl_path or os.environ.get("MSRGUARD_KG_TTL", "")

    if not kg_path:
        raise RuntimeError("Kein KG TTL Pfad gefunden. Setze payload.kg_ttl_path ODER ENV MSRGUARD_KG_TTL.")

    from msrguard.chatbot_core import build_bot
    bot = build_bot(kg_ttl_path=kg_path, plc_snapshot=ctx.plcSnapshot)
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


def build_auto_followup_request(session: ExcHChatBotSession) -> Dict[str, str]:
    diagnoseplan = session.ensure_bootstrap_plan()
    path_trace = _as_dict(diagnoseplan.get("path_trace")) if isinstance(diagnoseplan, dict) else {}
    point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
    point3 = _as_dict(path_trace.get("point_3_path_trace"))
    hardware_ctx = _build_hardware_followup_context(session.ctx, point1, point3)

    if str(hardware_ctx.get("mode", "") or "") == "hardware_diagnosis":
        anchor = _as_dict(hardware_ctx.get("anchor"))
        var_ctx = _as_dict(hardware_ctx.get("variable_context"))
        last_finished = str(hardware_ctx.get("last_finished_skill", "") or "-")
        sim_sources = ", ".join(str(x) for x in (hardware_ctx.get("variable_classification", {}) or {}).get("sim_sources", []) if str(x).strip()) or "-"
        prompt = (
            "Bitte erklaere jetzt den Hardware-Fehlerpfad aus der Initialanalyse im Detail. "
            f"Fokus: Welche Rolle spielt der Hardware-Endpunkt `{anchor.get('token', '-')}` mit Snapshot-Wert `{hardware_ctx.get('snapshot_value', None)!r}` "
            f"und Hardware-Adresse(n) `{', '.join(str(x) for x in (var_ctx.get('hardware_addresses') or []) if str(x).strip()) or '-'}`? "
            f"Untersuche den letzten abgeschlossenen Skill `{last_finished}` und seinen Bezug zu `{anchor.get('token', '-')}` "
            f"sowie zum zugehoerigen Nachbar-/Gegensensor `{str(_as_dict(hardware_ctx.get('last_finished_skill_context')).get('counterpart_sensor_short', '') or '-')}`. "
            f"Wenn ein Simulations-/Spiegelungs-Mapping existiert, beruecksichtige explizit die Quelle(n) `{sim_sources}`. "
            "Keine Periodizitaetsbewertung. Stattdessen: leite plausible Hardware-Ursachen ab, nenne konkrete manuelle Diagnose-/Behandlungsschritte an der Anlage, "
            "und bewerte auf Basis von Snapshot, KG und Code, ob Sensorfehler oder eine zwischenzeitliche manuelle/physische Werkstueckentfernung plausibler ist."
        )
        return {
            "mode": "hardware_diagnosis",
            "label": "Pfad-Erklärung + Hardware-Diagnose",
            "prompt": prompt,
        }

    point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
    point3 = _as_dict(path_trace.get("point_3_path_trace"))
    focus_pou = _pick_focus_pou_for_solution(point1, point3)
    prompt = (
        "Bitte erklaere jetzt den Fehlerpfad aus der Initialanalyse im Detail. "
        f"Fokus: Welche Rolle spielen die relevanten Signale im FB/POU `{focus_pou}`, "
        "wie fuehrt der Pfad bis `OPCUA.TriggerD2`, und ob eine periodische Ausloesung "
        "aus den vorhandenen Trace-Belegen abgeleitet werden kann (ohne Annahmen ohne Evidenz). "
        f"Nutze Code + Variablendeklaration von `{focus_pou}`, die bereits im KG liegen. "
        "Danach nenne konkrete Vermeidungsmaßnahmen, priorisiert, und begruende sie nur mit "
        "der vorhandenen softwareseitigen Evidenz des Triggerpfads."
    )
    return {
        "mode": "software_prevention",
        "label": "Pfad-Erklärung + Vermeidungsmaßnahmen",
        "prompt": prompt,
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
            path_trace = _as_dict(diagnoseplan.get("path_trace"))
            point1 = _as_dict(path_trace.get("point_1_trigger_setter"))
            point3 = _as_dict(path_trace.get("point_3_path_trace"))
            heading = "4) LLM-basierte Loesungsvorschlaege zur Vermeidung des Pfads:\n"
            followup_hint = (
                "Auf Nachfrage. Frage z.B.: `Bitte gib Massnahmen zur Vermeidung dieses Pfads.`"
            )
            hardware_ctx = _build_hardware_followup_context(session.ctx, point1, point3)
            if str(hardware_ctx.get("mode", "") or "") == "hardware_diagnosis":
                heading = "4) LLM-basierte Hardware-Diagnose und Behandlungsweise:\n"
                followup_hint = (
                    "Auf Nachfrage. Frage z.B.: "
                    "`Bitte gib Hardware-Diagnose und manuelle Prüfschritte fuer diesen Pfad.`"
                )
            answer = (
                answer
                + "\n\n"
                + heading
                + followup_hint
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
            session.bot.remember_turn("Bitte fuehre eine initiale Vorfallanalyse durch.", result)
            return result

    prompt = build_initial_prompt(session.ctx, diagnoseplan=diagnoseplan)
    return session.ask(prompt, debug=debug, include_bootstrap=False)
