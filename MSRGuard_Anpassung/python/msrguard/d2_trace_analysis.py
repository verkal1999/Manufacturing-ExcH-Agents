from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS

AG = Namespace("http://www.semanticweb.org/AgentProgramParams/")


P_DP_HAS_POU_NAME = AG["dp_hasPOUName"]
P_DP_HAS_POU_CODE = AG["dp_hasPOUCode"]
P_DP_HAS_POU_LANGUAGE = AG["dp_hasPOULanguage"]
P_DP_IS_GEMMA = AG["dp_isGEMMAStateMachine"]
P_DP_HAS_FBTYPE_DESCRIPTION = AG["dp_hasFBTypeDescription"]
P_DP_IS_JOB_METHOD = AG["dp_isJobMethod"]
P_DP_HAS_METHOD_TYPE = AG["dp_hasMethodType"]

P_OP_HAS_PORT = AG["op_hasPort"]
P_DP_HAS_PORT_NAME = AG["dp_hasPortName"]
P_DP_HAS_PORT_DIRECTION = AG["dp_hasPortDirection"]
P_DP_HAS_DEFAULT_VALUE = AG["dp_hasDefaultValue"]
P_DP_HAS_DEFAULT_PORT_VALUE = AG["dp_hasDefaultPortValue"]
P_DP_HAS_DEFAULT_VARIABLE_VALUE = AG["dp_hasDefaultVariableValue"]

P_OP_ASSIGNS_TO_PORT = AG["op_assignsToPort"]
P_OP_ASSIGNS_FROM = AG["op_assignsFrom"]
P_OP_INSTANTIATES_PORT = AG["op_instantiatesPort"]

P_OP_USES_VARIABLE = AG["op_usesVariable"]
P_OP_HAS_INTERNAL_VARIABLE = AG["op_hasInternalVariable"]
P_DP_HAS_VARIABLE_NAME = AG["dp_hasVariableName"]
P_DP_HAS_VARIABLE_TYPE = AG["dp_hasVariableType"]
P_DP_HAS_HARDWARE_ADDRESS = AG["dp_hasHardwareAddress"]
P_DP_HAS_OPCUA_DATA_ACCESS = AG["dp_hasOPCUADataAccess"]
P_DP_HAS_OPCUA_WRITE_ACCESS = AG["dp_hasOPCUAWriteAccess"]
P_DP_HAS_EXPRESSION_TEXT = AG["dp_hasExpressionText"]
P_DP_HAS_FBINSTANCE_NAME = AG["dp_hasFBInstanceName"]
P_OP_REPRESENTS_FB_INSTANCE = AG["op_representsFBInstance"]
P_OP_IS_INSTANCE_OF_FBTYPE = AG["op_isInstanceOfFBType"]
P_OP_IS_PORT_OF_INSTANCE = AG["op_isPortOfInstance"]
P_OP_HAS_METHOD = AG["op_hasMethod"]
P_OP_HAS_START_METHOD = AG["op_hasStartMethod"]
P_OP_HAS_ABORT_METHOD = AG["op_hasAbortMethod"]
P_OP_HAS_CHECK_STATE_METHOD = AG["op_hasCheckStateMethod"]
P_OP_IS_METHOD_OF = AG["op_isMethodOf"]

CLASS_CUSTOM_FB = AG["class_CustomFBType"]
CLASS_PORT_INSTANCE = AG["class_PortInstance"]
CLASS_METHOD = AG["class_Method"]


def _literal_is_true(lit: Optional[Literal]) -> bool:
    if lit is None:
        return False
    try:
        val = lit.toPython()
        if isinstance(val, bool):
            return val
    except Exception:
        pass
    return str(lit).strip().lower() in {"true", "1", "yes"}


def _as_text(node: Any) -> str:
    return "" if node is None else str(node)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        try:
            return int(value)
        except Exception:
            return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return default


def _first(graph: Graph, subj: URIRef, pred: URIRef) -> Optional[Any]:
    return next(graph.objects(subj, pred), None)


def _norm_dir(direction: str) -> str:
    d = (direction or "").strip().lower()
    if d in {"in", "input"}:
        return "Input"
    if d in {"out", "output"}:
        return "Output"
    if d in {"inout", "in_out"}:
        return "InOut"
    return direction or ""


def _is_internal_v(var: str) -> bool:
    return bool(re.fullmatch(r"V_\d+", (var or "").strip()))


def _is_time_literal(token: str) -> bool:
    return bool(re.fullmatch(r"T#.+", (token or "").strip(), flags=re.I))


def _split_top_level_csv(text: str) -> List[str]:
    out: List[str] = []
    if not text:
        return out
    cur: List[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
            cur.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            cur.append(ch)
            continue
        if ch == "," and depth == 0:
            part = "".join(cur).strip()
            if part:
                out.append(part)
            cur = []
            continue
        cur.append(ch)
    part = "".join(cur).strip()
    if part:
        out.append(part)
    return out


@dataclass
class Tracer:
    enabled: bool = True
    print_live: bool = False
    lines: List[str] = field(default_factory=list)

    def log(self, msg: str) -> None:
        if not self.enabled:
            return
        self.lines.append(msg)
        if self.print_live:
            print(msg)


@dataclass
class FbdNode:
    var: str
    func: Optional[str] = None
    args: Optional[List["FbdNode"]] = None

    def to_dict(self) -> Dict[str, Any]:
        if not self.func:
            return {"var": self.var}
        return {"var": self.var, "func": self.func, "args": [a.to_dict() for a in (self.args or [])]}


def _parse_fbd_call_assignments(fbd_py_code: str, trace: Optional[Tracer] = None) -> Dict[str, Tuple[str, List[str]]]:
    pattern = r"^\s*(V_\d+)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\(([^)]*)\)\s*$"
    m: Dict[str, Tuple[str, List[str]]] = {}
    for mm in re.finditer(pattern, fbd_py_code or "", flags=re.M):
        var, func, argstr = mm.group(1), mm.group(2), mm.group(3)
        args = [a.strip() for a in _split_top_level_csv(argstr) if a.strip()]
        m[var] = (func, args)
    if trace:
        trace.log(f"[FBD] Parsed call assignments: {len(m)} entries")
    return m


def _find_fbd_output_var(fbd_py_code: str, symbol_name: str, trace: Optional[Tracer] = None) -> Optional[str]:
    m = re.search(rf"^\s*{re.escape(symbol_name)}\s*=\s*(V_\d+)\s*$", fbd_py_code or "", flags=re.M)
    if trace:
        if m:
            trace.log(f"[FBD] Found final assignment: {symbol_name} = {m.group(1)}")
        else:
            trace.log(f"[FBD] Final assignment not found for '{symbol_name}'")
    return m.group(1) if m else None


def _build_fbd_tree(var: str, assigns: Dict[str, Tuple[str, List[str]]], depth: int = 0, max_depth: int = 35) -> FbdNode:
    if depth >= max_depth:
        return FbdNode(var=var, func="MAX_DEPTH", args=[])
    if var not in assigns:
        return FbdNode(var=var)
    func, args = assigns[var]
    return FbdNode(var=var, func=func, args=[_build_fbd_tree(a, assigns, depth + 1, max_depth) for a in args])


def trace_symbol_from_fbd(
    fbd_py_code: str,
    symbol_name: str,
    target_value: str = "TRUE",
    trace: Optional[Tracer] = None,
) -> Dict[str, Any]:
    assigns = _parse_fbd_call_assignments(fbd_py_code, trace=trace)
    out_from = _find_fbd_output_var(fbd_py_code, symbol_name, trace=trace)
    if not out_from:
        return {"error": f"Konnte '{symbol_name} = V_...' im FBD-Code nicht finden."}

    if out_from not in assigns:
        return {"error": f"'{symbol_name}' kommt von '{out_from}', aber '{out_from}' hat keine Call-Zuweisung."}

    func, args = assigns[out_from]
    full_tree = _build_fbd_tree(out_from, assigns).to_dict()
    result: Dict[str, Any] = {
        "symbol": symbol_name,
        "assigned_from": out_from,
        "func": func,
        "args": args,
        "tree": full_tree,
    }

    if func.startswith("RS_") and len(args) >= 2:
        set_var, reset_var = args[0], args[1]
        result["set_tree"] = _build_fbd_tree(set_var, assigns).to_dict()
        result["reset_tree"] = _build_fbd_tree(reset_var, assigns).to_dict()
        if (target_value or "").strip().upper() == "TRUE":
            result["focus_tree"] = result["set_tree"]
        elif (target_value or "").strip().upper() == "FALSE":
            result["focus_tree"] = result["reset_tree"]
    elif func.startswith("SR_") and len(args) >= 2:
        set_var, reset_var = args[0], args[1]
        result["set_tree"] = _build_fbd_tree(set_var, assigns).to_dict()
        result["reset_tree"] = _build_fbd_tree(reset_var, assigns).to_dict()
        if (target_value or "").strip().upper() == "TRUE":
            result["focus_tree"] = result["set_tree"]
        elif (target_value or "").strip().upper() == "FALSE":
            result["focus_tree"] = result["reset_tree"]

    if trace:
        trace.log(f"[FBD] {symbol_name} assigned_from {out_from} via {func}")
    return result


def _collect_leaf_tokens(node: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    if "func" not in node:
        out.append(node.get("var", ""))
        return out
    for a in node.get("args", []) or []:
        out.extend(_collect_leaf_tokens(a))
    return out


def _node_to_expr(node: Dict[str, Any]) -> str:
    if "func" not in node:
        return str(node.get("var", ""))
    args = ", ".join(_node_to_expr(a) for a in (node.get("args") or []))
    return f"{node['func']}({args})"


def _find_first_or_node(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    func = node.get("func")
    if func and str(func).startswith("OR_"):
        return node
    for a in node.get("args", []) or []:
        found = _find_first_or_node(a)
        if found:
            return found
    return None


def _leading_scope_token(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    m = re.match(r"^([A-Za-z0-9]+)(?:[_\.].*)?$", s)
    return str(m.group(1) or "").strip() if m else ""


def _collect_scope_hints(values: Optional[Iterable[Any]]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for raw in values or []:
        text = str(raw or "").strip()
        if not text:
            continue
        for cand in (text, _leading_scope_token(text)):
            key = str(cand or "").strip()
            if not key:
                continue
            norm = key.upper()
            if norm in seen:
                continue
            seen.add(norm)
            out.append(key)
    return out


def _score_name_with_scope(
    name: str,
    *,
    preferred_names: Optional[Iterable[str]] = None,
    scope_hints: Optional[Iterable[str]] = None,
) -> int:
    text = str(name or "").strip()
    if not text:
        return 0

    score = 0
    name_upper = text.upper()
    name_prefix = _leading_scope_token(text).upper()

    for pref in preferred_names or []:
        pref_text = str(pref or "").strip()
        if not pref_text:
            continue
        pref_upper = pref_text.upper()
        pref_prefix = _leading_scope_token(pref_text).upper()
        if name_upper == pref_upper:
            score = max(score, 100)
            continue
        if pref_prefix and name_prefix and pref_prefix == name_prefix:
            score = max(score, 60)

    for hint in scope_hints or []:
        hint_text = str(hint or "").strip()
        if not hint_text:
            continue
        hint_upper = hint_text.upper()
        hint_prefix = _leading_scope_token(hint_text).upper()
        if name_upper == hint_upper:
            score = max(score, 90)
            continue
        if hint_prefix and name_prefix and hint_prefix == name_prefix:
            score = max(score, 50)
            continue
        if name_upper.startswith(hint_upper + "_"):
            score = max(score, 40)
            continue
        if hint_upper in name_upper:
            score = max(score, 20)

    return score


def _pick_scoped_gemma_pou(
    graph: Graph,
    gemma_hits: List[Tuple[URIRef, str]],
    *,
    preferred_names: Optional[Iterable[str]] = None,
    scope_hints: Optional[Iterable[str]] = None,
    trace: Optional[Tracer] = None,
) -> Tuple[URIRef, str]:
    if not gemma_hits:
        raise ValueError("gemma_hits ist leer.")

    ranked: List[Tuple[int, str, URIRef, str]] = []
    pref_list = [str(x).strip() for x in (preferred_names or []) if str(x).strip()]
    hint_list = [str(x).strip() for x in (scope_hints or []) if str(x).strip()]

    for pou_uri, pou_name in gemma_hits:
        lang = get_pou_language(graph, pou_uri)
        score = _score_name_with_scope(
            pou_name,
            preferred_names=pref_list,
            scope_hints=hint_list,
        )
        if str(lang or "").strip().upper() == "FBD":
            score += 5
        ranked.append((score, str(pou_name or ""), pou_uri, str(lang or "")))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    chosen = ranked[0]

    if trace:
        if pref_list or hint_list:
            trace.log(
                "[KG] Scoped GEMMA selection: "
                f"preferred={pref_list or '-'} scope={hint_list or '-'} -> {chosen[1]} (score={chosen[0]})"
            )
        else:
            trace.log(f"[KG] GEMMA selection fallback -> {chosen[1]} (alphabetisch/FBD)")

    return chosen[2], chosen[1]


def gemma_state_list() -> List[str]:
    a = [f"A{i}" for i in range(1, 8)]
    f = [f"F{i}" for i in range(1, 7)]
    d = [f"D{i}" for i in range(1, 4)]
    return a + f + d


def _gemma_state_aliases(state: str) -> Set[str]:
    text = str(state or "").strip()
    if not text:
        return set()

    aliases: Set[str] = set()
    aliases.add(text.upper())

    if "." in text:
        aliases.add(text.rsplit(".", 1)[-1].strip().upper())

    m = re.search(r"([AFD]\d+)$", text, flags=re.I)
    if m:
        aliases.add(str(m.group(1) or "").strip().upper())

    return {a for a in aliases if a}


def infer_suspected_input_port_from_last_state(
    d2_trace: Dict[str, Any],
    last_state: str,
    trace: Optional[Tracer] = None,
) -> Dict[str, Any]:
    if "error" in d2_trace:
        return {"error": d2_trace["error"]}
    set_tree = d2_trace.get("set_tree") or d2_trace.get("focus_tree")
    if not set_tree:
        return {"error": "Kein set_tree/focus_tree im d2_trace vorhanden."}

    gemma_states = set(gemma_state_list())
    last_state_aliases = _gemma_state_aliases(last_state)
    or_node = _find_first_or_node(set_tree)
    if not or_node:
        leaves = [x for x in _collect_leaf_tokens(set_tree) if x and x not in gemma_states and not _is_internal_v(x)]
        uniq = sorted(set(leaves), key=leaves.index)
        return {"note": "Kein OR-Node gefunden.", "next_trace_candidates": uniq}

    branches = or_node.get("args") or []
    branch_infos = []
    for idx, b in enumerate(branches, start=1):
        leaves = _collect_leaf_tokens(b)
        states = sorted({x for x in leaves if x in gemma_states})
        branch_state_aliases = {str(x or "").strip().upper() for x in states if str(x or "").strip()}
        branch_infos.append(
            {
                "idx": idx,
                "expr": _node_to_expr(b),
                "gemma_states_in_branch": states,
                "contains_last_state": bool(last_state_aliases & branch_state_aliases),
            }
        )

    if trace:
        trace.log(f"[AUTO] Found OR node: {_node_to_expr(or_node)}")
        for bi in branch_infos:
            trace.log(f"[AUTO]   b{bi['idx']}: {bi['expr']} states={bi['gemma_states_in_branch']} contains_last={bi['contains_last_state']}")

    matching = [x for x in branch_infos if x["contains_last_state"]]
    chosen: Optional[Dict[str, Any]] = None
    note = ""
    if len(matching) == 1:
        chosen = matching[0]
    elif not str(last_state or "").strip():
        def _branch_rank(info: Dict[str, Any]) -> Tuple[int, int, int, int]:
            states = [str(x or "").strip().upper() for x in (info.get("gemma_states_in_branch") or []) if str(x or "").strip()]
            stable = sum(1 for s in states if re.fullmatch(r"[AF]\d+", s))
            errors = sum(1 for s in states if re.fullmatch(r"D\d+", s))
            others = len(states) - stable - errors
            # Ohne last_state bevorzugen wir A/F-Zweige vor D-Zweigen, da D2 typischerweise
            # aus einem stabilen Zustand heraus betreten wird und nicht aus einem anderen Fehlerzustand.
            return (stable, -errors, -others, -len(states))

        ranked = sorted(branch_infos, key=_branch_rank, reverse=True)
        if ranked:
            best = ranked[0]
            second_rank = _branch_rank(ranked[1]) if len(ranked) > 1 else None
            if second_rank is None or _branch_rank(best) > second_rank:
                chosen = best
                note = (
                    "LastGEMMAStateBeforeFailure fehlt; OR-Zweig heuristisch ueber stabilen GEMMA-Zustand "
                    "(A/F vor D) gewaehlt."
                )

    if chosen is None:
        return {
            "or_branches": branch_infos,
            "error": f"Nicht deterministisch: {len(matching)} passende OR-Ã„ste fÃ¼r last_state='{last_state}'.",
        }

    chosen_node = branches[chosen["idx"] - 1]
    leaves = _collect_leaf_tokens(chosen_node)
    candidates: List[str] = []
    for tok in leaves:
        t = (tok or "").strip()
        if not t:
            continue
        if t in gemma_states:
            continue
        if _is_internal_v(t):
            continue
        if t.upper() in {"TRUE", "FALSE"}:
            continue
        if t not in candidates:
            candidates.append(t)

    if trace:
        trace.log(f"[AUTO] Chosen branch: {chosen['expr']}")
        trace.log(f"[AUTO] Next trace candidates: {candidates}")
        if note:
            trace.log(f"[AUTO] {note}")

    out = {"or_branches": branch_infos, "chosen_branch": chosen, "next_trace_candidates": candidates}
    if note:
        out["note"] = note
    return out


def find_gemma_pous(graph: Graph, trace: Optional[Tracer] = None) -> List[Tuple[URIRef, str]]:
    hits: List[Tuple[URIRef, str]] = []
    for pou in graph.subjects(RDF.type, CLASS_CUSTOM_FB):
        lit = next(graph.objects(pou, P_DP_IS_GEMMA), None)
        if _literal_is_true(lit):
            name_lit = next(graph.objects(pou, P_DP_HAS_POU_NAME), None)
            hits.append((pou, _as_text(name_lit) if name_lit else str(pou)))
    hits.sort(key=lambda x: x[1])
    if trace:
        trace.log(f"[KG] GEMMA candidates: {len(hits)}")
        for uri, name in hits[:10]:
            trace.log(f"[KG]   {name} -> {uri}")
    return hits


def get_pou_name(graph: Graph, pou_uri: URIRef) -> str:
    return _as_text(_first(graph, pou_uri, P_DP_HAS_POU_NAME)) or str(pou_uri)


def get_pou_language(graph: Graph, pou_uri: URIRef) -> str:
    return _as_text(_first(graph, pou_uri, P_DP_HAS_POU_LANGUAGE))


def get_pou_code(graph: Graph, pou_uri: URIRef) -> str:
    return _as_text(_first(graph, pou_uri, P_DP_HAS_POU_CODE))


def get_port_by_name(graph: Graph, pou_uri: URIRef, port_name: str) -> Optional[URIRef]:
    for port in graph.objects(pou_uri, P_OP_HAS_PORT):
        if _as_text(_first(graph, port, P_DP_HAS_PORT_NAME)) == port_name:
            return port
    return None


def get_port_direction(graph: Graph, port_uri: URIRef) -> str:
    return _norm_dir(_as_text(_first(graph, port_uri, P_DP_HAS_PORT_DIRECTION)))


def get_port_default_value(graph: Graph, port_uri: URIRef) -> Optional[str]:
    val = _first(graph, port_uri, P_DP_HAS_DEFAULT_PORT_VALUE)
    if val is None:
        val = _first(graph, port_uri, P_DP_HAS_DEFAULT_VALUE)
    if val is None:
        return None
    return _as_text(val)


def resolve_input_to_upstream_output(graph: Graph, pou_uri: URIRef, input_port_name: str) -> Dict[str, Any]:
    port = get_port_by_name(graph, pou_uri, input_port_name)
    if not port:
        return {"error": f"Port '{input_port_name}' nicht in POU {pou_uri} gefunden."}

    assigns = list(graph.subjects(P_OP_ASSIGNS_TO_PORT, port))
    port_dir = get_port_direction(graph, port)
    if not assigns:
        return {
            "pou_uri": str(pou_uri),
            "input_port_name": input_port_name,
            "input_port_dir": port_dir,
            "is_wired": False,
            "assignments_count": 0,
            "default_value": get_port_default_value(graph, port),
        }

    assignment = assigns[0]
    caller_source = _first(graph, assignment, P_OP_ASSIGNS_FROM)
    if not caller_source:
        return {"error": f"Assignment {assignment} hat kein op_assignsFrom."}
    caller_port = _first(graph, caller_source, P_OP_INSTANTIATES_PORT)
    caller_port_name = ""
    caller_port_dir = ""
    caller_pou: Optional[URIRef] = None
    caller_pou_name = ""
    caller_code = ""

    if caller_port:
        caller_port_name = _as_text(_first(graph, caller_port, P_DP_HAS_PORT_NAME))
        caller_port_dir = get_port_direction(graph, caller_port)

        caller_pous = list(graph.subjects(P_OP_HAS_PORT, caller_port))
        if not caller_pous:
            return {"error": f"Kein CallerPOU gefunden, das caller_port {caller_port} besitzt."}
        caller_pou = caller_pous[0]
        caller_pou_name = get_pou_name(graph, caller_pou)
        caller_code = get_pou_code(graph, caller_pou)
    else:
        caller_var_name = _as_text(_first(graph, caller_source, P_DP_HAS_VARIABLE_NAME))
        caller_expr = _as_text(_first(graph, caller_source, P_DP_HAS_EXPRESSION_TEXT))
        caller_port_name = caller_var_name or caller_expr
        caller_pou = pou_uri
        caller_pou_name = "-"
        caller_code = get_pou_code(graph, pou_uri)
        if not caller_port_name:
            return {"error": f"Quelle {caller_source} hat weder op_instantiatesPort noch Variable-/Expression-Namen."}

    return {
        "pou_uri": str(pou_uri),
        "input_port_name": input_port_name,
        "input_port_dir": port_dir,
        "is_wired": True,
        "assignments_count": len(assigns),
        "assignment": str(assignment),
        "caller_port_name": caller_port_name,
        "caller_port_dir": caller_port_dir,
        "caller_pou_uri": str(caller_pou),
        "caller_pou_name": caller_pou_name,
        "caller_code": caller_code,
    }


def _strip_st_comments(st: str) -> str:
    st = re.sub(r"\(\*.*?\*\)", "", st or "", flags=re.S)
    st = re.sub(r"//.*?$", "", st, flags=re.M)
    return st


def extract_assignments_st(st_code: str, var_name: str, trace: Optional[Tracer] = None) -> List[Dict[str, Any]]:
    clean = _strip_st_comments(st_code)
    lines = clean.splitlines()

    rx_if = re.compile(r"^\s*IF\s+(.*?)\s+THEN\s*$", flags=re.I)
    rx_elsif = re.compile(r"^\s*ELSIF\s+(.*?)\s+THEN\s*$", flags=re.I)
    rx_else = re.compile(r"^\s*ELSE\s*$", flags=re.I)
    rx_end = re.compile(r"^\s*END_IF\s*;?\s*$", flags=re.I)
    rx_assign = re.compile(rf"^\s*{re.escape(var_name)}\s*:=\s*(.*?)\s*;\s*$", flags=re.I)

    if_stack: List[Dict[str, Any]] = []
    out: List[Dict[str, Any]] = []

    def _stack_repr() -> List[str]:
        rep: List[str] = []
        for e in if_stack:
            if e["branch"] == "IF":
                rep.append(f"IF@{e['if_start_line']}: {e['cond']}")
            elif e["branch"] == "ELSIF":
                rep.append(f"ELSIF@{e['if_start_line']}: {e['cond']}")
            elif e["branch"] == "ELSE":
                rep.append(f"ELSE@{e['if_start_line']} (zu IF: {e['if_cond']})")
        return rep

    for i, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue

        m_if = rx_if.match(line)
        if m_if:
            cond = m_if.group(1).strip()
            if_stack.append({"branch": "IF", "cond": cond, "if_start_line": i, "if_cond": cond})
            continue

        m_elsif = rx_elsif.match(line)
        if m_elsif:
            cond = m_elsif.group(1).strip()
            if not if_stack:
                if_stack.append({"branch": "ELSIF", "cond": cond, "if_start_line": i, "if_cond": cond})
            else:
                if_stack[-1]["branch"] = "ELSIF"
                if_stack[-1]["cond"] = cond
            continue

        if rx_else.match(line):
            if if_stack:
                if_stack[-1]["branch"] = "ELSE"
                if_stack[-1]["cond"] = ""
            continue

        if rx_end.match(line):
            if if_stack:
                if_stack.pop()
            continue

        m_assign = rx_assign.match(line)
        if not m_assign:
            continue

        rhs = m_assign.group(1).strip()
        conditions = _stack_repr()
        out.append(
            {
                "line_no": i,
                "assignment": raw.rstrip(),
                "rhs": rhs,
                "conditions": conditions,
                "conditions_conjunction": " AND ".join(conditions) if conditions else "(keine IF-Bedingung im Scope)",
            }
        )

    if trace:
        trace.log(f"[ST] Assignments for '{var_name}': {len(out)}")
    return out


def _rhs_bool_literal(rhs: str) -> Optional[bool]:
    r = (rhs or "").strip().upper()
    if r == "TRUE":
        return True
    if r == "FALSE":
        return False
    return None


def analyze_var_assignments_st(st_code: str, var_name: str, trace: Optional[Tracer] = None) -> Dict[str, Any]:
    all_assigns = extract_assignments_st(st_code, var_name, trace=None)
    merged: List[Dict[str, Any]] = []
    for a in all_assigns:
        lit = _rhs_bool_literal(a["rhs"])
        merged.append({**a, "value": ("TRUE" if lit is True else "FALSE" if lit is False else a["rhs"])})
    merged.sort(key=lambda x: x["line_no"])
    for idx, item in enumerate(merged):
        item["has_later_assignment"] = idx < len(merged) - 1
        item["later_assignments"] = [{"line_no": m["line_no"], "value": m["value"]} for m in merged[idx + 1 :]]
    summary = {"var_name": var_name, "assignment_count": len(merged), "last_assignment_in_code": merged[-1] if merged else None}
    if trace:
        trace.log(f"[ST] analyze_var_assignments_st('{var_name}') -> {len(merged)} assignments")
    return {"summary": summary, "assignments": merged}


class _BoolParser:
    def __init__(self, text: str):
        self.tokens = self._tokenize(text)
        self.pos = 0

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        token_re = re.compile(r"\s*(\(|\)|AND|OR|NOT|[A-Za-z_][A-Za-z0-9_\.]*|TRUE|FALSE)\s*", flags=re.I)
        out: List[str] = []
        idx = 0
        txt = text or ""
        while idx < len(txt):
            m = token_re.match(txt, idx)
            if not m:
                idx += 1
                continue
            out.append(m.group(1))
            idx = m.end()
        return out

    def _peek(self) -> Optional[str]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _eat(self) -> Optional[str]:
        tok = self._peek()
        if tok is not None:
            self.pos += 1
        return tok

    def parse(self) -> Any:
        if not self.tokens:
            return None
        return self._parse_or()

    def _parse_or(self) -> Any:
        node = self._parse_and()
        while True:
            tok = (self._peek() or "").upper()
            if tok != "OR":
                break
            self._eat()
            node = ("or", node, self._parse_and())
        return node

    def _parse_and(self) -> Any:
        node = self._parse_not()
        while True:
            tok = (self._peek() or "").upper()
            if tok != "AND":
                break
            self._eat()
            node = ("and", node, self._parse_not())
        return node

    def _parse_not(self) -> Any:
        tok = (self._peek() or "").upper()
        if tok == "NOT":
            self._eat()
            return ("not", self._parse_not())
        return self._parse_atom()

    def _parse_atom(self) -> Any:
        tok = self._peek()
        if tok is None:
            return None
        if tok == "(":
            self._eat()
            node = self._parse_or()
            if self._peek() == ")":
                self._eat()
            return node
        self._eat()
        up = tok.upper()
        if up == "TRUE":
            return ("lit", True)
        if up == "FALSE":
            return ("lit", False)
        return ("sym", tok)


def _negate_ast(ast: Any) -> Any:
    if ast is None:
        return None
    kind = ast[0]
    if kind == "lit":
        return ("lit", not ast[1])
    if kind == "sym":
        return ("not", ast)
    if kind == "not":
        return ast[1]
    if kind == "and":
        return ("or", _negate_ast(ast[1]), _negate_ast(ast[2]))
    if kind == "or":
        return ("and", _negate_ast(ast[1]), _negate_ast(ast[2]))
    return ("not", ast)


def _ast_to_dnf(ast: Any) -> List[List[Tuple[str, bool]]]:
    if ast is None:
        return []
    kind = ast[0]
    if kind == "lit":
        return [[]] if ast[1] else []
    if kind == "sym":
        return [[(ast[1], True)]]
    if kind == "not":
        sub = ast[1]
        if sub and sub[0] == "sym":
            return [[(sub[1], False)]]
        return _ast_to_dnf(_negate_ast(sub))
    if kind == "or":
        return _ast_to_dnf(ast[1]) + _ast_to_dnf(ast[2])
    if kind == "and":
        left = _ast_to_dnf(ast[1])
        right = _ast_to_dnf(ast[2])
        out: List[List[Tuple[str, bool]]] = []
        for l_path in left:
            for r_path in right:
                merged = l_path + r_path
                valid = True
                seen: Dict[str, bool] = {}
                for token, val in merged:
                    key = token.upper()
                    if key in seen and seen[key] != val:
                        valid = False
                        break
                    seen[key] = val
                if valid:
                    out.append(merged)
        return out
    return []


def condition_to_truth_paths(expr: str) -> List[List[Tuple[str, bool]]]:
    parser = _BoolParser(expr or "")
    ast = parser.parse()
    dnf_paths = _ast_to_dnf(ast)
    if dnf_paths:
        out: List[List[Tuple[str, bool]]] = []
        for path in dnf_paths:
            uniq: List[Tuple[str, bool]] = []
            used: Set[Tuple[str, bool]] = set()
            for token, req in path:
                item = (token.strip(), bool(req))
                if not item[0] or item in used:
                    continue
                used.add(item)
                uniq.append(item)
            if uniq:
                out.append(uniq)
        if out:
            return out

    tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_\.]*\b", expr or "")
    blacklist = {"IF", "THEN", "AND", "OR", "NOT", "TRUE", "FALSE", "ELSIF", "ELSE", "END_IF"}
    fallback = [(t, True) for t in tokens if t.upper() not in blacklist]
    return [fallback] if fallback else []


def _extract_condition_expr(condition_entry: str) -> str:
    if ":" not in (condition_entry or ""):
        return ""
    return condition_entry.split(":", 1)[1].strip()


def _find_first_call_block(st_code: str, inst_name: str) -> Optional[Dict[str, Any]]:
    if not st_code or not inst_name:
        return None
    lines = st_code.splitlines()
    start_pat = re.compile(rf"^\s*{re.escape(inst_name)}\s*\(", flags=re.I)
    collecting = False
    start_line = 0
    buf: List[str] = []
    balance = 0
    for idx, ln in enumerate(lines, start=1):
        if not collecting:
            if not start_pat.search(ln):
                continue
            collecting = True
            start_line = idx
            buf = [ln.rstrip()]
            balance = ln.count("(") - ln.count(")")
            if ln.strip().endswith(";") and balance <= 0:
                break
            continue
        buf.append(ln.rstrip())
        balance += ln.count("(") - ln.count(")")
        if ln.strip().endswith(";") and balance <= 0:
            break
    if not buf:
        return None
    call_text = " ".join(x.strip() for x in buf).strip()
    return {"line_no": start_line, "call_text": call_text}


def _parse_call_params(call_text: str) -> Dict[str, str]:
    if not call_text:
        return {}
    m = re.search(r"\((.*)\)\s*;?$", call_text)
    if not m:
        return {}
    inner = m.group(1).strip()
    out: Dict[str, str] = {}
    for part in _split_top_level_csv(inner):
        mm = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:=\s*(.+?)\s*$", part)
        if not mm:
            continue
        out[mm.group(1)] = mm.group(2)
    return out


def _find_var_default_in_st_declaration(st_code: str, var_name: str) -> Optional[str]:
    code = _strip_st_comments(st_code or "")
    name = str(var_name or "").strip()
    if not code or not name:
        return None
    m = re.search(
        rf"\b{re.escape(name)}\b\s*:\s*[A-Za-z_][A-Za-z0-9_]*\s*:=\s*([^;]+);",
        code,
        flags=re.I,
    )
    if not m:
        return None
    val = str(m.group(1) or "").strip()
    return val or None


def _lookup_instance_in_pou(graph: Graph, pou_uri: URIRef, instance_name: str) -> Optional[Dict[str, Any]]:
    needle = (instance_name or "").strip().lower()
    if not needle:
        return None

    # Nur Variablen im Kontext-POU betrachten.
    candidates: List[URIRef] = []
    seen: Set[URIRef] = set()
    for pred in (P_OP_HAS_INTERNAL_VARIABLE, P_OP_USES_VARIABLE):
        for var in graph.objects(pou_uri, pred):
            if not isinstance(var, URIRef) or var in seen:
                continue
            seen.add(var)
            candidates.append(var)

    for var in candidates:
        fb_inst = _first(graph, var, P_OP_REPRESENTS_FB_INSTANCE)
        if not fb_inst:
            continue
        fb_inst_uri = URIRef(fb_inst)

        var_name = _as_text(_first(graph, var, P_DP_HAS_VARIABLE_NAME)).strip()
        inst_name = _as_text(_first(graph, fb_inst_uri, P_DP_HAS_FBINSTANCE_NAME)).strip()
        names = {n.lower() for n in (var_name, inst_name) if n}
        if needle not in names:
            continue

        fb_type = _first(graph, fb_inst_uri, P_OP_IS_INSTANCE_OF_FBTYPE)
        if not fb_type:
            continue
        fb_type_uri = URIRef(fb_type)
        fb_desc = _as_text(_first(graph, fb_type_uri, P_DP_HAS_FBTYPE_DESCRIPTION))
        if not fb_desc:
            fb_desc = _as_text(_first(graph, fb_type_uri, RDFS.comment))
        return {
            "var_uri": var,
            "fb_inst_uri": fb_inst_uri,
            "fb_type_uri": fb_type_uri,
            "fb_type_name": get_pou_name(graph, fb_type_uri),
            "fb_type_lang": get_pou_language(graph, fb_type_uri),
            "fb_type_desc": fb_desc,
            "instance_name": inst_name or var_name,
        }
    return None


def _fb_instance_belongs_to_pou(graph: Graph, pou_uri: URIRef, fb_inst_uri: URIRef) -> bool:
    """True, wenn die FB-Instanz über eine interne/benutzte Variable im POU hängt."""
    for var in graph.subjects(P_OP_REPRESENTS_FB_INSTANCE, fb_inst_uri):
        if (pou_uri, P_OP_HAS_INTERNAL_VARIABLE, var) in graph or (pou_uri, P_OP_USES_VARIABLE, var) in graph:
            return True
    return False


def _lookup_instance_port_by_expression(graph: Graph, pou_uri: URIRef, token: str) -> Optional[Dict[str, Any]]:
    """
    Löst ein Instanz-Port-Token (z.B. rStep1.Q) primär über materialisierte PortInstances
    mit dp:hasExpressionText auf, kontextgebunden auf das aktuelle POU.
    """
    expr = (token or "").strip()
    if "." not in expr:
        return None
    expr_norm = expr.lower()

    # Erst exakter Treffer, dann case-insensitive Fallback.
    candidates: List[URIRef] = []
    for pi in graph.subjects(P_DP_HAS_EXPRESSION_TEXT, Literal(expr)):
        if isinstance(pi, URIRef):
            candidates.append(pi)

    if not candidates:
        for pi, lit in graph.subject_objects(P_DP_HAS_EXPRESSION_TEXT):
            if not isinstance(pi, URIRef):
                continue
            if str(lit).strip().lower() == expr_norm:
                candidates.append(pi)

    for pi_uri in candidates:
        if (pi_uri, RDF.type, CLASS_PORT_INSTANCE) not in graph:
            continue
        fb_inst = _first(graph, pi_uri, P_OP_IS_PORT_OF_INSTANCE)
        if not fb_inst:
            continue
        fb_inst_uri = URIRef(fb_inst)
        if not _fb_instance_belongs_to_pou(graph, pou_uri, fb_inst_uri):
            continue

        fb_type = _first(graph, fb_inst_uri, P_OP_IS_INSTANCE_OF_FBTYPE)
        if not fb_type:
            continue
        fb_type_uri = URIRef(fb_type)

        expr_text = _as_text(_first(graph, pi_uri, P_DP_HAS_EXPRESSION_TEXT)) or expr
        inst_name = _as_text(_first(graph, fb_inst_uri, P_DP_HAS_FBINSTANCE_NAME)).strip()
        if not inst_name:
            inst_name = expr_text.split(".", 1)[0].strip()
        port = expr_text.split(".", 1)[1].strip() if "." in expr_text else expr.split(".", 1)[1].strip()

        fb_desc = _as_text(_first(graph, fb_type_uri, P_DP_HAS_FBTYPE_DESCRIPTION))
        if not fb_desc:
            fb_desc = _as_text(_first(graph, fb_type_uri, RDFS.comment))

        var_uri: Optional[URIRef] = None
        for v in graph.subjects(P_OP_REPRESENTS_FB_INSTANCE, fb_inst_uri):
            if (pou_uri, P_OP_HAS_INTERNAL_VARIABLE, v) in graph or (pou_uri, P_OP_USES_VARIABLE, v) in graph:
                var_uri = URIRef(v)
                break

        return {
            "var_uri": var_uri,
            "port_instance_uri": pi_uri,
            "expression_text": expr_text,
            "fb_inst_uri": fb_inst_uri,
            "fb_type_uri": fb_type_uri,
            "fb_type_name": get_pou_name(graph, fb_type_uri),
            "fb_type_lang": get_pou_language(graph, fb_type_uri),
            "fb_type_desc": fb_desc,
            "instance_name": inst_name,
            "port_name": port,
        }

    return None


def _lookup_global_variable_by_name(graph: Graph, token: str) -> Optional[URIRef]:
    exact = list(graph.subjects(P_DP_HAS_VARIABLE_NAME, Literal(token)))
    if exact:
        return exact[0]
    if "." in token:
        prefix, suffix = token.split(".", 1)
        if prefix.upper() in {"GVL", "OPCUA"}:
            by_suffix = list(graph.subjects(P_DP_HAS_VARIABLE_NAME, Literal(suffix)))
            if len(by_suffix) == 1:
                return by_suffix[0]
    return None


def _variable_metadata(graph: Graph, var_uri: URIRef) -> Dict[str, Any]:
    return {
        "var_uri": str(var_uri),
        "var_name": _as_text(_first(graph, var_uri, P_DP_HAS_VARIABLE_NAME)),
        "var_type": _as_text(_first(graph, var_uri, P_DP_HAS_VARIABLE_TYPE)),
        "default_variable_value": _as_text(_first(graph, var_uri, P_DP_HAS_DEFAULT_VARIABLE_VALUE)),
        "hardware_address": _as_text(_first(graph, var_uri, P_DP_HAS_HARDWARE_ADDRESS)),
        "opcua_data_access": _as_text(_first(graph, var_uri, P_DP_HAS_OPCUA_DATA_ACCESS)),
        "opcua_write_access": _as_text(_first(graph, var_uri, P_DP_HAS_OPCUA_WRITE_ACCESS)),
    }


def _is_job_method_fb(graph: Graph, fb_uri: Optional[URIRef]) -> bool:
    if fb_uri is None:
        return False
    return _literal_is_true(_first(graph, fb_uri, P_DP_IS_JOB_METHOD))


def _get_job_method_owner_fb(graph: Graph, ctx_pou_uri: URIRef) -> Optional[URIRef]:
    if (ctx_pou_uri, RDF.type, CLASS_METHOD) not in graph:
        return None
    owner = _first(graph, ctx_pou_uri, P_OP_IS_METHOD_OF)
    if isinstance(owner, URIRef) and _is_job_method_fb(graph, owner):
        return owner
    return None


def _context_bundle(graph: Graph, pou_uri: URIRef) -> Dict[str, Any]:
    return {
        "pou_uri": pou_uri,
        "pou_name": get_pou_name(graph, pou_uri),
        "code": get_pou_code(graph, pou_uri),
        "lang": get_pou_language(graph, pou_uri),
    }


def _effective_trace_context(
    graph: Graph,
    cls: Dict[str, Any],
    fallback_pou_uri: URIRef,
    fallback_code: str,
    fallback_lang: str,
) -> Dict[str, Any]:
    declared_txt = str(cls.get("declared_in_pou_uri", "") or "").strip()
    if declared_txt:
        declared_uri = URIRef(declared_txt)
        if str(declared_uri) != str(fallback_pou_uri):
            return _context_bundle(graph, declared_uri)
    return {
        "pou_uri": fallback_pou_uri,
        "pou_name": get_pou_name(graph, fallback_pou_uri),
        "code": fallback_code,
        "lang": fallback_lang or get_pou_language(graph, fallback_pou_uri),
    }


def _job_method_contexts(graph: Graph, fb_uri: URIRef) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    rels = [
        (P_OP_HAS_START_METHOD, "Start"),
        (P_OP_HAS_ABORT_METHOD, "Abort"),
        (P_OP_HAS_CHECK_STATE_METHOD, "CheckState"),
        (P_OP_HAS_METHOD, ""),
    ]
    for pred, default_type in rels:
        for method_uri in graph.objects(fb_uri, pred):
            if not isinstance(method_uri, URIRef):
                continue
            key = str(method_uri)
            if key in seen:
                continue
            seen.add(key)
            method_type = _as_text(_first(graph, method_uri, P_DP_HAS_METHOD_TYPE)) or default_type
            bundle = _context_bundle(graph, method_uri)
            bundle["method_type"] = method_type
            bundle["owner_fb_uri"] = str(fb_uri)
            bundle["owner_fb_name"] = get_pou_name(graph, fb_uri)
            out.append(bundle)
    return out


def _pick_assignment_for_requirement(assignments: List[Dict[str, Any]], required_value: bool) -> Optional[Dict[str, Any]]:
    if not assignments:
        return None
    wanted = [a for a in assignments if _rhs_bool_literal(a.get("rhs", "")) == required_value]
    if wanted:
        return wanted[-1]
    non_literal = [a for a in assignments if _rhs_bool_literal(a.get("rhs", "")) is None and str(a.get("rhs", "")).strip()]
    if non_literal:
        return non_literal[-1]
    return assignments[-1]


def _score_assignment_candidate(candidate: Dict[str, Any], required_value: bool) -> int:
    rhs_lit = _rhs_bool_literal(candidate.get("rhs", ""))
    source_kind = str(candidate.get("source_kind", "")).strip().lower()
    method_type = str(candidate.get("method_type", "")).strip().lower()
    score = 0

    if rhs_lit is required_value:
        score += 120
    elif rhs_lit is None and str(candidate.get("rhs", "")).strip():
        score += 70
    else:
        score += 10

    if source_kind == "local":
        score += 40
    elif source_kind == "job_method":
        score += 20

    if method_type == "start":
        score += 35 if required_value else -5
    elif method_type == "abort":
        score += 35 if not required_value else -5
    elif method_type == "checkstate":
        score += 5

    return score


def _collect_job_method_assignment_candidates(
    graph: Graph,
    fb_uri: URIRef,
    token: str,
    required_value: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ctx in _job_method_contexts(graph, fb_uri):
        method_uri = ctx.get("pou_uri")
        if not isinstance(method_uri, URIRef):
            continue
        code = str(ctx.get("code", "") or "")
        assigns = extract_assignments_st(code, token, trace=None)
        chosen = _pick_assignment_for_requirement(assigns, required_value)
        if not chosen:
            continue
        rec = dict(chosen)
        rec["source_kind"] = "job_method"
        rec["method_uri"] = str(method_uri)
        rec["method_type"] = str(ctx.get("method_type", "") or "")
        rec["method_pou_name"] = str(ctx.get("pou_name", "") or "")
        rec["method_display_name"] = f"{ctx.get('owner_fb_name', '')}.{ctx.get('pou_name', '')}".strip(".")
        rec["code"] = code
        rec["lang"] = str(ctx.get("lang", "") or "")
        rec["_candidate_score"] = _score_assignment_candidate(rec, required_value)
        out.append(rec)
    out.sort(
        key=lambda x: (
            -_safe_int(x.get("_candidate_score", 0), 0),
            _safe_int(x.get("line_no", 999999), 999999),
            str(x.get("method_display_name", "")),
        )
    )
    return out


def _classify_token(graph: Graph, token: str, ctx_pou_uri: URIRef, trace: Optional[Tracer] = None) -> Dict[str, Any]:
    token = (token or "").strip()
    if not token:
        return {"kind": "empty", "token": token}

    owner_fb_uri = _get_job_method_owner_fb(graph, ctx_pou_uri)

    if "." in token:
        pi_obj = _lookup_instance_port_by_expression(graph, ctx_pou_uri, token)
        if pi_obj:
            out = {
                "kind": "instance_port",
                "token": token,
                "instance": pi_obj.get("instance_name", token.split(".", 1)[0].strip()),
                "port": pi_obj.get("port_name", token.split(".", 1)[1].strip()),
                "declared_in_pou_uri": str(ctx_pou_uri),
                **pi_obj,
            }
            if trace:
                trace.log(f"[CLS] token='{token}' kind=instance_port (via PortInstance)")
            return out

        inst, _, port = token.partition(".")
        inst_obj = _lookup_instance_in_pou(graph, ctx_pou_uri, inst)
        if inst_obj:
            out = {
                "kind": "instance_port",
                "token": token,
                "instance": inst_obj.get("instance_name", inst),
                "port": port,
                "declared_in_pou_uri": str(ctx_pou_uri),
                **inst_obj,
            }
            if trace:
                trace.log(f"[CLS] token='{token}' kind=instance_port")
            return out
        if owner_fb_uri is not None:
            pi_obj = _lookup_instance_port_by_expression(graph, owner_fb_uri, token)
            if pi_obj:
                out = {
                    "kind": "instance_port",
                    "token": token,
                    "instance": pi_obj.get("instance_name", token.split(".", 1)[0].strip()),
                    "port": pi_obj.get("port_name", token.split(".", 1)[1].strip()),
                    "declared_in_pou_uri": str(owner_fb_uri),
                    "resolved_via_owner_fb": True,
                    **pi_obj,
                }
                if trace:
                    trace.log(f"[CLS] token='{token}' kind=instance_port (via owner job-method FB)")
                return out

            inst_obj = _lookup_instance_in_pou(graph, owner_fb_uri, inst)
            if inst_obj:
                out = {
                    "kind": "instance_port",
                    "token": token,
                    "instance": inst_obj.get("instance_name", inst),
                    "port": port,
                    "declared_in_pou_uri": str(owner_fb_uri),
                    "resolved_via_owner_fb": True,
                    **inst_obj,
                }
                if trace:
                    trace.log(f"[CLS] token='{token}' kind=instance_port (owner job-method FB)")
                return out
        gv = _lookup_global_variable_by_name(graph, token)
        if gv:
            out = {"kind": "global_variable", "token": token, **_variable_metadata(graph, gv)}
            if trace:
                trace.log(f"[CLS] token='{token}' kind=global_variable")
            return out
        if trace:
            trace.log(f"[CLS] token='{token}' kind=unknown reason=dot_token->unresolved")
        return {"kind": "unknown", "token": token, "reason": "dot_token->unresolved"}

    port_uri = get_port_by_name(graph, ctx_pou_uri, token)
    if port_uri:
        out = {
            "kind": "local_port",
            "token": token,
            "port_uri": port_uri,
            "direction": get_port_direction(graph, port_uri),
            "default_value": get_port_default_value(graph, port_uri),
            "declared_in_pou_uri": str(ctx_pou_uri),
        }
        if trace:
            trace.log(f"[CLS] token='{token}' kind=local_port direction={out['direction']}")
        return out

    local_var_hits = list(graph.subjects(P_DP_HAS_VARIABLE_NAME, Literal(token)))
    chosen_var: Optional[URIRef] = None
    for var in local_var_hits:
        if (ctx_pou_uri, P_OP_HAS_INTERNAL_VARIABLE, var) in graph or (ctx_pou_uri, P_OP_USES_VARIABLE, var) in graph:
            chosen_var = var
            break
    if chosen_var is not None:
        out = {
            "kind": "internal_variable",
            "token": token,
            "declared_in_pou_uri": str(ctx_pou_uri),
            **_variable_metadata(graph, chosen_var),
        }
        if trace:
            trace.log(f"[CLS] token='{token}' kind=internal_variable")
        return out

    if owner_fb_uri is not None:
        owner_port_uri = get_port_by_name(graph, owner_fb_uri, token)
        if owner_port_uri:
            out = {
                "kind": "local_port",
                "token": token,
                "port_uri": owner_port_uri,
                "direction": get_port_direction(graph, owner_port_uri),
                "default_value": get_port_default_value(graph, owner_port_uri),
                "declared_in_pou_uri": str(owner_fb_uri),
                "resolved_via_owner_fb": True,
            }
            if trace:
                trace.log(f"[CLS] token='{token}' kind=local_port (owner job-method FB)")
            return out

        owner_var_hits = list(graph.subjects(P_DP_HAS_VARIABLE_NAME, Literal(token)))
        chosen_owner_var: Optional[URIRef] = None
        for var in owner_var_hits:
            if (owner_fb_uri, P_OP_HAS_INTERNAL_VARIABLE, var) in graph or (owner_fb_uri, P_OP_USES_VARIABLE, var) in graph:
                chosen_owner_var = var
                break
        if chosen_owner_var is not None:
            out = {
                "kind": "internal_variable",
                "token": token,
                "declared_in_pou_uri": str(owner_fb_uri),
                "resolved_via_owner_fb": True,
                **_variable_metadata(graph, chosen_owner_var),
            }
            if trace:
                trace.log(f"[CLS] token='{token}' kind=internal_variable (owner job-method FB)")
            return out

    gv = _lookup_global_variable_by_name(graph, token)
    if gv:
        out = {"kind": "global_variable", "token": token, **_variable_metadata(graph, gv)}
        if trace:
            trace.log(f"[CLS] token='{token}' kind=global_variable")
        return out

    if trace:
        trace.log(f"[CLS] token='{token}' kind=unknown")
    return {"kind": "unknown", "token": token}


def _standard_fb_q_requirements(fb_type_name: str) -> List[Tuple[str, str]]:
    n = (fb_type_name or "").upper()
    if "R_TRIG" in n:
        return [("CLK", "TRUE")]
    if "TON" in n:
        return [("IN", "TRUE"), ("PT", "elapsed")]
    if "TP" in n:
        return [("IN", "TRUE"), ("PT", "elapsed")]
    return []


def _add_requirement_row(
    rows: List[Dict[str, Any]],
    *,
    path_id: str,
    token: str,
    value: str,
    pou_name: str,
    kind: str,
    reason: str,
    depth: int,
    source: str,
    assignment: str = "",
    fb_type: str = "",
    fb_type_description: str = "",
) -> None:
    rows.append(
        {
            "path_id": path_id,
            "depth": depth,
            "token": token,
            "value": value,
            "pou": pou_name,
            "kind": kind,
            "reason": reason,
            "source": source,
            "assignment": assignment,
            "fb_type": fb_type,
            "fb_type_description": fb_type_description,
        }
    )


def _trace_expression_requirements(
    *,
    graph: Graph,
    expr: str,
    required_value: bool,
    ctx_pou_uri: URIRef,
    ctx_code: str,
    ctx_lang: str,
    path_id: str,
    depth: int,
    max_depth: int,
    trace: Tracer,
    rows: List[Dict[str, Any]],
    visiting: Set[Tuple[str, str, str]],
    assumed_false_states: Set[str],
    expand_false: bool = True,
    token_true_overrides: Optional[Dict[str, str]] = None,
) -> None:
    if _is_time_literal(expr):
        _add_requirement_row(
            rows,
            path_id=path_id,
            token=expr,
            value=expr,
            pou_name=get_pou_name(graph, ctx_pou_uri),
            kind="time_literal",
            reason="Zeitliteral",
            depth=depth,
            source="literal",
        )
        return

    paths = condition_to_truth_paths(expr if required_value else f"NOT ({expr})")
    if not paths:
        return
    if len(paths) > 1:
        trace.log(f"{'  '*depth}[EXPR] '{expr}' -> {len(paths)} path branches")
    for idx, path in enumerate(paths, start=1):
        cur_path = path_id if len(paths) == 1 else f"{path_id}.{idx}"
        for tok, req in path:
            _trace_token(
                graph=graph,
                token=tok,
                required_value=req,
                ctx_pou_uri=ctx_pou_uri,
                ctx_code=ctx_code,
                ctx_lang=ctx_lang,
                path_id=cur_path,
                depth=depth + 1,
                max_depth=max_depth,
                trace=trace,
                rows=rows,
                visiting=visiting,
                assumed_false_states=assumed_false_states,
                expand_false=expand_false,
                token_true_overrides=token_true_overrides,
            )


def _trace_token(
    *,
    graph: Graph,
    token: str,
    required_value: bool,
    ctx_pou_uri: URIRef,
    ctx_code: str,
    ctx_lang: str,
    path_id: str,
    depth: int,
    max_depth: int,
    trace: Tracer,
    rows: List[Dict[str, Any]],
    visiting: Set[Tuple[str, str, str]],
    assumed_false_states: Set[str],
    expand_false: bool = True,
    token_true_overrides: Optional[Dict[str, str]] = None,
) -> None:
    token = (token or "").strip()
    if not token:
        return

    pou_name = get_pou_name(graph, ctx_pou_uri)
    req_text = "TRUE" if required_value else "FALSE"
    indent = "  " * depth

    if (not expand_false) and (required_value is False):
        _add_requirement_row(
            rows,
            path_id=path_id,
            token=token,
            value=req_text,
            pou_name=pou_name,
            kind="guard_false",
            reason="Negierte Guard-Bedingung (Pre-State)",
            depth=depth,
            source="guard",
        )

    if depth >= max_depth:
        trace.log(f"{indent}[STOP] max_depth erreicht bei '{token}'")
        _add_requirement_row(
            rows,
            path_id=path_id,
            token=token,
            value=req_text,
            pou_name=pou_name,
            kind="stop",
            reason="max_depth",
            depth=depth,
            source="guard",
        )
        return

    if token.upper() in assumed_false_states and required_value is False:
        trace.log(f"{indent}[ASSUME] {token} wird im GEMMA-Kontext als FALSE angenommen -> Skip")
        _add_requirement_row(
            rows,
            path_id=path_id,
            token=token,
            value="FALSE (assumed)",
            pou_name=pou_name,
            kind="assumption",
            reason="gemma_assumed_false",
            depth=depth,
            source="assumption",
        )
        return

    visit_key = (str(ctx_pou_uri), token.upper(), req_text)
    if visit_key in visiting:
        trace.log(f"{indent}(loop detected) {token}")
        _add_requirement_row(
            rows,
            path_id=path_id,
            token=token,
            value=req_text,
            pou_name=pou_name,
            kind="loop",
            reason="loop detected",
            depth=depth,
            source="trace",
        )
        return
    visiting.add(visit_key)

    try:
        cls = _classify_token(graph, token, ctx_pou_uri, trace=trace)
        kind = cls.get("kind")

        if kind == "instance_port":
            token_ctx = _effective_trace_context(graph, cls, ctx_pou_uri, ctx_code, ctx_lang)
            token_ctx_pou_uri = token_ctx["pou_uri"]
            token_ctx_code = str(token_ctx.get("code", "") or "")
            token_ctx_lang = str(token_ctx.get("lang", "") or "")
            token_ctx_name = str(token_ctx.get("pou_name", "") or pou_name)
            inst = cls["instance"]
            port = cls["port"]
            fb_type_uri = cls["fb_type_uri"]
            fb_type_name = cls.get("fb_type_name", "")
            fb_lang = (cls.get("fb_type_lang") or "").upper()
            fb_desc = cls.get("fb_type_desc") or ""
            port_uri = get_port_by_name(graph, fb_type_uri, port)
            port_dir = get_port_direction(graph, port_uri) if port_uri else ""
            call_block = _find_first_call_block(token_ctx_code, inst)
            call_line = call_block.get("line_no") if call_block else None
            call_text = call_block.get("call_text") if call_block else ""
            call_params = _parse_call_params(call_text)

            trace.log(f"{indent}{token} ist Port '{port}' der FB-Instanz '{inst}' (Typ={fb_type_name})")
            if fb_desc:
                trace.log(f"{indent}  FBType-Beschreibung: {fb_desc}")
            if call_line:
                trace.log(f"{indent}  Call-Block @ {call_line}: {call_text}")
            else:
                trace.log(f"{indent}  Keine Call-Line '{inst}(...)' im ST-Code gefunden.")

            _add_requirement_row(
                rows,
                path_id=path_id,
                token=token,
                value=req_text,
                pou_name=token_ctx_name,
                kind="instance_port",
                reason=f"Instanzport ({port_dir or 'unbekannt'})",
                depth=depth,
                source="token",
                assignment=call_text,
                fb_type=fb_type_name,
                fb_type_description=fb_desc,
            )

            if port_dir == "Input":
                expr = call_params.get(port)
                if not expr:
                    _add_requirement_row(
                        rows,
                        path_id=path_id,
                        token=f"{inst}.{port}",
                        value=req_text,
                        pou_name=pou_name,
                        kind="terminal",
                        reason="Input-Port ohne Parameterzuweisung im Call",
                        depth=depth + 1,
                        source="call",
                    )
                else:
                    trace.log(f"{indent}  Param {port} = {expr}")
                    _trace_expression_requirements(
                        graph=graph,
                        expr=expr,
                        required_value=required_value,
                        ctx_pou_uri=token_ctx_pou_uri,
                        ctx_code=token_ctx_code,
                        ctx_lang=token_ctx_lang,
                        path_id=path_id,
                        depth=depth + 1,
                        max_depth=max_depth,
                        trace=trace,
                        rows=rows,
                        visiting=visiting,
                        assumed_false_states=assumed_false_states,
                        expand_false=expand_false,
                        token_true_overrides=token_true_overrides,
                    )
                return

            if port_dir == "Output" and port.upper() == "Q":
                deps = _standard_fb_q_requirements(fb_type_name)
                if deps:
                    for dep_name, dep_req in deps:
                        dep_expr = call_params.get(dep_name, "")
                        dep_value = f"{dep_expr}, abgelaufen" if dep_req == "elapsed" and dep_expr else dep_req
                        _add_requirement_row(
                            rows,
                            path_id=path_id,
                            token=f"{inst}.{dep_name}",
                            value=dep_value,
                            pou_name=pou_name,
                            kind="std_fb_dependency",
                            reason=f"{token} erfordert {dep_name}",
                            depth=depth + 1,
                            source="standard_fb",
                            fb_type=fb_type_name,
                            fb_type_description=fb_desc,
                        )
                        if dep_expr:
                            trace.log(f"{indent}  Param {dep_name} = {dep_expr}")
                            if dep_req == "TRUE":
                                # Selbstreferenz wie tPer(IN := NOT tPer.Q):
                                # Q=TRUE (aktueller Zyklus) benötigt IN=TRUE, was aus NOT Q (Vorzyklus) stammen kann.
                                if dep_name.upper() == "IN" and re.fullmatch(
                                    rf"\s*NOT\s+{re.escape(token)}\s*",
                                    dep_expr,
                                    flags=re.I,
                                ):
                                    _add_requirement_row(
                                        rows,
                                        path_id=path_id,
                                        token=token,
                                        value="FALSE (t-1)",
                                        pou_name=pou_name,
                                        kind="temporal_note",
                                        reason="Selbstreferenz IN := NOT Q (Vorzyklus)",
                                        depth=depth + 2,
                                        source="temporal",
                                        assignment=f"{inst}(IN := NOT {token}, ...)",
                                        fb_type=fb_type_name,
                                        fb_type_description=fb_desc,
                                    )
                                    continue
                                _trace_expression_requirements(
                                    graph=graph,
                                    expr=dep_expr,
                                    required_value=True,
                                    ctx_pou_uri=ctx_pou_uri,
                                    ctx_code=ctx_code,
                                    ctx_lang=ctx_lang,
                                    path_id=path_id,
                                    depth=depth + 1,
                                    max_depth=max_depth,
                                    trace=trace,
                                    rows=rows,
                                    visiting=visiting,
                                    assumed_false_states=assumed_false_states,
                                    expand_false=expand_false,
                                    token_true_overrides=token_true_overrides,
                                )
                            elif dep_req == "elapsed" and not _is_time_literal(dep_expr):
                                _trace_expression_requirements(
                                    graph=graph,
                                    expr=dep_expr,
                                    required_value=True,
                                    ctx_pou_uri=ctx_pou_uri,
                                    ctx_code=ctx_code,
                                    ctx_lang=ctx_lang,
                                    path_id=path_id,
                                    depth=depth + 1,
                                    max_depth=max_depth,
                                    trace=trace,
                                    rows=rows,
                                    visiting=visiting,
                                    assumed_false_states=assumed_false_states,
                                    expand_false=expand_false,
                                    token_true_overrides=token_true_overrides,
                                )
                    return

            callee_code = get_pou_code(graph, fb_type_uri)
            if not callee_code:
                _add_requirement_row(
                    rows,
                    path_id=path_id,
                    token=token,
                    value=req_text,
                    pou_name=get_pou_name(graph, fb_type_uri),
                    kind="terminal",
                    reason="Kein callee_code",
                    depth=depth + 1,
                    source="callee",
                )
                return

                trace.log(f"{indent}  [KG] Instanz-Typ: {fb_type_uri} (lang={fb_lang})")
                trace.log(f"{indent}  [KG] Port '{port}' direction im callee: {port_dir}")

            if fb_lang == "FBD":
                out_trace = trace_symbol_from_fbd(callee_code, port, target_value=req_text, trace=None)
                if "error" in out_trace:
                    trace.log(f"{indent}  [FBD] {out_trace['error']}")
                    return
                focus_tree = out_trace.get("focus_tree") or out_trace.get("set_tree") or out_trace.get("tree")
                leaves = _collect_leaf_tokens(focus_tree) if focus_tree else []
                uniq: List[str] = []
                for leaf in leaves:
                    if not leaf or _is_internal_v(leaf) or leaf in uniq:
                        continue
                    uniq.append(leaf)
                trace.log(f"{indent}  [FBD] Leaf tokens (uniq): {uniq}")
                callee_lang = get_pou_language(graph, fb_type_uri)
                for leaf in uniq:
                    _trace_token(
                        graph=graph,
                        token=leaf,
                        required_value=True,
                        ctx_pou_uri=fb_type_uri,
                        ctx_code=callee_code,
                        ctx_lang=callee_lang,
                        path_id=path_id,
                        depth=depth + 1,
                        max_depth=max_depth,
                        trace=trace,
                        rows=rows,
                        visiting=visiting,
                        assumed_false_states=assumed_false_states,
                        expand_false=expand_false,
                        token_true_overrides=token_true_overrides,
                    )
                return

            assigns = extract_assignments_st(callee_code, port, trace=None)
            if not assigns:
                trace.log(f"{indent}  [ST] Trace Output '{port}' im callee (ST) -> Assignments 0")
                _add_requirement_row(
                    rows,
                    path_id=path_id,
                    token=token,
                    value=req_text,
                    pou_name=get_pou_name(graph, fb_type_uri),
                    kind="terminal",
                    reason="Output ohne ST-Assignments",
                    depth=depth + 1,
                    source="callee_st",
                )
                return
            chosen = assigns[-1]
            trace.log(f"{indent}  [ST] Callee assignment @ {chosen['line_no']}: {chosen['assignment'].strip()}")
            _add_requirement_row(
                rows,
                path_id=path_id,
                token=token,
                value=req_text,
                pou_name=get_pou_name(graph, fb_type_uri),
                kind="output_assignment",
                reason=f"Callee-Output-Assignment @ {chosen['line_no']}",
                depth=depth + 1,
                source="assignment",
                assignment=str(chosen.get("assignment", "")).strip(),
            )
            for cond in chosen.get("conditions", []):
                cond_expr = _extract_condition_expr(cond)
                if cond_expr:
                    _trace_expression_requirements(
                        graph=graph,
                        expr=cond_expr,
                        required_value=True,
                        ctx_pou_uri=fb_type_uri,
                        ctx_code=callee_code,
                        ctx_lang=fb_lang,
                        path_id=path_id,
                        depth=depth + 1,
                        max_depth=max_depth,
                        trace=trace,
                        rows=rows,
                        visiting=visiting,
                        assumed_false_states=assumed_false_states,
                        expand_false=expand_false,
                        token_true_overrides=token_true_overrides,
                    )
            rhs = chosen.get("rhs", "")
            rhs_lit = _rhs_bool_literal(rhs)
            if rhs_lit is None:
                _trace_expression_requirements(
                    graph=graph,
                    expr=rhs,
                    required_value=required_value,
                    ctx_pou_uri=fb_type_uri,
                    ctx_code=callee_code,
                    ctx_lang=fb_lang,
                    path_id=path_id,
                    depth=depth + 1,
                    max_depth=max_depth,
                    trace=trace,
                    rows=rows,
                    visiting=visiting,
                    assumed_false_states=assumed_false_states,
                    expand_false=expand_false,
                    token_true_overrides=token_true_overrides,
                )
            return

        if kind == "local_port":
            local_ctx = _effective_trace_context(graph, cls, ctx_pou_uri, ctx_code, ctx_lang)
            local_ctx_pou_uri = local_ctx["pou_uri"]
            local_ctx_code = str(local_ctx.get("code", "") or "")
            local_ctx_lang = str(local_ctx.get("lang", "") or "")
            local_pou_name = str(local_ctx.get("pou_name", "") or pou_name)
            direction = _norm_dir(cls.get("direction") or "")
            default_value = cls.get("default_value")
            trace.log(f"{indent}'{token}' ist {direction or 'unbekannter'}-Port von {local_pou_name}")
            if direction != "Input":
                _add_requirement_row(
                    rows,
                    path_id=path_id,
                    token=token,
                    value=req_text,
                    pou_name=local_pou_name,
                    kind="local_port",
                    reason=f"{direction or 'Port'}",
                    depth=depth,
                    source="token",
                )
            if direction == "Input":
                origin = resolve_input_to_upstream_output(graph, local_ctx_pou_uri, token)
                if "error" in origin:
                    trace.log(f"{indent}  {origin['error']}")
                    return
                if not origin.get("is_wired"):
                    method_owner_fb = _get_job_method_owner_fb(graph, local_ctx_pou_uri)
                    if method_owner_fb is not None:
                        method_type = _as_text(_first(graph, local_ctx_pou_uri, P_DP_HAS_METHOD_TYPE)) or local_pou_name
                        owner_name = get_pou_name(graph, method_owner_fb)
                        display_name = f"{owner_name}.{local_pou_name}".strip(".")
                        trace.log(f"{indent}  Job-Method Input erkannt -> externer TcRpc/OPC-UA-Aufruf.")
                        _add_requirement_row(
                            rows,
                            path_id=path_id,
                            token=token,
                            value=req_text,
                            pou_name=display_name,
                            kind="job_method_input",
                            reason=f"Job-Method Input ({method_type}) via TcRpc/OPC UA",
                            depth=depth + 1,
                            source="job_method",
                            assignment=f"{display_name}({token})",
                        )
                        return

                    st_decl_default = _find_var_default_in_st_declaration(local_ctx_code, token)
                    default_effective = default_value if default_value is not None else st_decl_default
                    if default_effective is None:
                        default_effective = "DEFAULT_UNBEKANNT"
                    trace.log(f"{indent}  default_value = {default_value}")
                    trace.log(f"{indent}  is_wired = False (assignments=0)")
                    trace.log(f"{indent}  => nicht verdrahtet, daher wird DefaultValue verwendet (Trace endet hier).")
                    _add_requirement_row(
                        rows,
                        path_id=path_id,
                        token=token,
                        value=default_effective,
                        pou_name=local_pou_name,
                        kind="terminal",
                        reason="Input unverdrahtet -> DefaultValue",
                        depth=depth + 1,
                        source="default",
                        assignment=f"default={default_effective}",
                    )
                    return
                caller_pou_uri = URIRef(origin["caller_pou_uri"])
                caller_port = origin.get("caller_port_name", "")
                caller_code = origin.get("caller_code", "") or get_pou_code(graph, caller_pou_uri)
                caller_lang = get_pou_language(graph, caller_pou_uri)
                trace.log(f"{indent}  Verdrahtet von {origin.get('caller_pou_name')}:{caller_port}")
                caller_pou_name = str(origin.get("caller_pou_name", "") or "").strip()
                caller_ref = f"{caller_pou_name}.{caller_port}".strip(".") if caller_pou_name and caller_pou_name != "-" else str(caller_port)
                mapping_txt = f"{token} <= {caller_ref}".strip()
                if origin.get("assignment"):
                    mapping_txt = f"{mapping_txt} [{origin.get('assignment')}]".strip()
                _add_requirement_row(
                    rows,
                    path_id=path_id,
                    token=token,
                    value=req_text,
                    pou_name=local_pou_name,
                    kind="port_mapping",
                    reason="Input-Port Verdrahtung",
                    depth=depth + 1,
                    source="wiring",
                    assignment=mapping_txt,
                )
                override_expr = ""
                if required_value and isinstance(token_true_overrides, dict):
                    override_expr = str(token_true_overrides.get(str(caller_port), "") or "").strip()
                if override_expr and override_expr.upper() != str(caller_port).upper():
                    _add_requirement_row(
                        rows,
                        path_id=path_id,
                        token=str(caller_port),
                        value="TRUE",
                        pou_name=origin.get("caller_pou_name", "") or get_pou_name(graph, caller_pou_uri),
                        kind="branch_hint",
                        reason="LastGEMMA/State-Branch-Hint",
                        depth=depth + 1,
                        source="hint",
                        assignment=f"{caller_port} => {override_expr}",
                    )
                    _trace_expression_requirements(
                        graph=graph,
                        expr=override_expr,
                        required_value=True,
                        ctx_pou_uri=caller_pou_uri,
                        ctx_code=caller_code,
                        ctx_lang=caller_lang,
                        path_id=path_id,
                        depth=depth + 1,
                        max_depth=max_depth,
                        trace=trace,
                        rows=rows,
                        visiting=visiting,
                        assumed_false_states=assumed_false_states,
                        expand_false=expand_false,
                        token_true_overrides=token_true_overrides,
                    )
                    return
                _trace_token(
                    graph=graph,
                    token=caller_port,
                    required_value=required_value,
                    ctx_pou_uri=caller_pou_uri,
                    ctx_code=caller_code,
                    ctx_lang=caller_lang,
                    path_id=path_id,
                    depth=depth + 1,
                    max_depth=max_depth,
                    trace=trace,
                    rows=rows,
                    visiting=visiting,
                    assumed_false_states=assumed_false_states,
                    expand_false=expand_false,
                    token_true_overrides=token_true_overrides,
                )
                return

            if direction == "Output":
                owning_lang = (local_ctx_lang or get_pou_language(graph, local_ctx_pou_uri) or "").upper()
                if owning_lang == "FBD":
                    out_trace = trace_symbol_from_fbd(local_ctx_code, token, target_value=req_text, trace=None)
                    if "error" in out_trace:
                        trace.log(f"{indent}  [POU-OUT][FBD] {out_trace['error']}")
                        return
                    focus_tree = out_trace.get("focus_tree") or out_trace.get("tree")
                    leaves = _collect_leaf_tokens(focus_tree) if focus_tree else []
                    uniq: List[str] = []
                    for leaf in leaves:
                        if not leaf or _is_internal_v(leaf) or leaf in uniq:
                            continue
                        uniq.append(leaf)
                    for leaf in uniq:
                        _trace_token(
                            graph=graph,
                            token=leaf,
                            required_value=True,
                            ctx_pou_uri=local_ctx_pou_uri,
                            ctx_code=local_ctx_code,
                            ctx_lang=local_ctx_lang,
                            path_id=path_id,
                            depth=depth + 1,
                            max_depth=max_depth,
                            trace=trace,
                            rows=rows,
                            visiting=visiting,
                            assumed_false_states=assumed_false_states,
                            expand_false=expand_false,
                            token_true_overrides=token_true_overrides,
                        )
                    return

                assigns = extract_assignments_st(local_ctx_code, token, trace=None)
                if not assigns:
                    trace.log(f"{indent}  Output-Port: Wert wird intern im POU berechnet.")
                    _add_requirement_row(
                        rows,
                        path_id=path_id,
                        token=token,
                        value=req_text,
                        pou_name=local_pou_name,
                        kind="terminal",
                        reason="Output-Port ohne ST-Assignments",
                        depth=depth + 1,
                        source="output",
                    )
                    return
                chosen = assigns[-1]
                trace.log(f"{indent}  Letzte Assignment @ {chosen['line_no']}: {chosen['assignment'].strip()}")
                _add_requirement_row(
                    rows,
                    path_id=path_id,
                    token=token,
                    value=req_text,
                    pou_name=local_pou_name,
                    kind="output_assignment",
                    reason=f"Output-Assignment @ {chosen['line_no']}",
                    depth=depth + 1,
                    source="assignment",
                    assignment=str(chosen.get("assignment", "")).strip(),
                )
                for cond in chosen.get("conditions", []):
                    cond_expr = _extract_condition_expr(cond)
                    if cond_expr:
                        _trace_expression_requirements(
                            graph=graph,
                            expr=cond_expr,
                            required_value=True,
                            ctx_pou_uri=local_ctx_pou_uri,
                            ctx_code=local_ctx_code,
                            ctx_lang=local_ctx_lang,
                            path_id=path_id,
                            depth=depth + 1,
                            max_depth=max_depth,
                            trace=trace,
                            rows=rows,
                            visiting=visiting,
                            assumed_false_states=assumed_false_states,
                            expand_false=expand_false,
                            token_true_overrides=token_true_overrides,
                        )
                return

            return

        if kind == "internal_variable":
            var_ctx = _effective_trace_context(graph, cls, ctx_pou_uri, ctx_code, ctx_lang)
            var_ctx_pou_uri = var_ctx["pou_uri"]
            var_ctx_code = str(var_ctx.get("code", "") or "")
            var_ctx_lang = str(var_ctx.get("lang", "") or "")
            var_pou_name = str(var_ctx.get("pou_name", "") or pou_name)
            trace.log(f"{indent}'{token}' ist interne Variable in {var_pou_name}")
            _add_requirement_row(
                rows,
                path_id=path_id,
                token=token,
                value=req_text,
                pou_name=var_pou_name,
                kind="internal_variable",
                reason="interne Variable",
                depth=depth,
                source="token",
            )
            assigns = extract_assignments_st(var_ctx_code, token, trace=None)
            trace.log(f"{indent}  Assignments im ST-Code: {len(assigns)}")

            candidates: List[Dict[str, Any]] = []
            local_chosen = _pick_assignment_for_requirement(assigns, required_value)
            if local_chosen:
                local_rec = dict(local_chosen)
                local_rec["source_kind"] = "local"
                local_rec["_candidate_score"] = _score_assignment_candidate(local_rec, required_value)
                candidates.append(local_rec)

            if _is_job_method_fb(graph, var_ctx_pou_uri):
                method_candidates = _collect_job_method_assignment_candidates(graph, var_ctx_pou_uri, token, required_value)
                if method_candidates:
                    trace.log(f"{indent}  Job-Method-Assignments gefunden: {len(method_candidates)}")
                candidates.extend(method_candidates)

            if not candidates:
                md = {
                    "default_variable_value": cls.get("default_variable_value"),
                    "hardware_address": cls.get("hardware_address"),
                    "opcua_data_access": cls.get("opcua_data_access"),
                    "opcua_write_access": cls.get("opcua_write_access"),
                }
                trace.log(f"{indent}  [KG-META] {md}")
                _add_requirement_row(
                    rows,
                    path_id=path_id,
                    token=token,
                    value=req_text,
                    pou_name=var_pou_name,
                    kind="terminal",
                    reason=f"Keine Assignments; KG-Meta: {md}",
                    depth=depth + 1,
                    source="metadata",
                )
                return

            candidates.sort(
                key=lambda x: (
                    -_safe_int(x.get("_candidate_score", 0), 0),
                    _safe_int(x.get("line_no", 999999), 999999),
                    str(x.get("method_display_name", "")),
                )
            )
            chosen = candidates[0]
            chosen_source_kind = str(chosen.get("source_kind", "")).strip().lower()
            chosen_ctx_pou_uri = var_ctx_pou_uri
            chosen_ctx_code = var_ctx_code
            chosen_ctx_lang = var_ctx_lang
            chosen_pou_name = var_pou_name
            chosen_kind = "internal_assignment"
            chosen_reason = f"Gewählte Assignment @ {chosen['line_no']}"
            if chosen_source_kind == "job_method":
                chosen_ctx_pou_uri = URIRef(str(chosen.get("method_uri", "")))
                chosen_ctx_code = str(chosen.get("code", "") or "")
                chosen_ctx_lang = str(chosen.get("lang", "") or "")
                chosen_pou_name = str(chosen.get("method_display_name", "") or chosen.get("method_pou_name", "") or var_pou_name)
                method_type = str(chosen.get("method_type", "") or "")
                chosen_kind = "job_method_assignment"
                chosen_reason = f"Job-Method {method_type or 'Method'} Assignment @ {chosen['line_no']}"
            trace.log(f"{indent}    Gewählte Assignment @ {chosen['line_no']}: {chosen['assignment'].strip()}")
            _add_requirement_row(
                rows,
                path_id=path_id,
                token=token,
                value=req_text,
                pou_name=chosen_pou_name,
                kind=chosen_kind,
                reason=chosen_reason,
                depth=depth + 1,
                source="assignment",
                assignment=str(chosen.get("assignment", "")).strip(),
            )
            for cond in chosen.get("conditions", []):
                cond_expr = _extract_condition_expr(cond)
                if not cond_expr:
                    continue
                _trace_expression_requirements(
                    graph=graph,
                    expr=cond_expr,
                    required_value=True,
                    ctx_pou_uri=chosen_ctx_pou_uri,
                    ctx_code=chosen_ctx_code,
                    ctx_lang=chosen_ctx_lang,
                    path_id=path_id,
                    depth=depth + 1,
                    max_depth=max_depth,
                    trace=trace,
                    rows=rows,
                    visiting=visiting,
                    assumed_false_states=assumed_false_states,
                    expand_false=expand_false,
                    token_true_overrides=token_true_overrides,
                )
            rhs = chosen.get("rhs", "")
            if _rhs_bool_literal(rhs) is None and rhs:
                _trace_expression_requirements(
                    graph=graph,
                    expr=rhs,
                    required_value=required_value,
                    ctx_pou_uri=chosen_ctx_pou_uri,
                    ctx_code=chosen_ctx_code,
                    ctx_lang=chosen_ctx_lang,
                    path_id=path_id,
                    depth=depth + 1,
                    max_depth=max_depth,
                    trace=trace,
                    rows=rows,
                    visiting=visiting,
                    assumed_false_states=assumed_false_states,
                    expand_false=expand_false,
                    token_true_overrides=token_true_overrides,
                )
            return

        if kind == "global_variable":
            md = {
                "default_variable_value": cls.get("default_variable_value"),
                "hardware_address": cls.get("hardware_address"),
                "opcua_data_access": cls.get("opcua_data_access"),
                "opcua_write_access": cls.get("opcua_write_access"),
            }
            trace.log(f"{indent}'{token}' ist globale Variable. KG-Meta: {md}")
            _add_requirement_row(
                rows,
                path_id=path_id,
                token=token,
                value=req_text,
                pou_name="-",
                kind="global_variable",
                reason=f"terminal global ({md})",
                depth=depth,
                source="global",
            )
            return

        trace.log(f"{indent}Unbekanntes Token: {token}")
        _add_requirement_row(
            rows,
            path_id=path_id,
            token=token,
            value=req_text,
            pou_name=pou_name,
            kind="unknown",
            reason="nicht klassifiziert",
            depth=depth,
            source="unknown",
        )
    finally:
        visiting.discard(visit_key)


def _dominant_true_assignment(assignments: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    true_hits = [a for a in assignments if _rhs_bool_literal(a.get("rhs", "")) is True]
    return true_hits[-1] if true_hits else None


def _extract_innermost_if_expr(assignment: Dict[str, Any]) -> Dict[str, Any]:
    conds = assignment.get("conditions", []) if assignment else []
    if not conds:
        return {"if_line": None, "expr": ""}
    last = conds[-1]
    m = re.match(r"^(IF|ELSIF)@(\d+):\s*(.*)$", last)
    if m:
        return {"if_line": int(m.group(2)), "expr": m.group(3).strip()}
    if ":" in last:
        return {"if_line": None, "expr": last.split(":", 1)[1].strip()}
    return {"if_line": None, "expr": ""}


def run_unified_set_and_condition_trace(
    graph: Graph,
    *,
    last_gemma_state: str,
    state_name: str = "D2",
    target_var: str = "",
    max_depth: int = 12,
    trace_each_truth_path: bool = True,
    assumed_false_states_in_betriebsarten: Optional[Iterable[str]] = None,
    preferred_gemma_pou_names: Optional[Iterable[str]] = None,
    scope_hints: Optional[Iterable[str]] = None,
    verbose_trace: bool = False,
) -> Dict[str, Any]:
    tr = Tracer(enabled=True, print_live=verbose_trace)
    assumed_false_states = {
        str(x).strip().upper()
        for x in (assumed_false_states_in_betriebsarten or {"D1", "D2", "D3"})
        if str(x).strip()
    }

    tr.log("[RUN] Start unified trace")
    gemma = find_gemma_pous(graph, trace=tr)
    if not gemma:
        return {"error": "Kein GEMMA-CustomFBType mit dp_isGEMMAStateMachine=true gefunden.", "trace_log": tr.lines}

    gemma_pou_uri, gemma_pou_name = _pick_scoped_gemma_pou(
        graph,
        gemma,
        preferred_names=preferred_gemma_pou_names,
        scope_hints=scope_hints,
        trace=tr,
    )
    fbd_code = get_pou_code(graph, gemma_pou_uri)
    if not fbd_code:
        return {"error": f"Kein dp_hasPOUCode fÃ¼r GEMMA POU '{gemma_pou_name}'.", "trace_log": tr.lines}

    d2_trace = trace_symbol_from_fbd(fbd_code, state_name, target_value="TRUE", trace=tr)
    if "error" in d2_trace:
        return {"error": d2_trace["error"], "trace_log": tr.lines}

    auto = infer_suspected_input_port_from_last_state(d2_trace, last_state=last_gemma_state, trace=tr)
    if "error" in auto:
        return {"error": auto["error"], "d2_trace": d2_trace, "trace_log": tr.lines, "auto": auto}

    candidates = auto.get("next_trace_candidates", [])
    if not candidates:
        return {"error": "Keine next_trace_candidates gefunden.", "d2_trace": d2_trace, "trace_log": tr.lines, "auto": auto}

    candidate_origins: List[Dict[str, Any]] = []
    hint_list = _collect_scope_hints(scope_hints)
    pref_list = [str(x).strip() for x in (preferred_gemma_pou_names or []) if str(x).strip()]
    for cand in candidates:
        origin_try = resolve_input_to_upstream_output(graph, gemma_pou_uri, cand)
        score = 0
        if "error" not in origin_try:
            if bool(origin_try.get("is_wired")):
                score += 20
            score += _score_name_with_scope(
                str(origin_try.get("caller_pou_name", "") or ""),
                preferred_names=pref_list,
                scope_hints=hint_list,
            )
        candidate_origins.append({"candidate": cand, "origin": origin_try, "score": score})

    candidate_origins.sort(
        key=lambda item: (
            "error" in (item.get("origin") or {}),
            -_safe_int(item.get("score", 0), 0),
            str(item.get("candidate", "")),
        )
    )

    chosen_candidate = candidate_origins[0] if candidate_origins else {"candidate": candidates[0], "origin": {}}
    suspected_input_port = str(chosen_candidate.get("candidate", "") or candidates[0])
    tr.log(f"[RUN] Auto-suspected input port: {suspected_input_port}")
    origin = chosen_candidate.get("origin") if isinstance(chosen_candidate.get("origin"), dict) else {}
    if "error" in origin:
        return {
            "error": origin["error"],
            "d2_trace": d2_trace,
            "auto": auto,
            "candidate_origins": candidate_origins,
            "trace_log": tr.lines,
        }

    context_pou_uri = URIRef(origin["caller_pou_uri"]) if origin.get("caller_pou_uri") else gemma_pou_uri
    context_pou_name = origin.get("caller_pou_name") or get_pou_name(graph, context_pou_uri)
    context_code = origin.get("caller_code") or get_pou_code(graph, context_pou_uri)
    context_lang = get_pou_language(graph, context_pou_uri)
    auto_context_target = origin.get("caller_port_name") or ""
    requested_target = (target_var or "").strip()
    context_target = requested_target or auto_context_target
    if not context_target:
        return {"error": "Konnte keine Zielvariable im Kontext-POU ermitteln.", "trace_log": tr.lines}

    if requested_target and requested_target != auto_context_target:
        requested_aa = analyze_var_assignments_st(context_code, requested_target, trace=None)
        if requested_aa.get("summary", {}).get("assignment_count", 0) == 0 and auto_context_target:
            tr.log(
                "[CTX] requested target_var "
                f"'{requested_target}' hat im Kontext-POU '{context_pou_name}' keine Assignments. "
                f"Fallback auf auto target '{auto_context_target}'."
            )
            context_target = auto_context_target

    tr.log(f"[CTX] context_pou={context_pou_name} target_var={context_target}")

    aa = analyze_var_assignments_st(context_code, context_target, trace=tr)
    assigns = extract_assignments_st(context_code, context_target, trace=None)
    dom_true = _dominant_true_assignment(assigns)
    if not dom_true:
        return {
            "error": f"Keine TRUE-Zuweisung fÃ¼r '{context_target}' gefunden.",
            "d2_trace": d2_trace,
            "auto": auto,
            "origin": origin,
            "assignment_analysis": aa,
            "trace_log": tr.lines,
        }

    tr.log(f"[ST] Dominante TRUE-Zuweisung @ {dom_true['line_no']}: {dom_true['assignment']}")
    inn = _extract_innermost_if_expr(dom_true)
    dominant_expr = inn.get("expr", "")
    if_line = inn.get("if_line")
    tr.log(f"[ST] Path: IF@{if_line}: {dominant_expr}" if if_line else f"[ST] Path: {dominant_expr}")

    truth_paths = condition_to_truth_paths(dominant_expr)
    tr.log(f"[ST] Truth paths: {len(truth_paths)}")

    path_reports: List[Dict[str, Any]] = []
    for idx, path in enumerate(truth_paths, start=1):
        if not trace_each_truth_path and idx > 1:
            break
        path_label = f"{idx}"
        pieces = [f"{tok}" if req else f"NOT {tok}" for tok, req in path]
        tr.log(f"\n[PATH #{path_label}] {' AND '.join(pieces)}")
        rows: List[Dict[str, Any]] = []
        visiting: Set[Tuple[str, str, str]] = set()
        for tok, req in path:
            _trace_token(
                graph=graph,
                token=tok,
                required_value=req,
                ctx_pou_uri=context_pou_uri,
                ctx_code=context_code,
                ctx_lang=context_lang,
                path_id=path_label,
                depth=1,
                max_depth=max_depth,
                trace=tr,
                rows=rows,
                visiting=visiting,
                assumed_false_states=assumed_false_states,
            )
        path_reports.append({"path_id": path_label, "path_expr": " AND ".join(pieces), "requirements": rows})

    return {
        "gemma_pou_name": gemma_pou_name,
        "gemma_pou_uri": str(gemma_pou_uri),
        "state_name": state_name,
        "last_gemma_state_before_failure": last_gemma_state,
        "d2_trace": d2_trace,
        "auto_port": auto,
        "candidate_origins": candidate_origins,
        "origin": origin,
        "context": {
            "pou_uri": str(context_pou_uri),
            "pou_name": context_pou_name,
            "language": context_lang,
            "target_var": context_target,
        },
        "assignment_analysis": aa,
        "dominant_true_assignment": dom_true,
        "dominant_inner_if": inn,
        "truth_paths": [[{"token": t, "required": bool(v)} for t, v in path] for path in truth_paths],
        "path_reports": path_reports,
        "trace_log": tr.lines,
    }


def build_requirement_tables(analysis: Dict[str, Any]) -> Dict[str, Any]:
    if "error" in analysis:
        return analysis

    out_paths: List[Dict[str, Any]] = []
    for p in analysis.get("path_reports", []):
        rows = p.get("requirements", [])
        sorted_rows = sorted(rows, key=lambda r: (str(r.get("path_id", "")), _safe_int(r.get("depth", 0), 0)))
        table_rows = []
        for r in sorted_rows:
            table_rows.append(
                {
                    "Port/Variable": r.get("token"),
                    "Wert": r.get("value"),
                    "POU": r.get("pou"),
                    "Assignment": r.get("assignment", ""),
                    "FBType": r.get("fb_type", ""),
                    "FBTypeDescription": r.get("fb_type_description", ""),
                    "Typ": r.get("kind"),
                    "Herleitung": r.get("reason"),
                    "Quelle": r.get("source"),
                    "Tiefe": r.get("depth"),
                    "Pfad": r.get("path_id"),
                }
            )
        out_paths.append({"path_id": p.get("path_id"), "path_expr": p.get("path_expr"), "rows": table_rows})

    return {
        "context": analysis.get("context", {}),
        "state_name": analysis.get("state_name"),
        "last_gemma_state_before_failure": analysis.get("last_gemma_state_before_failure"),
        "dominant_true_assignment": analysis.get("dominant_true_assignment"),
        "dominant_inner_if": analysis.get("dominant_inner_if"),
        "paths": out_paths,
        "trace_log": analysis.get("trace_log", []),
    }


def _find_pou_uri_by_name(graph: Graph, pou_name: str) -> Optional[URIRef]:
    name = str(pou_name or "").strip()
    if not name:
        return None

    exact: List[URIRef] = []
    for subj in graph.subjects(P_DP_HAS_POU_NAME, Literal(name)):
        if _first(graph, subj, P_DP_HAS_POU_CODE) is None:
            continue
        exact.append(subj)
    if exact:
        return exact[0]

    low = name.lower()
    for subj, obj in graph.subject_objects(P_DP_HAS_POU_NAME):
        if str(obj).strip().lower() != low:
            continue
        if _first(graph, subj, P_DP_HAS_POU_CODE) is None:
            continue
        return subj
    return None


def _extract_hw_from_text(text: str) -> str:
    s = str(text or "")
    m1 = re.search(r"hardware_address['\"]?\s*:\s*['\"]([^'\"]+)['\"]", s, flags=re.I)
    if m1:
        return str(m1.group(1) or "").strip()
    m2 = re.search(r"(%[IQM][XWDB]?[0-9\.\[\]]+)", s, flags=re.I)
    if m2:
        return str(m2.group(1) or "").strip()
    return ""


def _summarize_trigger_terminal_sources(
    graph: Graph,
    rows: List[Dict[str, Any]],
    default_ctx_pou_uri: URIRef,
) -> Dict[str, Any]:
    hardware_rows: List[Dict[str, Any]] = []
    default_rows: List[Dict[str, Any]] = []
    mapping_rows: List[Dict[str, Any]] = []
    loop_rows: List[Dict[str, Any]] = []
    stop_rows: List[Dict[str, Any]] = []
    unknown_rows: List[Dict[str, Any]] = []

    for r in rows:
        token = str(r.get("token", "")).strip()
        source = str(r.get("source", "")).strip().lower()
        kind = str(r.get("kind", "")).strip().lower()
        reason = str(r.get("reason", "")).strip()
        assignment = str(r.get("assignment", "")).strip()
        pou_name = str(r.get("pou", "")).strip()

        if source == "default" or "defaultvalue" in reason.lower():
            default_rows.append(r)
        if source in {"wiring", "assignment"} or assignment:
            mapping_rows.append(r)
        if kind == "loop":
            loop_rows.append(r)
        if kind == "stop":
            stop_rows.append(r)
        if kind == "unknown":
            unknown_rows.append(r)

        ctx_uri = _find_pou_uri_by_name(graph, pou_name) or default_ctx_pou_uri
        hw = ""
        try:
            cls = _classify_token(graph, token, ctx_uri, trace=None)
            hw = str(cls.get("hardware_address", "") or "").strip()
        except Exception:
            hw = ""
        if not hw:
            hw = _extract_hw_from_text(reason) or _extract_hw_from_text(assignment)
        if hw:
            rec = dict(r)
            rec["hardware_address"] = hw
            hardware_rows.append(rec)

    def _uniq(rows_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen: Set[Tuple[str, str, str, str]] = set()
        out: List[Dict[str, Any]] = []
        for rr in rows_in:
            k = (
                str(rr.get("path_id", "")),
                str(rr.get("token", "")),
                str(rr.get("value", "")),
                str(rr.get("hardware_address", "")),
            )
            if k in seen:
                continue
            seen.add(k)
            out.append(rr)
        return out

    hardware_rows = _uniq(hardware_rows)
    default_rows = _uniq(default_rows)
    mapping_rows = _uniq(mapping_rows)
    loop_rows = _uniq(loop_rows)
    stop_rows = _uniq(stop_rows)
    unknown_rows = _uniq(unknown_rows)

    has_hw = len(hardware_rows) > 0
    has_default = len(default_rows) > 0
    software_likely = not has_hw

    if has_hw:
        origin_assessment = "Triggerpfad enthÃ¤lt mindestens eine Hardware-Adresse."
    elif has_default:
        origin_assessment = "Kein Hardware-Endpunkt im Triggerpfad; Endpunkt Ã¼ber Default-Wert/Softwareparameter."
    elif loop_rows or stop_rows:
        origin_assessment = "Kein Hardware-Endpunkt gefunden; Trace endet in Loop/Depth-Limit (softwarelogische AbhÃ¤ngigkeit)."
    else:
        origin_assessment = "Kein Hardware-Endpunkt gefunden; Triggerpfad wirkt softwaregetrieben."

    return {
        "has_hardware_address": has_hw,
        "has_default_value": has_default,
        "has_loop": len(loop_rows) > 0,
        "has_max_depth_stop": len(stop_rows) > 0,
        "has_unknown": len(unknown_rows) > 0,
        "software_likely": software_likely,
        "origin_assessment": origin_assessment,
        "hardware_rows": hardware_rows,
        "default_rows": default_rows,
        "mapping_rows": mapping_rows,
        "loop_rows": loop_rows,
        "stop_rows": stop_rows,
        "unknown_rows": unknown_rows,
    }


def trace_condition_paths_from_pou(
    graph: Graph,
    *,
    pou_name: str,
    condition_expr: str,
    max_depth: int = 18,
    assumed_false_states: Optional[Iterable[str]] = None,
    token_true_overrides: Optional[Dict[str, str]] = None,
    verbose_trace: bool = False,
) -> Dict[str, Any]:
    expr = str(condition_expr or "").strip()
    if not expr:
        return {"error": "condition_expr ist leer."}

    pou_uri = _find_pou_uri_by_name(graph, pou_name)
    if not pou_uri:
        return {"error": f"POU '{pou_name}' nicht gefunden."}

    ctx_code = get_pou_code(graph, pou_uri)
    if not ctx_code:
        return {"error": f"Kein POU-Code fÃ¼r '{pou_name}' gefunden."}

    ctx_lang = get_pou_language(graph, pou_uri)
    tr = Tracer(enabled=True, print_live=verbose_trace)
    assumed = {
        str(x).strip().upper()
        for x in (assumed_false_states or {"D1", "D2", "D3"})
        if str(x).strip()
    }
    truth_paths = condition_to_truth_paths(expr)
    if not truth_paths:
        return {"error": f"Keine Truth-Paths fÃ¼r Ausdruck gefunden: {expr}"}

    tr.log(f"[COND] Trace Ausdruck in {pou_name}: {expr}")
    path_reports: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []
    for idx, path in enumerate(truth_paths, start=1):
        pid = str(idx)
        parts = [tok if req else f"NOT {tok}" for tok, req in path]
        path_expr = " AND ".join(parts)
        tr.log(f"[COND][PATH {pid}] {path_expr}")

        rows: List[Dict[str, Any]] = []
        visiting: Set[Tuple[str, str, str]] = set()
        for tok, req in path:
            _trace_token(
                graph=graph,
                token=tok,
                required_value=bool(req),
                ctx_pou_uri=pou_uri,
                ctx_code=ctx_code,
                ctx_lang=ctx_lang,
                path_id=pid,
                depth=1,
                max_depth=int(max_depth),
                trace=tr,
                rows=rows,
                visiting=visiting,
                assumed_false_states=assumed,
                expand_false=False,
                token_true_overrides=token_true_overrides,
            )

        all_rows.extend(rows)
        sorted_rows = sorted(rows, key=lambda r: (str(r.get("path_id", "")), _safe_int(r.get("depth", 0), 0)))
        table_rows: List[Dict[str, Any]] = []
        for r in sorted_rows:
            table_rows.append(
                {
                    "Port/Variable": r.get("token"),
                    "Wert": r.get("value"),
                    "POU": r.get("pou"),
                    "Assignment": r.get("assignment", ""),
                    "FBType": r.get("fb_type", ""),
                    "FBTypeDescription": r.get("fb_type_description", ""),
                    "Typ": r.get("kind"),
                    "Herleitung": r.get("reason"),
                    "Quelle": r.get("source"),
                    "Tiefe": r.get("depth"),
                    "Pfad": r.get("path_id"),
                }
            )
        path_reports.append({"path_id": pid, "path_expr": path_expr, "rows": table_rows})

    summary = _summarize_trigger_terminal_sources(graph, all_rows, pou_uri)
    return {
        "context": {
            "pou_name": pou_name,
            "pou_uri": str(pou_uri),
            "language": ctx_lang,
            "condition_expr": expr,
        },
        "paths": path_reports,
        "summary": summary,
        "trace_log": tr.lines,
    }
