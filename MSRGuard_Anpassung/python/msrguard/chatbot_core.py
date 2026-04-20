from __future__ import annotations

import json
import os
import re
import inspect
from collections import deque
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF
from .d2_trace_analysis import (
    run_unified_set_and_condition_trace,
    build_requirement_tables,
    extract_assignments_st,
    condition_to_truth_paths,
    trace_condition_paths_from_pou,
)

# ----------------------------
# Prefixes / Guardrails
# ----------------------------

DEFAULT_PREFIXES = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ag:  <http://www.semanticweb.org/AgentProgramParams/>
PREFIX dp:  <http://www.semanticweb.org/AgentProgramParams/dp_>
PREFIX op:  <http://www.semanticweb.org/AgentProgramParams/op_>
"""

AG = Namespace("http://www.semanticweb.org/AgentProgramParams/")
DP = Namespace("http://www.semanticweb.org/AgentProgramParams/dp_")
OP = Namespace("http://www.semanticweb.org/AgentProgramParams/op_")


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


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


def _leading_scope_token(text: Any) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    m = re.match(r"^([A-Za-z0-9]+)(?:[_\.].*)?$", s)
    return str(m.group(1) or "").strip() if m else ""


def _collect_scope_hints(*values: Any) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for raw in values:
        items = list(raw) if isinstance(raw, (list, tuple, set)) else [raw]
        for item in items:
            text = str(item or "").strip()
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


def _unwrap_plc_snapshot(snapshot: Any) -> Dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    if isinstance(snapshot.get("vars"), list) or isinstance(snapshot.get("rows"), list):
        return snapshot
    for key in ("plcSnapshot", "snapShot", "snapshot"):
        nested = snapshot.get(key)
        if isinstance(nested, dict):
            return _unwrap_plc_snapshot(nested)
    payload = snapshot.get("payload")
    if isinstance(payload, dict):
        return _unwrap_plc_snapshot(payload)
    return snapshot


def _to_bool_str(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return "TRUE"
        if value == 0:
            return "FALSE"

    text = str(value or "").strip().upper()
    if not text:
        return None
    if text in {"TRUE", "BOOL#TRUE", "1"}:
        return "TRUE"
    if text in {"FALSE", "BOOL#FALSE", "0"}:
        return "FALSE"
    if "TRUE" in text and "FALSE" not in text:
        return "TRUE"
    if "FALSE" in text and "TRUE" not in text:
        return "FALSE"
    return None


@dataclass
class SnapshotEvidenceHit:
    query: str = ""
    matched_id: str = ""
    normalized_id: str = ""
    source: str = ""
    alias_kind: str = ""
    value_raw: Any = None
    value_bool: Optional[str] = None
    node_class: str = ""
    data_type: str = ""
    score: int = 0


@dataclass
class SnapshotEvidenceIndex:
    raw_snapshot: Dict[str, Any]
    alias_index: Dict[str, List[SnapshotEvidenceHit]] = field(default_factory=dict)
    total_items: int = 0

    @staticmethod
    def _normalize_id(identifier: Any) -> str:
        text = str(identifier or "").strip()
        if not text:
            return ""
        m = re.search(r";s=(.+)$", text, flags=re.I)
        if m:
            return str(m.group(1) or "").strip()
        return text

    @classmethod
    def _candidate_aliases(cls, identifier: Any) -> List[Tuple[str, str, int]]:
        raw = str(identifier or "").strip()
        norm = cls._normalize_id(raw)
        if not raw and not norm:
            return []

        out: List[Tuple[str, str, int]] = []
        seen: Set[str] = set()

        def add(alias: str, kind: str, score: int) -> None:
            key = str(alias or "").strip()
            if not key:
                return
            lk = key.lower()
            if lk in seen:
                return
            seen.add(lk)
            out.append((key, kind, score))

        add(norm or raw, "symbol", 100)
        if raw and raw != norm:
            add(raw, "raw_id", 80)

        segments = [seg.strip() for seg in re.split(r"\.", norm or raw) if seg.strip()]
        if len(segments) >= 3:
            add(".".join(segments[-3:]), "triple_suffix", 92)
        if len(segments) >= 2:
            add(".".join(segments[-2:]), "qualified_suffix", 88)
        if segments:
            add(segments[-1], "suffix", 55)

        method_match = re.search(r"\b(Method_[A-Za-z0-9_]+|JobMethode_[A-Za-z0-9_]+)\b", norm or raw, flags=re.I)
        if method_match:
            add(str(method_match.group(1) or "").strip(), "method_name", 94)

        return out

    @classmethod
    def build_from_snapshot(cls, snapshot: Any) -> "SnapshotEvidenceIndex":
        snap = _unwrap_plc_snapshot(snapshot)
        idx = cls(raw_snapshot=snap if isinstance(snap, dict) else {})
        vars_list = snap.get("vars") if isinstance(snap, dict) else None
        rows_list = snap.get("rows") if isinstance(snap, dict) else None

        for item in vars_list if isinstance(vars_list, list) else []:
            idx._ingest_item(item, source="vars")
        for item in rows_list if isinstance(rows_list, list) else []:
            idx._ingest_item(item, source="rows")
        return idx

    def _ingest_item(self, item: Any, *, source: str) -> None:
        if not isinstance(item, dict):
            return
        item_id = str(item.get("id", "") or "").strip()
        if not item_id:
            return
        self.total_items += 1
        normalized_id = self._normalize_id(item_id)
        value_raw = item.get("v")
        value_bool = _to_bool_str(value_raw)
        node_class = str(item.get("nodeClass", "") or "")
        data_type = str(item.get("t", "") or "")

        for alias, alias_kind, alias_score in self._candidate_aliases(item_id):
            hit = SnapshotEvidenceHit(
                matched_id=item_id,
                normalized_id=normalized_id,
                source=source,
                alias_kind=alias_kind,
                value_raw=value_raw,
                value_bool=value_bool,
                node_class=node_class,
                data_type=data_type,
                score=alias_score,
            )
            self.alias_index.setdefault(alias.lower(), []).append(hit)

    def has_data(self) -> bool:
        return self.total_items > 0

    def best_hit(self, symbol: Any, *, require_boolean: bool = False) -> Optional[SnapshotEvidenceHit]:
        query = str(symbol or "").strip()
        if not query:
            return None

        best: Optional[SnapshotEvidenceHit] = None
        best_score: Optional[int] = None
        query_norm = self._normalize_id(query).lower()

        for alias, _, query_score in self._candidate_aliases(query):
            for hit in self.alias_index.get(alias.lower(), []):
                if require_boolean and hit.value_bool not in {"TRUE", "FALSE"}:
                    continue

                total_score = _safe_int(hit.score, 0) + query_score
                if hit.normalized_id.lower() == query_norm:
                    total_score += 25
                if hit.matched_id.lower() == query.lower():
                    total_score += 20
                if hit.source == "vars":
                    total_score += 8
                if hit.value_bool in {"TRUE", "FALSE"}:
                    total_score += 6

                if best_score is None or total_score > best_score:
                    best_score = total_score
                    best = SnapshotEvidenceHit(
                        query=query,
                        matched_id=hit.matched_id,
                        normalized_id=hit.normalized_id,
                        source=hit.source,
                        alias_kind=hit.alias_kind,
                        value_raw=hit.value_raw,
                        value_bool=hit.value_bool,
                        node_class=hit.node_class,
                        data_type=hit.data_type,
                        score=total_score,
                    )

        return best

    def bool_for(self, symbol: Any) -> Optional[str]:
        hit = self.best_hit(symbol, require_boolean=True)
        return hit.value_bool if isinstance(hit, SnapshotEvidenceHit) else None


def enforce_select_only(query: str, max_limit: int = 200) -> str:
    q = query.strip()
    q_u = _normalize_ws(q).upper()

    if not (q_u.startswith("PREFIX") or q_u.startswith("SELECT")):
        raise ValueError("Only SELECT queries are allowed (optionally with PREFIX).")

    forbidden = [
        "INSERT", "DELETE", "LOAD", "CLEAR", "CREATE", "DROP", "MOVE", "COPY", "ADD",
        "SERVICE", "WITH", "USING", "GRAPH"
    ]
    for kw in forbidden:
        if re.search(rf"\b{kw}\b", q_u):
            raise ValueError(f"Forbidden SPARQL keyword detected: {kw}")

    m = re.search(r"\bLIMIT\s+(\d+)\b", q_u)
    if m:
        lim = _safe_int(m.group(1), max_limit)
        if lim > max_limit:
            q = re.sub(r"(?i)\bLIMIT\s+\d+\b", f"LIMIT {max_limit}", q)
    else:
        q = q.rstrip() + f"\nLIMIT {max_limit}\n"
    return q


def strip_code_fences(text: str) -> str:
    t = text.strip()
    t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    return t.strip()


def extract_sparql_from_llm(text: str) -> str:
    t = strip_code_fences(text)
    m = re.search(r"(PREFIX[\s\S]*?SELECT[\s\S]*)", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return t.strip()


def schema_card(graph: Graph, top_n: int = 15) -> str:
    from collections import Counter
    pred_counts = Counter()
    type_counts = Counter()

    for s, p, o in graph:
        try:
            pred_counts[graph.qname(p)] += 1
        except Exception:
            pred_counts[str(p)] += 1

        if p == RDF.type:
            try:
                type_counts[graph.qname(o)] += 1
            except Exception:
                type_counts[str(o)] += 1

    lines = []
    lines.append("TOP CLASSES (rdf:type):")
    for k, v in type_counts.most_common(top_n):
        lines.append(f"  - {k}: {v}")
    lines.append("")
    lines.append("TOP PROPERTIES:")
    for k, v in pred_counts.most_common(top_n):
        lines.append(f"  - {k}: {v}")
    return "\n".join(lines)


# ----------------------------
# Global graph access (pragmatic)
# ----------------------------

def sparql_select_raw(query: str, max_rows: int = 200) -> List[Dict[str, Any]]:
    if "g" not in globals():
        raise RuntimeError("Global graph 'g' not found via globals().")

    q = query.strip()
    if "PREFIX" not in q.upper():
        q = DEFAULT_PREFIXES + "\n" + q

    q = enforce_select_only(q, max_limit=max_rows)

    res = globals()["g"].query(q)
    vars_ = [str(v) for v in res.vars]

    out: List[Dict[str, Any]] = []
    for row in res:
        item = {}
        for i, v in enumerate(vars_):
            val = row[i]
            item[v] = None if val is None else str(val)
        out.append(item)
    return out


# ----------------------------
# KG Store + Routine Index
# ----------------------------

@dataclass
class SensorSnapshot:
    program_name: str
    sensor_values: Dict[str, Any]


@dataclass
class RoutineSignature:
    pou_name: str
    reachable_pous: List[str]
    called_pou_names: List[str]
    used_variable_names: List[str]
    hardware_addresses: List[str]
    port_names: List[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "pou_name": self.pou_name,
            "reachable_pous": self.reachable_pous,
            "called_pou_names": self.called_pou_names,
            "used_variable_names": self.used_variable_names,
            "hardware_addresses": self.hardware_addresses,
            "port_names": self.port_names,
        }


class KGStore:
    def __init__(self, graph: Graph):
        self.g = graph
        self._pou_by_name: Dict[str, URIRef] = {}
        self._build_cache()

    def _build_cache(self) -> None:
        for pou, _, name in self.g.triples((None, DP.hasPOUName, None)):
            if isinstance(name, Literal):
                self._pou_by_name[str(name)] = pou

    def pou_uri_by_name(self, pou_name: str) -> Optional[URIRef]:
        return self._pou_by_name.get(pou_name)

    def pou_name(self, pou_uri: URIRef) -> str:
        v = self.g.value(pou_uri, DP.hasPOUName)
        return str(v) if v else str(pou_uri)

    def get_reachable_pous(self, root_pou_uri: URIRef) -> Set[URIRef]:
        visited: Set[URIRef] = set()
        queue: List[URIRef] = [root_pou_uri]
        while queue:
            cur = queue.pop(0)
            if cur in visited:
                continue
            visited.add(cur)
            for call in self.g.objects(cur, OP.containsPOUCall):
                for called in self.g.objects(call, OP.callsPOU):
                    if isinstance(called, URIRef) and called not in visited:
                        queue.append(called)
        return visited

    def get_called_pous(self, pou_uri: URIRef) -> Set[URIRef]:
        called: Set[URIRef] = set()
        for call in self.g.objects(pou_uri, OP.containsPOUCall):
            for target in self.g.objects(call, OP.callsPOU):
                if isinstance(target, URIRef):
                    called.add(target)
        return called

    def get_used_variables(self, pou_uri: URIRef) -> Set[URIRef]:
        vars_: Set[URIRef] = set()
        for v in self.g.objects(pou_uri, OP.usesVariable):
            if isinstance(v, URIRef):
                vars_.add(v)
        for v in self.g.objects(pou_uri, OP.hasInternalVariable):
            if isinstance(v, URIRef):
                vars_.add(v)
        return vars_

    def get_variable_names(self, var_uri: URIRef) -> Set[str]:
        names: Set[str] = set()
        for _, _, name in self.g.triples((var_uri, DP.hasVariableName, None)):
            if isinstance(name, Literal):
                names.add(str(name))
        return names

    def get_hardware_address(self, var_uri: URIRef) -> Optional[str]:
        v = self.g.value(var_uri, DP.hasHardwareAddress)
        return str(v) if v else None

    def get_ports_of_pou(self, pou_uri: URIRef) -> Set[URIRef]:
        ports: Set[URIRef] = set()
        for p in self.g.objects(pou_uri, OP.hasPort):
            if isinstance(p, URIRef):
                ports.add(p)
        return ports

    def get_port_name(self, port_uri: URIRef) -> str:
        v = self.g.value(port_uri, DP.hasPortName)
        return str(v) if v else ""


def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


class SignatureExtractor:
    def __init__(self, kg: KGStore):
        self.kg = kg

    def extract_signature(self, pou_name: str) -> RoutineSignature:
        pou_uri = self.kg.pou_uri_by_name(pou_name)
        if pou_uri is None:
            raise ValueError(f"POU '{pou_name}' not found in KG.")

        reachable = self.kg.get_reachable_pous(pou_uri)

        reachable_names: Set[str] = set()
        called_names: Set[str] = set()
        used_var_names: Set[str] = set()
        hw_addrs: Set[str] = set()
        port_names: Set[str] = set()

        for rp in reachable:
            reachable_names.add(self.kg.pou_name(rp))
            for callee in self.kg.get_called_pous(rp):
                called_names.add(self.kg.pou_name(callee))
            for var in self.kg.get_used_variables(rp):
                used_var_names |= self.kg.get_variable_names(var)
                ha = self.kg.get_hardware_address(var)
                if ha:
                    hw_addrs.add(ha)
            for port in self.kg.get_ports_of_pou(rp):
                pn = self.kg.get_port_name(port)
                if pn:
                    port_names.add(pn)

        return RoutineSignature(
            pou_name=pou_name,
            reachable_pous=sorted(reachable_names),
            called_pou_names=sorted(called_names),
            used_variable_names=sorted(used_var_names),
            hardware_addresses=sorted(hw_addrs),
            port_names=sorted(port_names),
        )


class RoutineIndex:
    def __init__(self, signatures: List[RoutineSignature]):
        self.signatures = signatures

    def save(self, path: str) -> None:
        Path(path).write_text(
            json.dumps([s.as_dict() for s in self.signatures], indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

    @staticmethod
    def load(path: str) -> "RoutineIndex":
        data = json.loads(Path(path).read_text(encoding="utf-8-sig").strip() or "[]")
        sigs = [RoutineSignature(**d) for d in data]
        return RoutineIndex(sigs)

    @staticmethod
    def build_from_kg(kg: KGStore, only_pous: Optional[List[str]] = None) -> "RoutineIndex":
        extractor = SignatureExtractor(kg)
        if only_pous is None:
            only_pous = sorted(kg._pou_by_name.keys())

        sigs: List[RoutineSignature] = []
        for name in only_pous:
            try:
                sigs.append(extractor.extract_signature(name))
            except Exception:
                pass
        return RoutineIndex(sigs)

    def find_similar(self, target: RoutineSignature, top_k: int = 5) -> List[Dict[str, Any]]:
        tgt_hw = set(target.hardware_addresses)
        tgt_vars = set(target.used_variable_names)
        tgt_called = set(target.called_pou_names)

        scored: List[Tuple[float, RoutineSignature]] = []
        for cand in self.signatures:
            cand_hw = set(cand.hardware_addresses)
            cand_vars = set(cand.used_variable_names)
            cand_called = set(cand.called_pou_names)

            sim_hw = jaccard(tgt_hw, cand_hw) if (tgt_hw or cand_hw) else 0.0
            sim_vars = jaccard(tgt_vars, cand_vars)
            sim_called = jaccard(tgt_called, cand_called)

            score = 0.55 * sim_hw + 0.25 * sim_vars + 0.20 * sim_called
            scored.append((score, cand))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [{"score": round(s, 4), "pou_name": r.pou_name} for s, r in scored[:top_k]]


def classify_checkable_sensors(snapshot: SensorSnapshot, sig: RoutineSignature) -> Dict[str, str]:
    checkable_set = set(sig.used_variable_names) | set(sig.hardware_addresses)
    return {k: ("checkable" if k in checkable_set else "not_checkable") for k in snapshot.sensor_values.keys()}


# ----------------------------
# LLM Wrapper (Multi-Provider)
# ----------------------------

_PROVIDER_ENV_VARS: Dict[str, str] = {
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "azure_openai": "AZURE_OPENAI_API_KEY",
    "google": "GOOGLE_API_KEY",
    "groq": "GROQ_API_KEY",
    "together": "TOGETHER_API_KEY",
}

_PROVIDER_INSTALL_HINTS: Dict[str, str] = {
    "openai": "pip install -U langchain-openai langchain-core",
    "anthropic": "pip install -U langchain-anthropic",
    "azure_openai": "pip install -U langchain-openai",
    "ollama": "pip install -U langchain-ollama",
    "google": "pip install -U langchain-google-genai",
    "groq": "pip install -U langchain-groq",
    "together": "pip install -U langchain-together",
}


@dataclass
class LLMUsage:
    """Token- und Kostentracking für einen einzelnen LLM-Aufruf."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0

    def add(self, other: "LLMUsage") -> None:
        self.prompt_tokens += other.prompt_tokens
        self.completion_tokens += other.completion_tokens
        self.total_tokens += other.total_tokens
        self.cost_usd += other.cost_usd

    def as_dict(self) -> Dict[str, Any]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cost_usd": round(self.cost_usd, 6),
        }


def get_llm_invoke(
    model: str = "gpt-4o-mini",
    temperature: float = 0,
    provider: str = "openai",
    usage_accumulator: Optional["LLMUsage"] = None,
    pricing: Optional[Dict[str, float]] = None,
) -> Callable[[str, str], str]:
    """Return a (system: str, user: str) -> str callable for the requested provider.

    Supported providers: openai, anthropic, azure_openai, ollama, google, groq

    If usage_accumulator is provided, token usage and cost are accumulated into it.
    pricing must be {"input_per_1k": ..., "output_per_1k": ...} in USD.
    """
    try:
        from langchain_core.messages import SystemMessage, HumanMessage
    except ImportError as e:
        raise RuntimeError("pip install -U langchain-core") from e

    provider = (provider or "openai").lower().strip()

    if provider == "openai":
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["openai"]) from e
        llm = ChatOpenAI(model=model, temperature=temperature, max_tokens=4096)

    elif provider == "anthropic":
        try:
            from langchain_anthropic import ChatAnthropic
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["anthropic"]) from e
        llm = ChatAnthropic(model=model, temperature=temperature, max_tokens=4096)

    elif provider == "azure_openai":
        try:
            from langchain_openai import AzureChatOpenAI
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["azure_openai"]) from e
        llm = AzureChatOpenAI(azure_deployment=model, temperature=temperature, max_tokens=4096)

    elif provider == "ollama":
        try:
            from langchain_ollama import ChatOllama
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["ollama"]) from e
        llm = ChatOllama(model=model, temperature=temperature)

    elif provider == "google":
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["google"]) from e
        llm = ChatGoogleGenerativeAI(model=model, temperature=temperature)

    elif provider == "groq":
        try:
            from langchain_groq import ChatGroq
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["groq"]) from e
        llm = ChatGroq(model=model, temperature=temperature, max_tokens=4096)

    elif provider == "together":
        try:
            from langchain_together import ChatTogether
        except ImportError as e:
            raise RuntimeError(_PROVIDER_INSTALL_HINTS["together"]) from e
        llm = ChatTogether(model=model, temperature=temperature, max_tokens=4096)

    else:
        raise ValueError(
            f"Unbekannter LLM-Provider: '{provider}'. "
            "Erlaubt: openai, anthropic, azure_openai, ollama, google, groq"
        )

    def _invoke(system: str, user: str) -> str:
        msgs = [SystemMessage(content=system), HumanMessage(content=user)]
        response = llm.invoke(msgs)
        if usage_accumulator is not None:
            try:
                u = response.usage_metadata or {}
                p = _safe_int(u.get("input_tokens", 0), 0)
                c = _safe_int(u.get("output_tokens", 0), 0)
                usage_accumulator.prompt_tokens += p
                usage_accumulator.completion_tokens += c
                usage_accumulator.total_tokens += p + c
                if pricing:
                    usage_accumulator.cost_usd += (
                        p / 1000 * pricing.get("input_per_1k", 0.0)
                        + c / 1000 * pricing.get("output_per_1k", 0.0)
                    )
            except Exception:
                pass
        return response.content

    return _invoke


# ----------------------------
# Tools + Registry
# ----------------------------

class BaseAgentTool(ABC):
    name: str = ""
    description: str = ""
    usage_guide: str = ""

    def get_prompt_signature(self) -> str:
        sig = inspect.signature(self.run)
        params = [
            f"{k}"
            for k, v in sig.parameters.items()
            if k != "self" and v.kind != inspect.Parameter.VAR_KEYWORD
        ]
        return f"{self.name}({', '.join(params)})"

    def get_documentation(self) -> str:
        return (
            f"- {self.get_prompt_signature()}\n"
            f"  Beschreibung: {self.description}\n"
            f"  Wann nutzen: {self.usage_guide}\n"
        )

    @abstractmethod
    def run(self, **kwargs) -> Any:
        pass


class ListProgramsTool(BaseAgentTool):
    name = "list_programs"
    description = "Listet alle verfügbaren Programme im Projekt auf."
    usage_guide = "Wenn der User fragt 'Welche Programme gibt es?' oder einen Einstiegspunkt sucht."

    def run(self, **kwargs) -> List[Dict[str, Any]]:
        q = """
        SELECT ?programName WHERE {
          ?program rdf:type ag:class_Program ;
                   dp:hasProgramName ?programName .
        } ORDER BY ?programName
        """
        return sparql_select_raw(q)


class CalledPousTool(BaseAgentTool):
    name = "called_pous"
    description = "Zeigt alle POUs, die von einem Programm aufgerufen werden."
    usage_guide = "Bei Fragen nach Call-Graph, Struktur, 'Wer ruft wen auf?'."

    def run(self, program_name: str, **kwargs) -> List[Dict[str, Any]]:
        q = f"""
        SELECT ?calledName WHERE {{
          ?program rdf:type ag:class_Program ;
                   dp:hasProgramName "{program_name}" ;
                   op:containsPOUCall ?call .
          ?call op:callsPOU ?pou .
          ?pou dp:hasPOUName ?calledName .
        }} ORDER BY ?calledName
        """
        return sparql_select_raw(q)


class PouCallersTool(BaseAgentTool):
    name = "pou_callers"
    description = "Zeigt, welche POUs eine bestimmte POU aufrufen."
    usage_guide = "Wenn du wissen willst: 'Wer ruft POU X auf?'."

    def run(self, pou_name: str, **kwargs) -> List[Dict[str, Any]]:
        q = f"""
        SELECT ?callerName WHERE {{
          ?caller rdf:type ag:class_POU ;
                  dp:hasPOUName ?callerName ;
                  op:containsPOUCall ?call .
          ?call op:callsPOU ?callee .
          ?callee dp:hasPOUName "{pou_name}" .
        }} ORDER BY ?callerName
        """
        return sparql_select_raw(q)


class PouCodeTool(BaseAgentTool):
    name = "pou_code"
    description = "Gibt den PLC Code einer POU zurück."
    usage_guide = "Wenn der User fragt 'Zeig mir den Code von X'."

    def run(self, pou_name: str, **kwargs) -> List[Dict[str, Any]]:
        q = f"""
        SELECT ?code WHERE {{
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName "{pou_name}" ;
               dp:hasPOUCode ?code .
        }}
        """
        return sparql_select_raw(q)


class SearchVariablesTool(BaseAgentTool):
    name = "search_variables"
    description = "Sucht Variablen im KG anhand eines Substrings."
    usage_guide = "Wenn der User Variablen sucht ('enthält NotAus')."

    def run(self, name_contains: str, **kwargs) -> List[Dict[str, Any]]:
        needle = name_contains.replace('"', '\\"')
        q = f"""
        SELECT DISTINCT ?name ?type WHERE {{
          ?v rdf:type ag:class_Variable ;
             dp:hasVariableName ?name ;
             dp:hasVariableType ?type .
          FILTER(CONTAINS(LCASE(STR(?name)), LCASE("{needle}")))
        }} LIMIT 50
        """
        return sparql_select_raw(q)


class VariableTraceTool(BaseAgentTool):
    name = "variable_trace"
    description = (
        "Gibt Details zu einem Symbol zurück: Variable (Typ/HW-Adresse) oder "
        "PortInstance-Ausdruck (z.B. rStep1.Q über dp:hasExpressionText)."
    )
    usage_guide = "Wenn du zu einer Variable oder einem Instanz-Portausdruck Debug-Infos brauchst."

    def run(self, var_name: str, **kwargs) -> List[Dict[str, Any]]:
        needle = var_name.replace('"', '\\"')
        q = f"""
        SELECT DISTINCT ?category ?name ?type ?addr ?expr ?fb_instance ?fb_type ?pou WHERE {{
          {{
            ?v rdf:type ag:class_Variable ;
               dp:hasVariableName ?name .
            OPTIONAL {{ ?v dp:hasVariableType ?type . }}
            OPTIONAL {{ ?v dp:hasHardwareAddress ?addr . }}
            BIND("" AS ?expr)
            BIND("" AS ?fb_instance)
            BIND("" AS ?fb_type)
            BIND("" AS ?pou)
            BIND("Variable" AS ?category)
            FILTER(LCASE(STR(?name)) = LCASE("{needle}"))
          }}
          UNION
          {{
            ?pi rdf:type ag:class_PortInstance ;
                dp:hasExpressionText ?expr ;
                op:isPortOfInstance ?fbInst .
            OPTIONAL {{ ?fbInst dp:hasFBInstanceName ?fb_instance . }}
            OPTIONAL {{
              ?fbInst op:isInstanceOfFBType ?fbTypeUri .
              OPTIONAL {{ ?fbTypeUri dp:hasPOUName ?fb_type . }}
            }}
            OPTIONAL {{
              ?var op:representsFBInstance ?fbInst .
              ?pouUri (op:hasInternalVariable|op:usesVariable) ?var .
              OPTIONAL {{ ?pouUri dp:hasPOUName ?pou . }}
            }}
            BIND(?expr AS ?name)
            BIND("" AS ?type)
            BIND("" AS ?addr)
            BIND("PortInstance" AS ?category)
            FILTER(LCASE(STR(?expr)) = LCASE("{needle}"))
          }}
        }} LIMIT 30
        """
        return sparql_select_raw(q, max_rows=30)


class PLCSnapshotSearchTool(BaseAgentTool):
    name = "plc_snapshot_search"
    description = "Durchsucht den aktuellen plcSnapshot nach beobachteten Laufzeitwerten und Browse-Knoten."
    usage_guide = (
        "Wenn fuer eine Folgefrage ein beobachteter Runtime-Wert zum Diagnosezeitpunkt gebraucht wird "
        "(z. B. Timer, Trigger, Flags oder aktuelle Snapshot-Werte)."
    )

    def __init__(self, plc_snapshot: Any):
        self.snapshot = _unwrap_plc_snapshot(plc_snapshot)

    @staticmethod
    def _matches(name: str, query: str, *, exact: bool) -> bool:
        lhs = str(name or "").strip().lower()
        rhs = str(query or "").strip().lower()
        if not lhs or not rhs:
            return False
        if exact:
            return lhs == rhs or lhs.endswith("." + rhs)
        return rhs in lhs

    def run(
        self,
        *,
        name_contains: str = "",
        exact_name: str = "",
        include_rows: bool = False,
        max_hits: int = 20,
        **kwargs,
    ) -> Dict[str, Any]:
        vars_list = self.snapshot.get("vars")
        rows_list = self.snapshot.get("rows")
        if not isinstance(vars_list, list) and not isinstance(rows_list, list):
            return {"error": "Kein plcSnapshot im aktuellen Incident verfuegbar."}

        query = str(exact_name or name_contains or "").strip()
        if not query:
            return {"error": "Bitte 'exact_name' oder 'name_contains' angeben."}

        limit = max(1, _safe_int(max_hits, 20))
        vars_out: List[Dict[str, Any]] = []
        rows_out: List[Dict[str, Any]] = []

        for item in vars_list if isinstance(vars_list, list) else []:
            if not isinstance(item, dict):
                continue
            var_id = str(item.get("id", "") or "")
            if not self._matches(var_id, query, exact=bool(exact_name)):
                continue
            vars_out.append(
                {
                    "id": var_id,
                    "type": str(item.get("t", "") or ""),
                    "value": item.get("v"),
                }
            )
            if len(vars_out) >= limit:
                break

        if include_rows:
            for item in rows_list if isinstance(rows_list, list) else []:
                if not isinstance(item, dict):
                    continue
                row_id = str(item.get("id", "") or "")
                if not self._matches(row_id, query, exact=bool(exact_name)):
                    continue
                rows_out.append(
                    {
                        "id": row_id,
                        "nodeClass": str(item.get("nodeClass", "") or ""),
                        "type": str(item.get("t", "") or ""),
                    }
                )
                if len(rows_out) >= limit:
                    break

        return {
            "query": query,
            "vars": vars_out,
            "rows": rows_out,
            "snapshot_available": True,
        }


class ExceptionAnalysisTool(BaseAgentTool):
    name = "exception_prep"
    description = "Analysiert einen Snapshot gegen Routine-Signaturen."
    usage_guide = "Bei konkreten Sensorwerten oder 'Fehlerbild'."

    def __init__(self, kg_store: KGStore, index: RoutineIndex):
        self.kg = kg_store
        self.index = index

    def run(self, program_name: str, snapshot: Dict[str, Any], top_k: int = 5, **kwargs) -> Dict[str, Any]:
        extractor = SignatureExtractor(self.kg)
        try:
            sig = extractor.extract_signature(program_name)
        except ValueError as e:
            return {"error": str(e)}

        snap = SensorSnapshot(program_name=program_name, sensor_values=snapshot)
        check_map = classify_checkable_sensors(snap, sig)
        similar = self.index.find_similar(sig, top_k=top_k)

        return {"signature": sig.as_dict(), "checkable": check_map, "similar": similar}


class Text2SparqlTool(BaseAgentTool):
    name = "text2sparql_select"
    description = "Generiert und führt SPARQL SELECT aus (Fallback)."
    usage_guide = "NUR nutzen, wenn kein anderes Tool passt."

    def __init__(self, llm_invoke_fn: Callable, schema_card_text: str):
        self.llm_invoke = llm_invoke_fn
        self.schema_card = schema_card_text

    def run(self, question: str, max_rows: int = 50, **kwargs) -> Dict[str, Any]:
        system_prompt = f"""
Du bist ein SPARQL-Generator.
Regeln: Nur SELECT, Prefixes nutzen (rdf, ag, dp, op).
Schema:
{self.schema_card}
"""
        raw = self.llm_invoke(system_prompt, question)
        q = extract_sparql_from_llm(raw)
        rows = sparql_select_raw(q, max_rows=max_rows)
        return {"sparql": q, "rows": rows}


class ToolRegistry:
    def __init__(self):
        self._tools: Dict[str, BaseAgentTool] = {}

    def register(self, tool: BaseAgentTool):
        self._tools[tool.name] = tool

    def get_system_prompt_part(self) -> str:
        parts = [t.get_documentation() for t in self._tools.values()]
        return "Verfügbare Tools:\n" + "".join(parts)

    def execute(self, tool_name: str, args: Dict[str, Any]) -> Any:
        tool = self._tools.get(tool_name)
        if not tool:
            return {"error": f"Tool '{tool_name}' not found."}
        try:
            return tool.run(**args)
        except Exception as e:
            return {"error": f"Error in '{tool_name}': {e}"}


# ----------------------------
# (Optional) RAG tools
# ----------------------------

def build_vector_index(kg_store: KGStore, tool_registry: ToolRegistry):
    try:
        from langchain_openai import OpenAIEmbeddings
        from langchain_community.vectorstores import FAISS
        from langchain_core.documents import Document
    except Exception:
        return None

    docs = []
    for pou_name in kg_store._pou_by_name.keys():
        code_res = tool_registry.execute("pou_code", {"pou_name": pou_name})
        if isinstance(code_res, list) and code_res and "code" in code_res[0]:
            code_text = code_res[0]["code"]
            if code_text:
                content = f"POU Name: {pou_name}\nCode Content: {code_text[:1000]}"
                meta = {"type": "POU", "name": pou_name}
                docs.append(Document(page_content=content, metadata=meta))

    if not docs:
        return None

    try:
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        vs = FAISS.from_documents(docs, embeddings)
        return vs
    except Exception:
        return None


class SemanticSearchTool(BaseAgentTool):
    name = "semantic_search"
    description = "Sucht semantisch nach POUs oder Logik anhand von Beschreibungen (RAG)."
    usage_guide = "Fallback wenn User den exakten Namen nicht kennt."

    def __init__(self, vector_store):
        self.vs = vector_store

    def run(self, query: str, k: int = 3, **kwargs) -> List[Dict[str, Any]]:
        if not self.vs:
            return [{"error": "Kein Vektor-Index verfügbar."}]
        docs = self.vs.similarity_search(query, k=k)
        return [{"pou_name": d.metadata.get("name"), "snippet": d.page_content[:300] + "..."} for d in docs]


class GeneralSearchTool(BaseAgentTool):
    name = "general_search"
    description = "Sucht universell nach POUs, Variablen oder Ports."
    usage_guide = "Wenn unklar ist, ob Name POU oder Variable ist (z.B. wegen Punkten)."

    def run(self, name_contains: str, **kwargs) -> List[Dict[str, Any]]:
        needle = name_contains.replace('"', '\\"')
        needle_dot = needle.replace(".", "__dot__")
        q = f"""
        SELECT DISTINCT ?name ?type ?category WHERE {{
          {{
            ?s rdf:type ag:class_POU ;
               dp:hasPOUName ?name .
            BIND("" AS ?type)
            BIND("POU" AS ?category)
          }}
          UNION
          {{
            ?s rdf:type ag:class_Variable ;
               dp:hasVariableName ?name ;
               dp:hasVariableType ?type .
            BIND("Variable" AS ?category)
          }}
          UNION
          {{
            ?s rdf:type ag:class_Port ;
               dp:hasPortName ?name ;
               dp:hasPortType ?type .
            BIND("Port" AS ?category)
          }}
          UNION
          {{
            ?s rdf:type ag:class_PortInstance ;
               dp:hasExpressionText ?name .
            BIND("" AS ?type)
            BIND("PortInstance" AS ?category)
          }}
          FILTER(
            CONTAINS(LCASE(STR(?name)), LCASE("{needle}")) ||
            CONTAINS(LCASE(STR(?s)), LCASE("{needle_dot}"))
          )
        }} LIMIT 20
        """
        return sparql_select_raw(q)


@dataclass(frozen=True)
class InvestigationNode:
    type: str
    key: str
    depth: int
    priority: int
    parent: str
    reason: str

    @property
    def node_id(self) -> str:
        return f"{self.type}:{self.key}"


class GraphInvestigateTool(BaseAgentTool):
    name = "graph_investigate"
    description = (
        "Generischer Suchalgorithmus: startet mit Seed-Knoten und expandiert iterativ "
        "(Code-/KG-Suche, Call-Chain, Setter-Guards), bis keine neuen Knoten mehr entstehen."
    )
    usage_guide = (
        "Nutzen, wenn du eine echte Root-Cause-Kette brauchst (Setter -> Bedingung -> Upstream-Signale). "
        "Gib seed_terms (z.B. Trigger-Variable, lastSkill, wichtige Ports/Variablen) und optional target_terms."
    )

    _kw = {
        "IF", "THEN", "ELSE", "ELSIF", "END_IF", "CASE", "OF", "END_CASE",
        "FOR", "TO", "DO", "END_FOR", "WHILE", "END_WHILE", "REPEAT", "UNTIL", "END_REPEAT",
        "AND", "OR", "NOT", "XOR",
        "TRUE", "FALSE",
        "VAR", "END_VAR", "VAR_INPUT", "VAR_OUTPUT", "VAR_IN_OUT", "VAR_TEMP", "VAR_GLOBAL", "VAR_CONFIG",
        "R_TRIG", "F_TRIG", "RS", "SR",
    }

    @staticmethod
    def _escape_sparql_string(s: str) -> str:
        return (s or "").replace("\\", "\\\\").replace('"', '\\"')

    @staticmethod
    def _snippets(code: str, needle: str, radius: int = 10, max_snips: int = 6) -> List[Dict[str, Any]]:
        if not code or not needle:
            return []
        lines = code.splitlines()
        hits = [i for i, ln in enumerate(lines) if needle in ln]
        out: List[Dict[str, Any]] = []
        for idx in hits[:max_snips]:
            lo = max(0, idx - radius)
            hi = min(len(lines), idx + radius + 1)
            out.append(
                {
                    "line": idx + 1,
                    "needle": needle,
                    "snippet": "\n".join(lines[lo:hi]),
                }
            )
        return out

    @classmethod
    def _extract_symbols(cls, text: str) -> List[str]:
        if not text:
            return []
        toks = re.findall(r"[A-Za-z_][A-Za-z0-9_.]*", text)
        out: List[str] = []
        seen: Set[str] = set()
        for t in toks:
            if t.upper() in cls._kw:
                continue
            if re.fullmatch(r"\d+", t):
                continue
            # kleine Heuristik: sehr kurze Tokens ignorieren (Q, I, etc.) außer bei dotted form
            if len(t) <= 1 and "." not in t:
                continue
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    @staticmethod
    def _extract_if_conditions(snippet: str, max_conditions: int = 3) -> List[str]:
        if not snippet:
            return []
        lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]
        conds: List[str] = []
        for ln in lines:
            m = re.match(r"(?i)^IF\s+(.+?)\s+THEN\b", ln)
            if m:
                conds.append(m.group(1).strip())
                if len(conds) >= max_conditions:
                    break
        return conds

    @staticmethod
    def _general_search(name_contains: str, limit: int = 20) -> List[Dict[str, Any]]:
        needle = GraphInvestigateTool._escape_sparql_string(name_contains)
        needle_dot = needle.replace(".", "__dot__")
        q = f"""
        SELECT DISTINCT ?name ?type ?category WHERE {{
          {{
            ?s rdf:type ag:class_POU ;
               dp:hasPOUName ?name .
            BIND("" AS ?type)
            BIND("POU" AS ?category)
          }}
          UNION
          {{
            ?s rdf:type ag:class_Variable ;
               dp:hasVariableName ?name ;
               dp:hasVariableType ?type .
            BIND("Variable" AS ?category)
          }}
          UNION
          {{
            ?s rdf:type ag:class_Port ;
               dp:hasPortName ?name ;
               dp:hasPortType ?type .
            BIND("Port" AS ?category)
          }}
          UNION
          {{
            ?s rdf:type ag:class_PortInstance ;
               dp:hasExpressionText ?name .
            BIND("" AS ?type)
            BIND("PortInstance" AS ?category)
          }}
          FILTER(
            CONTAINS(LCASE(STR(?name)), LCASE("{needle}")) ||
            CONTAINS(LCASE(STR(?s)), LCASE("{needle_dot}"))
          )
        }} LIMIT {int(limit)}
        """
        return sparql_select_raw(q, max_rows=limit)

    @staticmethod
    def _pou_code(pou_name: str) -> str:
        pn = GraphInvestigateTool._escape_sparql_string(pou_name)
        q = f"""
        SELECT ?code WHERE {{
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName "{pn}" ;
               dp:hasPOUCode ?code .
        }}
        """
        rows = sparql_select_raw(q, max_rows=3)
        return rows[0].get("code", "") if rows else ""

    @staticmethod
    def _pou_callers(pou_name: str, limit: int = 50) -> List[str]:
        pn = GraphInvestigateTool._escape_sparql_string(pou_name)
        q = f"""
        SELECT ?callerName WHERE {{
          ?caller rdf:type ag:class_POU ;
                  dp:hasPOUName ?callerName ;
                  op:containsPOUCall ?call .
          ?call op:callsPOU ?callee .
          ?callee dp:hasPOUName "{pn}" .
        }} ORDER BY ?callerName LIMIT {int(limit)}
        """
        rows = sparql_select_raw(q, max_rows=limit)
        out = []
        for r in rows:
            n = (r.get("callerName") or "").strip()
            if n:
                out.append(n)
        return out

    @staticmethod
    def _variable_trace(var_name: str) -> List[Dict[str, Any]]:
        needle = GraphInvestigateTool._escape_sparql_string(var_name)
        q = f"""
        SELECT DISTINCT ?category ?name ?type ?addr ?expr ?fb_instance ?fb_type ?pou WHERE {{
          {{
            ?v rdf:type ag:class_Variable ;
               dp:hasVariableName ?name .
            OPTIONAL {{ ?v dp:hasVariableType ?type . }}
            OPTIONAL {{ ?v dp:hasHardwareAddress ?addr . }}
            BIND("" AS ?expr)
            BIND("" AS ?fb_instance)
            BIND("" AS ?fb_type)
            BIND("" AS ?pou)
            BIND("Variable" AS ?category)
            FILTER(LCASE(STR(?name)) = LCASE("{needle}"))
          }}
          UNION
          {{
            ?pi rdf:type ag:class_PortInstance ;
                dp:hasExpressionText ?expr ;
                op:isPortOfInstance ?fbInst .
            OPTIONAL {{ ?fbInst dp:hasFBInstanceName ?fb_instance . }}
            OPTIONAL {{
              ?fbInst op:isInstanceOfFBType ?fbTypeUri .
              OPTIONAL {{ ?fbTypeUri dp:hasPOUName ?fb_type . }}
            }}
            OPTIONAL {{
              ?var op:representsFBInstance ?fbInst .
              ?pouUri (op:hasInternalVariable|op:usesVariable) ?var .
              OPTIONAL {{ ?pouUri dp:hasPOUName ?pou . }}
            }}
            BIND(?expr AS ?name)
            BIND("" AS ?type)
            BIND("" AS ?addr)
            BIND("PortInstance" AS ?category)
            FILTER(LCASE(STR(?expr)) = LCASE("{needle}"))
          }}
        }} LIMIT 30
        """
        return sparql_select_raw(q, max_rows=30)

    @staticmethod
    def _code_search_pous(term: str, limit: int = 20) -> List[Dict[str, Any]]:
        needle = GraphInvestigateTool._escape_sparql_string(term)
        q = f"""
        SELECT ?pou_name ?code WHERE {{
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName ?pou_name ;
               dp:hasPOUCode ?code .
          FILTER(CONTAINS(LCASE(STR(?code)), LCASE("{needle}")))
        }} ORDER BY ?pou_name LIMIT {int(limit)}
        """
        return sparql_select_raw(q, max_rows=limit)

    def run(
        self,
        *,
        seed_terms: List[str],
        target_terms: Optional[List[str]] = None,
        max_iters: int = 40,
        max_nodes: int = 240,
        max_pous_per_term: int = 10,
        max_callers_per_pou: int = 25,
        snippet_radius: int = 10,
        max_snips: int = 6,
    ) -> Dict[str, Any]:
        """
        Führt eine iterative Expansion durch (BFS-ähnlich mit Priorität).
        - seed_terms: Startknoten (Variablen/POUs/Literale/Symbole)
        - target_terms: optional; wenn gesetzt, versucht der Report diese Targets bevorzugt zu erklären
        """
        target_terms = target_terms or []
        seed_terms = [s for s in (seed_terms or []) if str(s).strip()]

        visited: Set[str] = set()
        frontier: deque[InvestigationNode] = deque()
        evidence: Dict[str, Any] = {}
        edges: List[Dict[str, Any]] = []

        tool_cache: Dict[str, Any] = {}

        def cache_get(k: str) -> Any:
            return tool_cache.get(k)

        def cache_set(k: str, v: Any) -> Any:
            tool_cache[k] = v
            return v

        def push(node: InvestigationNode) -> None:
            if node.node_id in visited:
                return
            # einfache Priorisierung: "Priority queue" via sortierten Insert (kleine Datenmengen)
            if len(frontier) == 0:
                frontier.append(node)
                return
            inserted = False
            for i, cur in enumerate(frontier):
                if node.priority > cur.priority:
                    frontier.insert(i, node)
                    inserted = True
                    break
            if not inserted:
                frontier.append(node)

        def add_node(node_type: str, key: str, *, depth: int, priority: int, parent: str, reason: str) -> None:
            key = str(key).strip()
            if not key:
                return
            node = InvestigationNode(type=node_type, key=key, depth=depth, priority=priority, parent=parent, reason=reason)
            if node.node_id in visited:
                return
            push(node)

        def record_edge(src: InvestigationNode, dst_type: str, dst_key: str, kind: str, meta: Optional[Dict[str, Any]] = None) -> None:
            edges.append(
                {
                    "src": src.node_id,
                    "dst": f"{dst_type}:{dst_key}",
                    "kind": kind,
                    "meta": meta or {},
                }
            )

        def expand_term(node: InvestigationNode) -> None:
            term = node.key
            # Disambiguate: falls es eindeutig POU/Variable ist, Knoten anlegen
            ckey = f"general_search:{term}"
            hits = cache_get(ckey)
            if hits is None:
                hits = cache_set(ckey, self._general_search(term, limit=20))
            for h in hits:
                cat = (h.get("category") or "").strip()
                name = (h.get("name") or "").strip()
                if not cat or not name:
                    continue
                if cat == "POU":
                    add_node("pou", name, depth=node.depth + 1, priority=node.priority, parent=node.node_id, reason="general_search")
                    record_edge(node, "pou", name, "resolves_to")
                elif cat == "Variable":
                    add_node("var", name, depth=node.depth + 1, priority=node.priority, parent=node.node_id, reason="general_search")
                    record_edge(node, "var", name, "resolves_to")
                elif cat == "Port":
                    add_node("port", name, depth=node.depth + 1, priority=node.priority - 1, parent=node.node_id, reason="general_search")
                    record_edge(node, "port", name, "resolves_to")
                elif cat == "PortInstance":
                    add_node("var", name, depth=node.depth + 1, priority=node.priority, parent=node.node_id, reason="general_search")
                    record_edge(node, "var", name, "resolves_to")

            # Immer zusätzlich: Code-Suche nach dem term (um Setter/Guards zu finden)
            ckey2 = f"code_search:{term}"
            pou_rows = cache_get(ckey2)
            if pou_rows is None:
                pou_rows = cache_set(ckey2, self._code_search_pous(term, limit=max_pous_per_term))

            ev_key = node.node_id
            ev = evidence.get(ev_key) if isinstance(evidence.get(ev_key), dict) else {}
            ev = dict(ev)
            ev.setdefault("code_hits", [])

            for r in pou_rows:
                pou_name = (r.get("pou_name") or "").strip()
                code = r.get("code", "") or ""
                if not pou_name:
                    continue

                sn_any = self._snippets(code, term, radius=snippet_radius, max_snips=max_snips)
                sn_true = self._snippets(code, f"{term} := TRUE", radius=snippet_radius, max_snips=max_snips)
                sn_false = self._snippets(code, f"{term} := FALSE", radius=snippet_radius, max_snips=max_snips)
                item = {
                    "pou_name": pou_name,
                    "snips_TRUE": sn_true,
                    "snips_FALSE": sn_false,
                    "snips_any": sn_any,
                }
                ev["code_hits"].append(item)

                # Setter-POU ist meistens relevant -> als POU-Knoten hinzufügen
                add_node("pou", pou_name, depth=node.depth + 1, priority=node.priority - 1, parent=node.node_id, reason="code_search_hit")
                record_edge(node, "pou", pou_name, "mentioned_in_code")

                # Aus Snippets: IF-Guards extrahieren -> expr + sym
                for sn in sn_true + sn_any:
                    conds = self._extract_if_conditions(sn.get("snippet", ""))
                    for cond in conds:
                        add_node("expr", cond, depth=node.depth + 1, priority=node.priority + 2, parent=node.node_id, reason="if_guard")
                        record_edge(node, "expr", cond, "guard_of", meta={"pou": pou_name, "line": sn.get("line")})
                        for sym in self._extract_symbols(cond):
                            add_node("term", sym, depth=node.depth + 2, priority=node.priority + 1, parent=f"expr:{cond}", reason="symbol_in_guard")

            evidence[ev_key] = ev

        def expand_var(node: InvestigationNode) -> None:
            # var-trace + dann wie term expandieren (Setter/Guards)
            ckey = f"variable_trace:{node.key}"
            rows = cache_get(ckey)
            if rows is None:
                rows = cache_set(ckey, self._variable_trace(node.key))
            evidence[node.node_id] = {"trace": rows}
            expand_term(InvestigationNode(type="term", key=node.key, depth=node.depth, priority=node.priority, parent=node.parent, reason=node.reason))

        def expand_pou(node: InvestigationNode) -> None:
            code_key = f"pou_code:{node.key}"
            code = cache_get(code_key)
            if code is None:
                code = cache_set(code_key, self._pou_code(node.key))

            callers_key = f"pou_callers:{node.key}"
            callers = cache_get(callers_key)
            if callers is None:
                callers = cache_set(callers_key, self._pou_callers(node.key, limit=max_callers_per_pou))

            ev = evidence.get(node.node_id) if isinstance(evidence.get(node.node_id), dict) else {}
            ev = dict(ev)
            ev["callers"] = callers
            evidence[node.node_id] = ev

            # Call-chain nach oben
            for c in callers:
                add_node("pou", c, depth=node.depth + 1, priority=node.priority - 2, parent=node.node_id, reason="pou_callers")
                record_edge(node, "pou", c, "called_by")

            # Deklarationen: wenn es dotted Symbole gibt, Base-Name extrahieren und Typ suchen
            decls = {}
            for m in re.finditer(r"(?m)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*;", code or ""):
                decls[m.group(1)] = m.group(2)
            if decls:
                ev["decls"] = decls

            # Wenn target_terms im Code vorkommen, extrahiere Snippets + Guards
            for t in (target_terms or []):
                if not t or not code:
                    continue
                if t not in code:
                    continue
                sn = self._snippets(code, t, radius=snippet_radius, max_snips=max_snips)
                if sn:
                    ev.setdefault("target_snips", {})[t] = sn
                    for s in sn:
                        for cond in self._extract_if_conditions(s.get("snippet", "")):
                            add_node("expr", cond, depth=node.depth + 1, priority=node.priority + 2, parent=node.node_id, reason="if_guard")
                            for sym in self._extract_symbols(cond):
                                add_node("term", sym, depth=node.depth + 2, priority=node.priority + 1, parent=f"expr:{cond}", reason="symbol_in_guard")

            evidence[node.node_id] = ev

            # Wenn im POU Variableninstanzen deklariert sind, die wie Trig/Req/Busy heißen, als Terms hinzufügen
            for var_name, typ in list(decls.items())[:80]:
                if any(k in var_name.lower() for k in ["trig", "trigger", "busy", "req", "request", "alarm", "fault", "stoer", "diagnose"]):
                    add_node("term", var_name, depth=node.depth + 1, priority=node.priority - 1, parent=node.node_id, reason="heuristic_decl")
                    add_node("term", typ, depth=node.depth + 1, priority=node.priority - 3, parent=node.node_id, reason="decl_type")

        def expand_expr(node: InvestigationNode) -> None:
            # Expr ist nur ein Symbol-Generator
            syms = self._extract_symbols(node.key)
            evidence[node.node_id] = {"symbols": syms}
            for sym in syms:
                add_node("term", sym, depth=node.depth + 1, priority=node.priority - 1, parent=node.node_id, reason="expr_symbol")

        def expand_port(node: InvestigationNode) -> None:
            # Ports können wir aktuell nur als Term weiterverfolgen (Wiring steckt in Code)
            expand_term(InvestigationNode(type="term", key=node.key, depth=node.depth, priority=node.priority, parent=node.parent, reason=node.reason))

        # seeds
        for s in seed_terms:
            add_node("term", s, depth=0, priority=10, parent="", reason="seed")
        for t in target_terms:
            add_node("term", t, depth=0, priority=12, parent="", reason="target")

        it = 0
        while frontier and it < int(max_iters) and len(visited) < int(max_nodes):
            it += 1
            node = frontier.popleft()
            if node.node_id in visited:
                continue
            visited.add(node.node_id)

            if node.type == "term":
                expand_term(node)
            elif node.type == "var":
                expand_var(node)
            elif node.type == "pou":
                expand_pou(node)
            elif node.type == "expr":
                expand_expr(node)
            elif node.type == "port":
                expand_port(node)
            else:
                expand_term(InvestigationNode(type="term", key=node.key, depth=node.depth, priority=node.priority, parent=node.parent, reason=node.reason))

        return {
            "stats": {
                "iterations": it,
                "visited_count": len(visited),
                "frontier_left": len(frontier),
                "cache_size": len(tool_cache),
            },
            "visited_nodes": sorted(visited),
            "edges": edges,
            "evidence": evidence,
        }


class StringTripleSearchTool(BaseAgentTool):
    name = "string_triple_search"
    description = "Sucht einen String als Substring in allen Tripeln (Subject, Predicate, Object)."
    usage_guide = "Letzter Fallback, wenn strukturierte Tools keine Treffer liefern."

    def __init__(self, kg_store: KGStore):
        self.graph = kg_store.g

    def run(self, term: str, limit: int = 30, **kwargs) -> List[Dict[str, Any]]:
        t = term.lower()
        hits = []
        for s, p, o in self.graph:
            ss, pp, oo = str(s).lower(), str(p).lower(), str(o).lower()
            if t in ss or t in pp or t in oo:
                hits.append({"s": str(s), "p": str(p), "o": str(o)})
                if len(hits) >= limit:
                    break
        return hits

class EvD2DiagnosisTool(BaseAgentTool):
    name = "evd2_diagnoseplan"
    description = (
        "Erstellt einen deterministischen Diagnoseplan für evD2: warum D2 aktiv wurde "
        "und warum OPCUA.TriggerD2 TRUE ist, inkl. GEMMA-Port-Chain und FBD-RS-Logik."
    )
    usage_guide = (
        "Nutzen, wenn triggerEvent=evD2 und OPCUA.TriggerD2 TRUE ist. "
        "Tool liefert: GEMMA-State-Machine, D2 Output-Port, Call-Chain bis MAIN, "
        "Trigger-Setter, lastSkill-Setter, Skill->FBInst Mapping, und RS(Set/Reset) für D2."
    )

    @staticmethod
    def _snippets(code: str, needle: str, radius: int = 2, max_snips: int = 5) -> list[dict]:
        if not code or not needle:
            return []
        lines = code.splitlines()
        hits = [i for i, ln in enumerate(lines) if needle in ln]
        out = []
        for idx in hits[:max_snips]:
            lo = max(0, idx - radius)
            hi = min(len(lines), idx + radius + 1)
            out.append(
                {
                    "line": idx + 1,
                    "needle": needle,
                    "snippet": "\n".join(lines[lo:hi]),
                }
            )
        return out

    @staticmethod
    def _extract_main_lines(code: str) -> dict:
        if not code:
            return {"lines": []}
        needles = [
            "TriggerD2",
            "Diagnose_gefordert",
            "fbDiag",
            "fbBA",
            "lastExecutedSkill",
            "lastExecutedProcess",
            "Auto_Stoerung",
            "D2",
        ]
        lines = []
        for i, ln in enumerate(code.splitlines()):
            if any(n in ln for n in needles):
                lines.append({"line": i + 1, "text": ln.rstrip()})
        return {"lines": lines}

    @staticmethod
    def _parse_fbd_assignments(fbd_py: str) -> dict:
        """
        Parst die Python-Repräsentation eines FBD-POUs.

        Erwartet u.a.:
          V_40000000017 = RS_4(V_40000000010, V_40000000016)
          V_40000000016 = OR_7(D3, Alt_abort)
          D2 = V_40000000017

        RS/SR werden semantisch interpretiert:
          1. Argument = Set-Bedingung
          2. Argument = Reset-Bedingung
        """
        assigns: dict[str, dict] = {}
        out_vars: dict[str, str] = {}

        call_re = re.compile(r"^(V_\d+)\s*=\s*([A-Za-z_]+\d*)\((.*)\)\s*$")
        out_re = re.compile(r"^([A-Za-z][A-Za-z0-9_]*)\s*=\s*(V_\d+)\s*$")

        for ln in (fbd_py or "").splitlines():
            ln = ln.strip()
            m = call_re.match(ln)
            if m:
                var, func, args = m.group(1), m.group(2), m.group(3)
                args_list = [a.strip() for a in args.split(",") if a.strip()]
                assigns[var] = {"func": func, "args": args_list}
                continue
            m2 = out_re.match(ln)
            if m2:
                out_name, v = m2.group(1), m2.group(2)
                out_vars[out_name] = v

        return {"assigns": assigns, "out_vars": out_vars}

    @staticmethod
    def _expr_of(token: str, assigns: dict, depth: int = 0, max_depth: int = 25) -> str:
        token = (token or "").strip()
        if depth >= max_depth:
            return token

        if token in {"TRUE", "FALSE", "True", "False"}:
            return token.upper() if token in {"TRUE", "FALSE"} else token
        if re.fullmatch(r"\d+", token):
            return token

        if not token.startswith("V_"):
            return token

        node = assigns.get(token)
        if not node:
            return token

        func = node.get("func", "")
        args = node.get("args", [])

        if func.startswith("OR") and len(args) >= 2:
            return f"({EvD2DiagnosisTool._expr_of(args[0], assigns, depth+1)} ODER {EvD2DiagnosisTool._expr_of(args[1], assigns, depth+1)})"
        if func.startswith("AND") and len(args) >= 2:
            return f"({EvD2DiagnosisTool._expr_of(args[0], assigns, depth+1)} UND {EvD2DiagnosisTool._expr_of(args[1], assigns, depth+1)})"
        if func.startswith("NOT") and len(args) >= 1:
            return f"(NICHT {EvD2DiagnosisTool._expr_of(args[0], assigns, depth+1)})"

        if len(args) == 0:
            return token
        if len(args) == 1:
            return f"{func}({EvD2DiagnosisTool._expr_of(args[0], assigns, depth+1)})"
        return f"{func}({', '.join(EvD2DiagnosisTool._expr_of(a, assigns, depth+1) for a in args)})"

    @staticmethod
    def _extract_d2_logic_from_fbd_code(code: str) -> dict:
        parsed = EvD2DiagnosisTool._parse_fbd_assignments(code or "")
        assigns = parsed.get("assigns", {})
        out_vars = parsed.get("out_vars", {})

        d2_v = out_vars.get("D2")
        if not d2_v:
            return {"error": "Konnte D2-Zuweisung (D2 = V_...) im FBD-Code nicht finden."}

        node = assigns.get(d2_v)
        if not node:
            return {"error": f"Konnte Ursprung von {d2_v} im FBD-Code nicht finden."}

        func = node.get("func", "")
        args = node.get("args", [])
        if not (func.startswith("RS") or func.startswith("SR")) or len(args) < 2:
            return {
                "warning": "D2 stammt nicht direkt aus einem RS/SR-Call oder hat unerwartete Argumente.",
                "d2_var": d2_v,
                "origin": node,
            }

        set_arg, reset_arg = args[0], args[1]
        set_expr = EvD2DiagnosisTool._expr_of(set_arg, assigns)
        reset_expr = EvD2DiagnosisTool._expr_of(reset_arg, assigns)

        def tokens(expr: str) -> list[str]:
            t = re.findall(r"\b[A-Za-z_][A-Za-z0-9_\.]*\b", expr)
            blacklist = {"ODER", "UND", "NICHT"}
            return sorted(
                {
                    x
                    for x in t
                    if x not in blacklist
                    and not x.startswith("V_")
                    and not x.startswith("OR")
                    and not x.startswith("AND")
                    and not x.startswith("NOT")
                }
            )

        return {
            "d2_var": d2_v,
            "rs_func": func,
            "set_condition": set_expr,
            "reset_condition": reset_expr,
            "influencing_signals": {"set": tokens(set_expr), "reset": tokens(reset_expr)},
        }

    @staticmethod
    def _gemma_state_aliases(active_state: str) -> List[str]:
        text = str(active_state or "").strip()
        if not text:
            return []

        out: List[str] = []
        seen: Set[str] = set()

        def add(value: str) -> None:
            v = str(value or "").strip()
            if not v:
                return
            key = v.upper()
            if key in seen:
                return
            seen.add(key)
            out.append(v)

        add(text)
        if "." in text:
            add(text.rsplit(".", 1)[-1].strip())

        m = re.search(r"([AFD]\d+)$", text, flags=re.I)
        if m:
            add(str(m.group(1) or "").strip().upper())

        return out

    @staticmethod
    def _pick_or_branch(expr: str, active_state: str) -> dict:
        """
        Heuristik: Wenn expr eine ODER-Verknüpfung enthält, wähle den Branch, der active_state enthält.
        Liefert {picked, branches, explanation}.
        """
        active_state = (active_state or "").strip()
        if not expr:
            return {"picked": "", "branches": [], "explanation": ""}

        branches = [b.strip() for b in str(expr).split("ODER")]
        if len(branches) <= 1 or not active_state:
            return {"picked": "", "branches": branches if len(branches) > 1 else [], "explanation": ""}

        for b in branches:
            for alias in EvD2DiagnosisTool._gemma_state_aliases(active_state):
                if re.search(rf"\\b{re.escape(alias)}\\b", b):
                    return {
                        "picked": b,
                        "branches": branches,
                        "explanation": f"Active GEMMA state hint '{active_state}' matches this OR-branch via '{alias}'.",
                    }

        return {
            "picked": "",
            "branches": branches,
            "explanation": f"Active GEMMA state hint '{active_state}' did not match any OR-branch.",
        }

    def run(
        self,
        last_skill: str = "",
        last_gemma_state: str = "",
        trigger_var: str = "OPCUA.TriggerD2",
        event_name: str = "evD2",
        port_name_contains: str = "D2",
        max_rows: int = 200,
        **kwargs,
    ) -> dict:
        # 1) GEMMA State Machine POUs
        q_gemma = f"""
        SELECT ?pou ?pou_name ?lang WHERE {{
          ?pou rdf:type ag:class_CustomFBType ;
               dp:isGEMMAStateMachine true ;
               dp:hasPOUName ?pou_name .
          OPTIONAL {{ ?pou dp:hasPOULanguage ?lang . }}
        }} ORDER BY ?pou_name
        """
        gemma_rows = sparql_select_raw(q_gemma, max_rows=max_rows)

        # 2) D2 Output ports
        q_ports = f"""
        SELECT ?pou_name ?port ?port_name ?dir WHERE {{
          ?pou rdf:type ag:class_CustomFBType ;
               dp:isGEMMAStateMachine true ;
               dp:hasPOUName ?pou_name ;
               op:hasPort ?port .
          ?port dp:hasPortDirection ?dir ;
                dp:hasPortName ?port_name .
          FILTER(?dir = "Output")
          FILTER(CONTAINS(LCASE(STR(?port_name)), LCASE("{port_name_contains}")))
        }} ORDER BY ?pou_name ?port_name
        """
        d2_ports = sparql_select_raw(q_ports, max_rows=max_rows)

        # 3) Port instance call chain
        call_chain = []
        for pr in d2_ports[:10]:
            port_uri = pr.get("port")
            if not port_uri:
                continue
            q_chain = f"""
            SELECT ?port_name ?port_instance ?fb_inst ?var_inst ?caller_pou_name WHERE {{
              BIND(<{port_uri}> AS ?p)
              ?p dp:hasPortName ?port_name .
              ?port_instance op:instantiatesPort ?p ;
                             op:isPortOfInstance ?fb_inst .
              ?var_inst op:representsFBInstance ?fb_inst .
              ?caller_pou op:usesVariable ?var_inst ;
                         dp:hasPOUName ?caller_pou_name .
            }}
            """
            call_chain.extend(sparql_select_raw(q_chain, max_rows=max_rows))

        # 4) MAIN wiring
        q_main = """
        SELECT ?code WHERE {
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName "MAIN" ;
               dp:hasPOUCode ?code .
        }
        """
        main_rows = sparql_select_raw(q_main, max_rows=5)
        main_code = main_rows[0].get("code", "") if main_rows else ""

        # 5) Trigger setters
        trig_needle = trigger_var
        q_trig = f"""
        SELECT ?pou_name ?code WHERE {{
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName ?pou_name ;
               dp:hasPOUCode ?code .
          FILTER(CONTAINS(STR(?code), "{trig_needle}"))
        }} ORDER BY ?pou_name
        """
        trig_rows = sparql_select_raw(q_trig, max_rows=max_rows)
        trigger_setters = []
        for r in trig_rows:
            code = r.get("code", "") or ""
            trigger_setters.append(
                {
                    "pou_name": r.get("pou_name", ""),
                    # Größerer Radius, damit IF/CASE-Kontext sichtbar wird (Root-Cause statt nur "wurde TRUE gesetzt").
                    "snips_TRUE": self._snippets(code, f"{trig_needle} := TRUE", radius=10, max_snips=8),
                    "snips_FALSE": self._snippets(code, f"{trig_needle} := FALSE", radius=10, max_snips=8),
                    "snips_any": self._snippets(code, trig_needle, radius=4, max_snips=8),
                }
            )

        # 6) lastSkill setters
        skill_setters = []
        if last_skill:
            needle_skill = f"'{last_skill}'"
            q_skill = f"""
            SELECT ?pou_name ?code WHERE {{
              ?pou rdf:type ag:class_POU ;
                   dp:hasPOUName ?pou_name ;
                   dp:hasPOUCode ?code .
              FILTER(CONTAINS(STR(?code), "{needle_skill}"))
            }} ORDER BY ?pou_name
            """
            for r in sparql_select_raw(q_skill, max_rows=max_rows):
                code = r.get("code", "") or ""
                skill_setters.append(
                    {"pou_name": r.get("pou_name", ""), "snips": self._snippets(code, needle_skill, radius=2, max_snips=8)}
                )
        else:
            q_skill_any = """
            SELECT ?pou_name ?code WHERE {
              ?pou rdf:type ag:class_POU ;
                   dp:hasPOUName ?pou_name ;
                   dp:hasPOUCode ?code .
              FILTER(CONTAINS(STR(?code), "OPCUA.lastExecutedSkill"))
            } ORDER BY ?pou_name
            """
            for r in sparql_select_raw(q_skill_any, max_rows=max_rows):
                code = r.get("code", "") or ""
                skill_setters.append(
                    {"pou_name": r.get("pou_name", ""), "snips": self._snippets(code, "OPCUA.lastExecutedSkill", radius=2, max_snips=8)}
                )

        # 7) Skill -> FBType -> FBInstance
        skill_instances = []
        if last_skill:
            needle_skill = f"'{last_skill}'"
            q_fbtypes = f"""
            SELECT ?fbtype ?fbtype_name ?code WHERE {{
              ?fbtype rdf:type ag:class_FBType ;
                      dp:hasPOUName ?fbtype_name ;
                      dp:hasPOUCode ?code .
              FILTER(CONTAINS(STR(?code), "{needle_skill}"))
            }} ORDER BY ?fbtype_name
            """
            fbtypes = sparql_select_raw(q_fbtypes, max_rows=max_rows)
            for ft in fbtypes:
                fbtype_uri = ft.get("fbtype")
                if not fbtype_uri:
                    continue
                q_insts = f"""
                SELECT ?fb_inst ?var_name ?caller_pou_name WHERE {{
                  ?fb_inst rdf:type ag:class_FBInstance ;
                           op:isInstanceOfFBType <{fbtype_uri}> .
                  ?var_inst op:representsFBInstance ?fb_inst ;
                            dp:hasVariableName ?var_name .
                  ?caller_pou op:usesVariable ?var_inst ;
                              dp:hasPOUName ?caller_pou_name .
                }} ORDER BY ?caller_pou_name
                """
                inst_rows = sparql_select_raw(q_insts, max_rows=max_rows)
                skill_instances.append({"fbtype_name": ft.get("fbtype_name", ""), "fbtype_uri": fbtype_uri, "instances": inst_rows})

        d2_callers = sorted({r.get("caller_pou_name", "") for r in call_chain if r.get("caller_pou_name")})
        skill_setter_names = sorted({r.get("pou_name", "") for r in skill_setters if r.get("pou_name")})
        overlap = sorted(set(d2_callers) & set(skill_setter_names))
        scope_hints = _collect_scope_hints(last_skill, skill_setter_names, overlap)

        # 8) GEMMA FBD D2 RS-Analyse
        gemma_fbd_logic = {}
        selected_gemma_pou_name = ""
        if gemma_rows:
            ranked_gemma_rows = sorted(
                gemma_rows,
                key=lambda gr: (
                    -(
                        _score_name_with_scope(
                            str(gr.get("pou_name", "") or ""),
                            scope_hints=scope_hints,
                        )
                        + (5 if str(gr.get("lang", "") or "").strip().upper() == "FBD" else 0)
                    ),
                    str(gr.get("pou_name", "") or ""),
                ),
            )
            gemma_fbd_pou_name = str(ranked_gemma_rows[0].get("pou_name", "") or "")
            selected_gemma_pou_name = gemma_fbd_pou_name

            if gemma_fbd_pou_name:
                q_code = f"""
                SELECT ?code ?lang WHERE {{
                  ?pou rdf:type ag:class_POU ;
                       dp:hasPOUName "{gemma_fbd_pou_name}" ;
                       dp:hasPOUCode ?code .
                  OPTIONAL {{ ?pou dp:hasPOULanguage ?lang . }}
                }}
                """
                rows = sparql_select_raw(q_code, max_rows=3)
                if rows:
                    fbd_code = rows[0].get("code", "") or ""
                    gemma_fbd_logic = {
                        "pou_name": gemma_fbd_pou_name,
                        "lang": rows[0].get("lang", ""),
                        "d2_logic": self._extract_d2_logic_from_fbd_code(fbd_code),
                    }

        # GEMMA-State-Hint (Snapshot) nutzen, um OR-Branch einzugrenzen (one-hot Annahme)
        set_expr = ""
        if isinstance(gemma_fbd_logic, dict):
            d2_logic = gemma_fbd_logic.get("d2_logic")
            if isinstance(d2_logic, dict):
                set_expr = str(d2_logic.get("set_condition") or "")
        branch_hint = self._pick_or_branch(set_expr, last_gemma_state)
        inferred_driver = ""
        if branch_hint.get("picked") and last_gemma_state:
            picked = str(branch_hint.get("picked") or "")
            for alias in EvD2DiagnosisTool._gemma_state_aliases(last_gemma_state):
                m = re.search(
                    rf"\\b([A-Za-z_][A-Za-z0-9_.]*)\\b\\s+UND\\s+\\b{re.escape(alias)}\\b",
                    picked,
                )
                if m:
                    inferred_driver = m.group(1)
                    break

        executed_gemma_path = []
        if last_gemma_state:
            executed_gemma_path.append(
                {
                    "from": last_gemma_state,
                    "to": "D2",
                    "type": "stable_state_to_error_state",
                    "assumption": "LastGEMMAStateBeforeFailure ist der letzte stabile Zustand; D2 ist der Fehlerzustand (evD2).",
                    "evidence": {
                        "last_gemma_state_before_failure": last_gemma_state,
                        "picked_set_or_branch": branch_hint.get("picked") or "",
                        "inferred_driver_signal": inferred_driver,
                    },
                }
            )

        plan_steps = [
            {
                "step": 1,
                "title": "Trigger verstehen",
                "do": [
                    f"Prüfe im PLC Snapshot, dass {trigger_var} TRUE ist (das löst {event_name} aus).",
                    "Identifiziere lastSkill / lastExecutedSkill aus dem Snapshot.",
                    "Wenn verfügbar: nutze LastGEMMAStateBeforeFailure als Hinweis, welcher GEMMA-Zweig aktiv war (one-hot).",
                ],
            },
            {
                "step": 2,
                "title": "GEMMA State Machine + D2 Port finden",
                "do": [
                    "Finde POU(s) mit dp:isGEMMAStateMachine true.",
                    f"Finde Output-Port(s), deren Name '{port_name_contains}' enthält (z.B. D2).",
                ],
            },
            {
                "step": 3,
                "title": "Call-Chain D2 -> MAIN",
                "do": [
                    "Ermittle PortInst -> FBInst -> VarInst -> CallerPOU.",
                    "Prüfe im MAIN Code die Verdrahtung (z.B. fbDiag(Diagnose_gefordert := fbBA.D2)).",
                ],
            },
            {
                "step": 4,
                "title": "Warum wurde D2 gesetzt",
                "do": [
                    "Extrahiere im GEMMA-FBD die Set/Reset Bedingungen des RS für D2.",
                    "Suche, wo die Einflussgrößen (Auto_Stoerung, NotStopp, DiagnoseRequested, Alt_abort, ...) gesetzt werden.",
                ],
            },
            {
                "step": 5,
                "title": "Bezug zu lastSkill",
                "do": [
                    "Finde POUs, die lastExecutedSkill auf den lastSkill String setzen.",
                    "Mappe den String über FBType -> FBInstanz -> CallerPOU (z.B. fbAuto).",
                    "Prüfe, ob dieselben Caller auch den GEMMA State beeinflussen (z.B. Auto_Stoerung).",
                ],
            },
        ]

        return {
            "event": event_name,
            "trigger": {"var": trigger_var, "explanation": "Wenn TriggerD2 TRUE wird, wird evD2 ausgelöst."},
            "last_skill": last_skill,
            "last_gemma_state_before_failure": last_gemma_state,
            "gemma_assumption": "Im GEMMA ist typischerweise genau 1 Zustand gleichzeitig aktiv (one-hot).",
            "gemma_architecture_note": (
                "Das GEMMA-Layer ist die Hauptarchitektur/Steuerungslogik: der aktive Zustand bestimmt, "
                "welche Zweige/Logikpfade im Programm wirksam sind. D2 ist der Fehlerzustand."
            ),
            "gemma_state_machines": gemma_rows,
            "d2_output_ports": d2_ports,
            "d2_call_chain": call_chain,
            "main_wiring": self._extract_main_lines(main_code),
            "trigger_setters": trigger_setters,
            "skill_setters": skill_setters,
            "skill_instances": skill_instances,
            "gemma_d2_logic": gemma_fbd_logic,
            "gemma_d2_set_branch_hint": branch_hint,
            "inferred_d2_driver_signal": inferred_driver,
            "executed_gemma_path_hint": executed_gemma_path,
            "d2_callers": d2_callers,
            "skill_setter_pous": skill_setter_names,
            "intersection_d2callers_and_skillsetters": overlap,
            "selected_gemma_pou_name": selected_gemma_pou_name,
            "scope_hints": scope_hints,
            "diagnose_plan": plan_steps,
        }
class EvD2UnifiedTraceTool(BaseAgentTool):
    name = "evd2_unified_trace"
    description = (
        "Deterministische Tiefenanalyse (Zelle-3 Logik): verfolgt die dominante TRUE-Bedingung "
        "bis in Call-/Port-/Variable-Pfade mit ausführlichem Trace-Log."
    )
    usage_guide = (
        "Nutzen, wenn du für D2/evD2 die konkrete Kette der Bedingungen sehen willst "
        "(inkl. IF-Pfade, Instanzports, Timer/Trigger-Parameter, Loops, Defaults). "
        "target_var leer lassen für Auto-Ziel (typisch: Stoerung_erkannt)."
    )

    def run(
        self,
        last_gemma_state: str = "",
        state_name: str = "D2",
        target_var: str = "",
        max_depth: int = 12,
        trace_each_truth_path: bool = True,
        assumed_false_states: str = "D1,D2,D3",
        verbose_trace: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        if "g" not in globals():
            return {"error": "Global graph 'g' nicht gesetzt. build_bot(...) zuerst aufrufen."}
        graph = globals()["g"]
        if not isinstance(graph, Graph):
            return {"error": "Global 'g' ist kein rdflib.Graph."}
        if isinstance(assumed_false_states, (list, tuple, set)):
            assumed = [str(x).strip() for x in assumed_false_states if str(x).strip()]
        else:
            assumed = [x.strip() for x in str(assumed_false_states or "").split(",") if x.strip()]
        return run_unified_set_and_condition_trace(
            graph,
            last_gemma_state=last_gemma_state,
            state_name=state_name,
            target_var=target_var,
            max_depth=int(max_depth),
            trace_each_truth_path=bool(trace_each_truth_path),
            assumed_false_states_in_betriebsarten=assumed,
            verbose_trace=bool(verbose_trace),
        )


class EvD2RequirementPathsTool(BaseAgentTool):
    name = "evd2_requirement_paths"
    description = (
        "Tabellarische Pfad-Sicht (Zelle-4 Logik): gibt pro Pfad die notwendigen "
        "Port/Variablen-Werte inkl. POU, Typ und Herleitung aus."
    )
    usage_guide = (
        "Nutzen, wenn du aus der Detailanalyse eine kompakte Tabelle pro Pfad brauchst "
        "(Spalten: Port/Variable, Wert, POU, Herleitung). "
        "target_var leer lassen für Auto-Ziel (typisch: Stoerung_erkannt)."
    )

    def run(
        self,
        last_gemma_state: str = "",
        state_name: str = "D2",
        target_var: str = "",
        max_depth: int = 12,
        trace_each_truth_path: bool = True,
        assumed_false_states: str = "D1,D2,D3",
        verbose_trace: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        if "g" not in globals():
            return {"error": "Global graph 'g' nicht gesetzt. build_bot(...) zuerst aufrufen."}
        graph = globals()["g"]
        if not isinstance(graph, Graph):
            return {"error": "Global 'g' ist kein rdflib.Graph."}
        if isinstance(assumed_false_states, (list, tuple, set)):
            assumed = [str(x).strip() for x in assumed_false_states if str(x).strip()]
        else:
            assumed = [x.strip() for x in str(assumed_false_states or "").split(",") if x.strip()]
        analysis = run_unified_set_and_condition_trace(
            graph,
            last_gemma_state=last_gemma_state,
            state_name=state_name,
            target_var=target_var,
            max_depth=int(max_depth),
            trace_each_truth_path=bool(trace_each_truth_path),
            assumed_false_states_in_betriebsarten=assumed,
            verbose_trace=bool(verbose_trace),
        )
        return build_requirement_tables(analysis)


class EvD2PathTracingTool(BaseAgentTool):
    name = "evd2_path_trace"
    description = (
        "Komplette evD2-Hauptanalyse: Trigger-Setter, Skill-Kontext und "
        "zeitlich plausibles State-Path-Matching (inkl. Skill-Case TestSkill3)."
    )
    usage_guide = (
        "Nutzen als Haupttool für evD2. Liefert Punkt 1/2/3 der Analyse in einer "
        "deterministischen JSON-Struktur inkl. ausgewähltem State-Path."
    )

    def __init__(self, plc_snapshot: Any = None):
        self.snapshot = _unwrap_plc_snapshot(plc_snapshot)
        self.snapshot_index = SnapshotEvidenceIndex.build_from_snapshot(self.snapshot)

    @staticmethod
    def _sparql_escape(text: str) -> str:
        return str(text or "").replace("\\", "\\\\").replace('"', '\\"')

    @staticmethod
    def _normalize_skill_literal(rhs: str) -> str:
        s = str(rhs or "").strip()
        if len(s) >= 2 and ((s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"'))):
            s = s[1:-1]
        return s.strip()

    @staticmethod
    def _condition_entry_to_expr(entry: str) -> Optional[str]:
        e = str(entry or "").strip()
        if not e:
            return None
        m_if = re.match(r"^(?:IF|ELSIF)@\d+\s*:\s*(.+)$", e, flags=re.I)
        if m_if:
            return m_if.group(1).strip()
        m_else = re.match(r"^ELSE@\d+\s*\(zu IF:\s*(.+)\)$", e, flags=re.I)
        if m_else:
            return f"NOT ({m_else.group(1).strip()})"
        return None

    @staticmethod
    def _conditions_to_expr(conditions: List[str]) -> str:
        parts: List[str] = []
        for c in conditions or []:
            expr = EvD2PathTracingTool._condition_entry_to_expr(c)
            if expr:
                parts.append(f"({expr})")
        return " AND ".join(parts)

    @staticmethod
    def _split_top_level_csv(text: str) -> List[str]:
        out: List[str] = []
        cur: List[str] = []
        depth = 0
        for ch in str(text or ""):
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

    @staticmethod
    def _find_call_param_expr(st_code: str, instance_name: str, param_name: str) -> str:
        if not st_code or not instance_name or not param_name:
            return ""
        pat = re.compile(rf"{re.escape(instance_name)}\s*\((.*?)\)\s*;", flags=re.I | re.S)
        m = pat.search(st_code)
        if not m:
            return ""
        inside = m.group(1)
        for part in EvD2PathTracingTool._split_top_level_csv(inside):
            mm = re.match(rf"^\s*{re.escape(param_name)}\s*:=\s*(.+?)\s*$", part, flags=re.I)
            if mm:
                return mm.group(1).strip()
        return ""

    @staticmethod
    def _req_key(symbol: str, phase: str, value: str) -> Tuple[str, str, str]:
        return (str(symbol or "").strip(), str(phase or "").strip(), str(value or "").strip().upper())

    @staticmethod
    def _add_requirement(
        reqs: List[Dict[str, str]],
        *,
        symbol: str,
        value: str,
        phase: str,
        reason: str,
    ) -> None:
        if not symbol:
            return
        key = EvD2PathTracingTool._req_key(symbol, phase, value)
        for r in reqs:
            if EvD2PathTracingTool._req_key(r.get("symbol", ""), r.get("phase", ""), r.get("value", "")) == key:
                return
        reqs.append({"symbol": symbol, "value": str(value).upper(), "phase": phase, "reason": reason})

    @staticmethod
    def _path_to_clauses(path: List[Tuple[str, bool]]) -> List[str]:
        out: List[str] = []
        for token, req in path:
            t = str(token or "").strip()
            if not t:
                continue
            out.append(t if req else f"NOT {t}")
        return out

    @staticmethod
    def _extract_if_expr_from_snippet(snippet: str) -> str:
        m = re.search(r"\bIF\s+(.*?)\s+THEN\b", str(snippet or ""), flags=re.I | re.S)
        return m.group(1).strip() if m else ""

    def _resolve_skill_identity(self, skill_name: str) -> Dict[str, Any]:
        skill = str(skill_name or "").strip()
        if not skill:
            return {"skill_name": "", "as_class_skill": False, "class_skill_iris": [], "interpretation": "Kein lastSkill im Event."}

        esc = self._sparql_escape(skill)
        esc_rx = re.escape(skill)

        q_cls = f"""
        SELECT ?s WHERE {{
          ?s rdf:type ag:class_Skill .
          FILTER(REGEX(STR(?s), "(^|[#/]){esc_rx}$"))
        }}
        """
        cls_rows = sparql_select_raw(q_cls, max_rows=20)

        q_any = f"""
        SELECT ?s ?type WHERE {{
          ?s ?p ?o .
          FILTER(REGEX(STR(?s), "(^|[#/]){esc_rx}$"))
          OPTIONAL {{ ?s rdf:type ?type . }}
        }}
        """
        any_rows = sparql_select_raw(q_any, max_rows=50)

        as_class_skill = len(cls_rows) > 0
        if as_class_skill:
            interpretation = "Skill-IRI als ag:class_Skill im KG vorhanden."
        else:
            interpretation = (
                "Kein ag:class_Skill-Treffer für diesen Namen im aktuellen Steuerungs-KG; "
                "der Name tritt typischerweise als Stringliteral in OPCUA.lastExecutedSkill-Zuweisungen auf."
            )

        return {
            "skill_name": skill,
            "as_class_skill": as_class_skill,
            "class_skill_iris": [r.get("s", "") for r in cls_rows if isinstance(r, dict)],
            "entity_hits": any_rows,
            "interpretation": interpretation,
        }

    def _build_skill_trace(
        self,
        *,
        last_skill: str,
        preferred_pous: List[str],
        process_name: str,
    ) -> Dict[str, Any]:
        skill = str(last_skill or "").strip()
        if not skill:
            return {"error": "last_skill ist leer."}

        esc_skill = self._sparql_escape(skill)
        q = f"""
        SELECT ?pou ?pou_name ?code WHERE {{
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName ?pou_name ;
               dp:hasPOUCode ?code .
          FILTER(CONTAINS(STR(?code), "OPCUA.lastExecutedSkill"))
          FILTER(CONTAINS(STR(?code), "'{esc_skill}'"))
        }}
        ORDER BY ?pou_name
        """
        rows = sparql_select_raw(q, max_rows=30)
        if not rows:
            return {"error": f"Kein Skill-Setter für '{skill}' gefunden."}

        pref = {str(x) for x in (preferred_pous or []) if str(x)}
        proc = str(process_name or "").strip()

        def score(row: Dict[str, Any]) -> Tuple[int, str]:
            code = str(row.get("code", "") or "")
            name = str(row.get("pou_name", "") or "")
            s = 0
            if name in pref:
                s += 4
            if re.search(r"\b([A-Za-z_][A-Za-z0-9_\.]*)\s*:=\s*NOT\s+\1\s*;", code):
                s += 3
            if re.search(r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(\s*CLK\s*:=", code):
                s += 2
            if proc and proc in code:
                s += 1
            return (s, name)

        chosen_row = sorted(rows, key=score, reverse=True)[0]
        pou_name = str(chosen_row.get("pou_name", "") or "")
        pou_uri = str(chosen_row.get("pou", "") or "")
        code = str(chosen_row.get("code", "") or "")

        assigns = extract_assignments_st(code, "OPCUA.lastExecutedSkill", trace=None)
        target_assigns = [a for a in assigns if self._normalize_skill_literal(a.get("rhs", "")).lower() == skill.lower()]
        if not target_assigns:
            return {"error": f"POU '{pou_name}' enthält '{skill}', aber keine passende Zuweisung gefunden."}
        chosen = target_assigns[-1]

        expr = self._conditions_to_expr(chosen.get("conditions", []))
        truth_paths = condition_to_truth_paths(expr) if expr else []
        clauses_first = self._path_to_clauses(truth_paths[0]) if truth_paths else []

        requirements: List[Dict[str, str]] = []
        req_map_t1: Dict[str, str] = {}
        for token, req_true in (truth_paths[0] if truth_paths else []):
            value = "TRUE" if req_true else "FALSE"
            self._add_requirement(
                requirements,
                symbol=str(token),
                value=value,
                phase="t-1",
                reason=f"Skill-Klausel: {token if req_true else f'NOT {token}'}",
            )
            req_map_t1[str(token)] = value

        toggle_hints: List[Dict[str, Any]] = []
        rx_toggle = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_\.]*)\s*:=\s*NOT\s+\1\s*;\s*$", flags=re.I)
        chosen_line_no = _safe_int(chosen.get("line_no", 0), 0)
        for idx, ln in enumerate(code.splitlines(), start=1):
            if idx >= chosen_line_no:
                break
            m = rx_toggle.match(ln.strip())
            if not m:
                continue
            var_name = str(m.group(1) or "").strip()
            if var_name not in req_map_t1:
                continue
            post = req_map_t1.get(var_name, "TRUE")
            pre = "FALSE" if post == "TRUE" else "TRUE"
            toggle_hints.append(
                {
                    "line_no": idx,
                    "var": var_name,
                    "pre": pre,
                    "post": post,
                    "assignment": f"{var_name} := NOT {var_name};",
                }
            )
            self._add_requirement(
                requirements,
                symbol=var_name,
                value=pre,
                phase="t-2",
                reason=f"Vor Toggle ({var_name} := NOT {var_name})",
            )
            self._add_requirement(
                requirements,
                symbol=var_name,
                value=post,
                phase="t-1",
                reason=f"Nach Toggle ({var_name} im Skill-Pfad)",
            )

        q_output_rx = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\.Q$")
        for symbol, req_value in list(req_map_t1.items()):
            if str(req_value).upper() != "TRUE":
                continue
            m_q = q_output_rx.match(str(symbol))
            if not m_q:
                continue
            inst = str(m_q.group(1))
            driver_expr = self._find_call_param_expr(code, inst, "CLK")
            driver_param = "CLK"
            if not driver_expr:
                driver_expr = self._find_call_param_expr(code, inst, "IN")
                driver_param = "IN"
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_\.]*", driver_expr or ""):
                continue
            self._add_requirement(
                requirements,
                symbol=driver_expr,
                value="TRUE",
                phase="t-1",
                reason=f"{inst}.Q-Heuristik: {driver_param}=TRUE",
            )
            req_map_t1[str(driver_expr)] = "TRUE"

        overwrite_blockers: List[str] = []
        later_assigns = [
            a
            for a in assigns
            if _safe_int(a.get("line_no", 0), 0) > chosen_line_no
            and self._normalize_skill_literal(a.get("rhs", "")).lower() != skill.lower()
        ]
        for la in later_assigns:
            la_expr = self._conditions_to_expr(la.get("conditions", []))
            if not la_expr:
                continue
            la_paths = condition_to_truth_paths(la_expr)
            for lp in la_paths:
                if not isinstance(lp, list) or not lp:
                    continue
                path_already_blocked = False
                unconstrained_literals: List[Tuple[str, bool]] = []
                for tok, req_true in lp:
                    token = str(tok or "").strip()
                    if not token:
                        continue
                    path_val = "TRUE" if bool(req_true) else "FALSE"
                    existing = req_map_t1.get(token)
                    if existing is None:
                        unconstrained_literals.append((token, bool(req_true)))
                    elif str(existing).upper() != path_val:
                        path_already_blocked = True
                        break

                if path_already_blocked:
                    overwrite_blockers.append(f"Path already blocked: NOT ({' AND '.join(self._path_to_clauses(lp))})")
                    continue
                if not unconstrained_literals:
                    overwrite_blockers.append(f"Unresolved overwrite risk: {la_expr}")
                    continue

                def _blocker_rank(item: Tuple[str, bool]) -> Tuple[int, int, int, str]:
                    token, req_true = item
                    t = str(token or "").strip()
                    # Dynamic priority:
                    # 1) Prefer blocking positive literals (X) via X=FALSE over negated literals (NOT X) via X=TRUE.
                    # 2) Prefer event-like outputs (*.Q), then other dotted tokens, then plain tokens.
                    # 3) Prefer shorter token names as weak tie-breaker.
                    pos_prio = 0 if req_true else 1
                    if t.endswith(".Q"):
                        kind_prio = 0
                    elif "." in t:
                        kind_prio = 1
                    else:
                        kind_prio = 2
                    return (pos_prio, kind_prio, len(t), t)

                blocker_token, blocker_req_true = sorted(unconstrained_literals, key=_blocker_rank)[0]
                blocker_value = "FALSE" if blocker_req_true else "TRUE"
                self._add_requirement(
                    requirements,
                    symbol=blocker_token,
                    value=blocker_value,
                    phase="t-1",
                    reason=f"Blockiert spätere Skill-Überschreibung: NOT ({la_expr})",
                )
                req_map_t1[blocker_token] = blocker_value
                blocker_clause = blocker_token if blocker_value == "TRUE" else f"NOT {blocker_token}"
                overwrite_blockers.append(blocker_clause)
                break

        reqs_sorted = sorted(
            requirements,
            key=lambda r: (0 if r.get("phase") == "t-1" else 1 if r.get("phase") == "t-2" else 2, r.get("symbol", "")),
        )
        expr_parts: List[str] = []
        for r in reqs_sorted:
            if r.get("phase") != "t-1":
                continue
            sym = str(r.get("symbol", "")).strip()
            if not sym:
                continue
            expr_parts.append(sym if str(r.get("value", "")).upper() == "TRUE" else f"NOT {sym}")
        skill_expr = " AND ".join(expr_parts)

        return {
            "setter_pou_name": pou_name,
            "setter_pou_uri": pou_uri,
            "dominant_assignment": chosen,
            "condition_expr": expr,
            "condition_paths": [self._path_to_clauses(p) for p in truth_paths],
            "skill_expr": skill_expr,
            "requirements": reqs_sorted,
            "toggle_hint": toggle_hints[0] if toggle_hints else {},
            "toggle_hints": toggle_hints,
            "overwrite_blockers": overwrite_blockers,
        }

    @staticmethod
    def _extract_hw_addresses_from_req_rows(rows: List[Dict[str, Any]]) -> List[str]:
        if not isinstance(rows, list):
            return []
        out: List[str] = []
        seen: Set[str] = set()
        for r in rows:
            if not isinstance(r, dict):
                continue
            text = " | ".join(
                [
                    str(r.get("Port/Variable", "") or ""),
                    str(r.get("Herleitung", "") or ""),
                    str(r.get("Assignment", "") or ""),
                ]
            )
            for m in re.finditer(r"hardware_address['\"]?\s*:\s*['\"]([^'\"]+)['\"]", text, flags=re.I):
                hw = str(m.group(1) or "").strip()
                if hw and hw not in seen:
                    seen.add(hw)
                    out.append(hw)
            for m in re.finditer(r"(%[IQM][XWDB]?[0-9\.\[\]]+)", text, flags=re.I):
                hw = str(m.group(1) or "").strip()
                if hw and hw not in seen:
                    seen.add(hw)
                    out.append(hw)
        return out

    def _score_state_paths(
        self,
        req: Dict[str, Any],
        skill_trace: Dict[str, Any],
        snapshot_index: Optional[SnapshotEvidenceIndex] = None,
    ) -> Dict[str, Any]:
        paths = req.get("paths") if isinstance(req.get("paths"), list) else []
        skill_reqs = skill_trace.get("requirements") if isinstance(skill_trace.get("requirements"), list) else []

        req_t1: Dict[str, str] = {}
        for r in skill_reqs:
            if not isinstance(r, dict):
                continue
            if str(r.get("phase", "")) != "t-1":
                continue
            b = _to_bool_str(r.get("value", ""))
            if not b:
                continue
            req_t1[str(r.get("symbol", ""))] = b

        evaluations: List[Dict[str, Any]] = []
        for p in paths:
            if not isinstance(p, dict):
                continue
            path_id = str(p.get("path_id", ""))
            path_expr = str(p.get("path_expr", ""))
            path_rows = p.get("rows") if isinstance(p.get("rows"), list) else []
            hardware_addresses = self._extract_hw_addresses_from_req_rows(path_rows)
            has_default_terminator = any(
                isinstance(r, dict)
                and (
                    "defaultvalue" in str(r.get("Herleitung", "")).lower()
                    or str(r.get("Quelle", "")).lower() == "default"
                )
                for r in path_rows
            )
            dnf = condition_to_truth_paths(path_expr)
            lit_map: Dict[str, str] = {}
            for tok, req_true in (dnf[0] if dnf else []):
                lit_map[str(tok)] = "TRUE" if req_true else "FALSE"

            mutual_exclusion_map: Dict[str, str] = {}
            token_bool_sets: Dict[str, Set[str]] = {}
            for row in path_rows:
                if not isinstance(row, dict):
                    continue
                token = str(row.get("Port/Variable", "")).strip()
                if not token:
                    continue
                row_val = _to_bool_str(row.get("Wert", ""))
                if not row_val:
                    continue
                reason_l = str(row.get("Herleitung", "")).lower()
                if "max_depth" in reason_l or "loop detected" in reason_l:
                    continue
                token_bool_sets.setdefault(token, set()).add(row_val)
                if "mutual exclusion" in reason_l:
                    mutual_exclusion_map[token] = row_val

            strong_row_map: Dict[str, str] = {}
            for token, vals in token_bool_sets.items():
                if len(vals) == 1:
                    strong_row_map[token] = list(vals)[0]

            conflicts: List[str] = []
            static_score = 0
            exact = 0
            soft = 0
            unresolved = 0

            for token, req_val in req_t1.items():
                t = str(token or "").strip()
                if not t:
                    continue
                path_val = lit_map.get(t)
                if path_val in {"TRUE", "FALSE"}:
                    if path_val == req_val:
                        exact += 1
                        static_score += 2
                    else:
                        conflicts.append(f"{t}: skill@t-1={req_val}, state@t0={path_val}")
                    continue

                me_val = mutual_exclusion_map.get(t)
                if me_val in {"TRUE", "FALSE"}:
                    if me_val == req_val:
                        soft += 1
                        static_score += 1
                    else:
                        conflicts.append(f"{t}: skill@t-1={req_val}, state@t0(mutual_exclusion)={me_val}")
                    continue

                strong_val = strong_row_map.get(t)
                if strong_val in {"TRUE", "FALSE"}:
                    if strong_val == req_val:
                        soft += 1
                        static_score += 1
                    else:
                        conflicts.append(f"{t}: skill@t-1={req_val}, state(path_rows)={strong_val}")
                    continue

                unresolved += 1

            snapshot_matches: List[Dict[str, Any]] = []
            snapshot_conflicts: List[str] = []
            snapshot_exact = 0
            snapshot_soft = 0
            snapshot_unresolved = 0
            snapshot_score = 0
            snapshot_seen: Set[str] = set()

            def _apply_snapshot_evidence(token: str, expected: str, *, via: str, weight: int) -> None:
                nonlocal snapshot_exact, snapshot_soft, snapshot_unresolved, snapshot_score
                t = str(token or "").strip()
                exp = str(expected or "").strip().upper()
                if not t or exp not in {"TRUE", "FALSE"} or t in snapshot_seen:
                    return
                snapshot_seen.add(t)

                hit = snapshot_index.best_hit(t, require_boolean=True) if isinstance(snapshot_index, SnapshotEvidenceIndex) else None
                if not isinstance(hit, SnapshotEvidenceHit):
                    snapshot_unresolved += 1
                    return

                evidence = {
                    "token": t,
                    "expected": exp,
                    "observed": hit.value_bool,
                    "snapshot_id": hit.matched_id,
                    "snapshot_symbol": hit.normalized_id,
                    "source": hit.source,
                    "alias_kind": hit.alias_kind,
                    "via": via,
                }
                if hit.value_bool == exp:
                    if via == "literal":
                        snapshot_exact += 1
                    else:
                        snapshot_soft += 1
                    snapshot_score += weight
                    snapshot_matches.append(evidence)
                    return

                snapshot_score -= weight + (2 if via == "literal" else 1)
                snapshot_conflicts.append(
                    f"{t}: snapshot={hit.value_bool}, {via}@t0={exp} ({hit.matched_id})"
                )

            if isinstance(snapshot_index, SnapshotEvidenceIndex) and snapshot_index.has_data():
                for token, req_val in lit_map.items():
                    _apply_snapshot_evidence(token, req_val, via="literal", weight=6)
                for token, req_val in strong_row_map.items():
                    if token in lit_map:
                        continue
                    _apply_snapshot_evidence(token, req_val, via="row", weight=3)

            possible = len(conflicts) == 0
            total_score = static_score - 4 * len(conflicts) + snapshot_score
            evaluations.append(
                {
                    "state_path": path_id,
                    "path_expr": path_expr,
                    "possible": possible,
                    "runtime_supported": len(snapshot_conflicts) == 0 and (snapshot_exact + snapshot_soft) > 0,
                    "score": total_score,
                    "static_score": static_score - 4 * len(conflicts),
                    "snapshot_evidence_score": snapshot_score,
                    "conflicts": conflicts,
                    "snapshot_conflicts": snapshot_conflicts,
                    "snapshot_matches": snapshot_matches,
                    "literal_map_t0": lit_map,
                    "mutual_exclusion_map_t0": mutual_exclusion_map,
                    "row_bool_map_t0": strong_row_map,
                    "exact": exact,
                    "soft": soft,
                    "unresolved": unresolved,
                    "snapshot_exact": snapshot_exact,
                    "snapshot_soft": snapshot_soft,
                    "snapshot_unresolved": snapshot_unresolved,
                    "hardware_addresses": hardware_addresses,
                    "has_hardware_address": len(hardware_addresses) > 0,
                    "has_default_terminator": has_default_terminator,
                }
            )

        evaluations.sort(
            key=lambda e: (
                not e.get("possible", False),
                -_safe_int(e.get("score", 0), 0),
                str(e.get("state_path", "")),
            )
        )
        possible_paths = [e["state_path"] for e in evaluations if e.get("possible")]
        best_paths: List[str] = []
        if possible_paths:
            best_score = max(_safe_int(e.get("score", 0), 0) for e in evaluations if e.get("possible"))
            best_paths = [
                e["state_path"]
                for e in evaluations
                if e.get("possible") and _safe_int(e.get("score", 0), 0) == best_score
            ]

        return {
            "skill_requirements": skill_reqs,
            "snapshot_available": isinstance(snapshot_index, SnapshotEvidenceIndex) and snapshot_index.has_data(),
            "path_evaluations": evaluations,
            "possible_paths": possible_paths,
            "best_paths": best_paths,
        }

    @staticmethod
    def _select_trigger_setter(core: Dict[str, Any], trigger_var: str) -> Dict[str, Any]:
        setters = core.get("trigger_setters") if isinstance(core.get("trigger_setters"), list) else []
        if not setters:
            return {}
        preferred_pou_names = (
            core.get("preferred_trigger_pou_names")
            if isinstance(core.get("preferred_trigger_pou_names"), list)
            else []
        )
        scope_hints = core.get("scope_hints") if isinstance(core.get("scope_hints"), list) else []
        ranked: List[Tuple[int, int, Dict[str, Any]]] = []
        for idx, setter in enumerate(setters):
            if not isinstance(setter, dict):
                continue
            score = 0
            if isinstance(setter.get("snips_TRUE"), list) and setter.get("snips_TRUE"):
                score += 50
            score += _score_name_with_scope(
                str(setter.get("pou_name", "") or ""),
                preferred_names=preferred_pou_names,
                scope_hints=scope_hints,
            )
            ranked.append((score, idx, setter))
        if not ranked:
            return {}
        ranked.sort(key=lambda item: (-item[0], item[1]))
        chosen = ranked[0][2]
        snip_obj = (chosen.get("snips_TRUE") or [{}])[0] if isinstance(chosen, dict) else {}
        snippet = str(snip_obj.get("snippet", "") or "")
        condition = EvD2PathTracingTool._extract_if_expr_from_snippet(snippet)
        return {
            "trigger_var": trigger_var,
            "pou_name": chosen.get("pou_name", "") if isinstance(chosen, dict) else "",
            "snippet_true": snippet,
            "condition_expr": condition,
        }

    @staticmethod
    def _compact_trigger_condition_trace(trace_data: Dict[str, Any], max_rows_per_path: int = 80) -> Dict[str, Any]:
        if not isinstance(trace_data, dict):
            return {}
        if trace_data.get("error"):
            return {"error": trace_data.get("error")}

        out_paths: List[Dict[str, Any]] = []
        for p in trace_data.get("paths", []) or []:
            if not isinstance(p, dict):
                continue
            compact_rows: List[Dict[str, Any]] = []
            seen_row: Set[Tuple[str, str, str, str]] = set()
            for r in (p.get("rows") or [])[:max_rows_per_path]:
                if not isinstance(r, dict):
                    continue
                val = str(r.get("Wert", "")).strip()
                tok = str(r.get("Port/Variable", "")).strip()
                if not tok:
                    continue
                if not val:
                    continue
                assignment = str(r.get("Assignment", "") or "").strip()
                row_key = (tok, val, str(r.get("POU", "")), assignment)
                if row_key in seen_row:
                    continue
                seen_row.add(row_key)
                compact_rows.append(
                    {
                        "token": tok,
                        "value": val,
                        "pou": r.get("POU", ""),
                        "assignment": assignment,
                        "fb_type": r.get("FBType", ""),
                        "fb_type_description": r.get("FBTypeDescription", ""),
                        "kind": r.get("Typ", ""),
                        "reason": r.get("Herleitung", ""),
                        "source": r.get("Quelle", ""),
                        "depth": r.get("Tiefe", ""),
                    }
                )
            compact_rows.sort(
                key=lambda x: (
                    _safe_int(x.get("depth", 9999), 9999),
                    str(x.get("token", "")),
                )
            )
            out_paths.append(
                {
                    "path_id": str(p.get("path_id", "")),
                    "path_expr": str(p.get("path_expr", "")),
                    "rows": compact_rows,
                }
            )

        return {
            "context": trace_data.get("context", {}),
            "summary": trace_data.get("summary", {}),
            "paths": out_paths,
            "trace_log": trace_data.get("trace_log", []),
        }

    @staticmethod
    def _compact_state_path(
        req: Dict[str, Any],
        path_id: str,
        state_fit: Optional[Dict[str, Any]] = None,
        skill_trace: Optional[Dict[str, Any]] = None,
        max_rows: int = 14,
    ) -> List[Dict[str, Any]]:
        def _to_boolish(v: Any) -> Optional[bool]:
            s = str(v or "").strip().upper()
            has_true = "TRUE" in s
            has_false = "FALSE" in s
            if has_true and not has_false:
                return True
            if has_false and not has_true:
                return False
            return None

        def _phase_rank(phase: str) -> int:
            p = str(phase or "").strip().lower()
            if p == "t0":
                return 0
            m = re.match(r"t-(\d+)$", p)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return 999
            return 999

        def _eval_simple_expr(expr: str, constraints: Dict[str, str]) -> Optional[bool]:
            text = str(expr or "").strip()
            if not text:
                return None
            text = text.strip("()").strip()
            up = text.upper()
            if up == "TRUE":
                return True
            if up == "FALSE":
                return False
            m_not = re.match(r"(?i)^NOT\s+([A-Za-z_][A-Za-z0-9_.]*)$", text)
            if m_not:
                base = m_not.group(1)
                b = _to_boolish(constraints.get(base))
                return (not b) if b is not None else None
            if re.match(r"^[A-Za-z_][A-Za-z0-9_.]*$", text):
                return _to_boolish(constraints.get(text))
            return None

        paths = req.get("paths") if isinstance(req.get("paths"), list) else []
        selected_eval: Dict[str, Any] = {}
        if isinstance(state_fit, dict):
            for ev in (state_fit.get("path_evaluations") if isinstance(state_fit.get("path_evaluations"), list) else []):
                if not isinstance(ev, dict):
                    continue
                if str(ev.get("state_path", "")) == str(path_id):
                    selected_eval = ev
                    break

        phase_constraints: Dict[str, Dict[str, str]] = {"t0": {}}
        lit_map_t0 = selected_eval.get("literal_map_t0") if isinstance(selected_eval.get("literal_map_t0"), dict) else {}
        for token, val in lit_map_t0.items():
            t = str(token or "").strip()
            vv = str(val or "").strip().upper()
            if not t or not vv:
                continue
            phase_constraints.setdefault("t0", {})[t] = vv

        symbol_phase: Dict[str, str] = {}
        if isinstance(skill_trace, dict):
            reqs = skill_trace.get("requirements") if isinstance(skill_trace.get("requirements"), list) else []
            for r in reqs:
                if not isinstance(r, dict):
                    continue
                sym = str(r.get("symbol", "")).strip()
                val = str(r.get("value", "")).strip().upper()
                ph = str(r.get("phase", "")).strip()
                if not sym or not val or not ph:
                    continue
                phase_constraints.setdefault(ph, {})[sym] = val
                prev = symbol_phase.get(sym)
                if prev is None or _phase_rank(ph) <= _phase_rank(prev):
                    symbol_phase[sym] = ph

        for p in paths:
            if not isinstance(p, dict):
                continue
            if str(p.get("path_id", "")) != str(path_id):
                continue
            local_t0_constraints: Dict[str, str] = dict(phase_constraints.get("t0", {}))
            for rr in (p.get("rows") or []):
                if not isinstance(rr, dict):
                    continue
                tok0 = str(rr.get("Port/Variable", "")).strip()
                val0 = str(rr.get("Wert", "")).strip()
                reason0 = str(rr.get("Herleitung", "")).lower()
                b0 = _to_boolish(val0)
                if not tok0 or b0 is None:
                    continue
                if "mutual exclusion" in reason0 and tok0 not in local_t0_constraints:
                    local_t0_constraints[tok0] = "TRUE" if b0 else "FALSE"

            candidates: List[Dict[str, Any]] = []
            for r in (p.get("rows") or []):
                if not isinstance(r, dict):
                    continue
                val = str(r.get("Wert", "")).strip()
                if not val or val.lower() in {"unbekannt", "none", "-"}:
                    continue
                token = str(r.get("Port/Variable", "")).strip()
                if not token:
                    continue

                typ = str(r.get("Typ", "")).strip()
                reason = str(r.get("Herleitung", "")).strip()
                source = str(r.get("Quelle", "")).strip()
                depth = _safe_int(r.get("Tiefe", 9999), 9999)

                typ_l = typ.lower()
                reason_l = reason.lower()
                source_l = source.lower()
                skip_low_conf = (
                    typ_l == "stop"
                    or "max_depth" in reason_l
                    or "max_depth" in source_l
                    or "loop detected" in reason_l
                    or "loop detected" in source_l
                )
                if skip_low_conf:
                    continue

                score = 100
                score -= min(depth, 999) * 2
                if typ_l == "internal_variable":
                    score += 20
                elif typ_l == "job_method_assignment":
                    score += 26
                elif typ_l == "instance_port":
                    score += 18
                elif typ_l == "std_fb_dependency":
                    score += 14
                elif typ_l == "local_port":
                    score += 10
                elif typ_l == "job_method_input":
                    score -= 18
                elif typ_l == "terminal":
                    score -= 8
                if "abgelaufen" in val.lower():
                    score += 6
                assignment = str(r.get("Assignment", "")).strip()
                fb_type = str(r.get("FBType", "")).strip()
                fb_type_description = str(r.get("FBTypeDescription", "")).strip()
                ton_in_expr = ""
                ton_instance = ""
                if fb_type.upper() == "TON":
                    if "." in token:
                        ton_instance = token.split(".", 1)[0]
                    m_in = re.search(r"(?i)\bIN\s*:=\s*([^,)\r\n]+)", assignment)
                    ton_in_expr = str(m_in.group(1)).strip() if m_in else ""
                phase = symbol_phase.get(token) or ("t0" if token in phase_constraints.get("t0", {}) else "t0")
                token_constraints = local_t0_constraints if phase == "t0" else phase_constraints.get(phase, {})
                req_val = token_constraints.get(token)
                req_b = _to_boolish(req_val) if req_val is not None else None
                val_b = _to_boolish(val)
                if req_b is not None and val_b is not None and req_b != val_b:
                    continue

                if fb_type.upper() == "TON" and token.endswith(".Q") and val_b is True:
                    in_expr = ton_in_expr
                    in_val = _eval_simple_expr(in_expr, token_constraints)
                    if in_val is False:
                        continue

                if assignment:
                    score += 4
                if fb_type:
                    score += 3
                if source_l == "default":
                    score += 2

                candidates.append(
                    {
                        "token": token,
                        "value": val,
                        "pou": r.get("POU", ""),
                        "assignment": assignment,
                        "fb_type": fb_type,
                        "fb_type_description": fb_type_description,
                        "reason": reason,
                        "source": source,
                        "kind": typ,
                        "phase": phase,
                        "_ton_instance": ton_instance,
                        "_ton_in_expr": ton_in_expr,
                        "depth": depth,
                        "_score": score,
                    }
                )

            ton_in_by_instance: Dict[str, str] = {}
            for c in candidates:
                inst = str(c.get("_ton_instance", "")).strip()
                in_expr = str(c.get("_ton_in_expr", "")).strip()
                if inst and in_expr and inst not in ton_in_by_instance:
                    ton_in_by_instance[inst] = in_expr

            filtered_candidates: List[Dict[str, Any]] = []
            for c in candidates:
                token_c = str(c.get("token", "")).strip()
                fb_c = str(c.get("fb_type", "")).strip().upper()
                phase_c = str(c.get("phase", "t0")).strip()
                val_c = str(c.get("value", "")).strip()
                constraints_c = local_t0_constraints if phase_c == "t0" else phase_constraints.get(phase_c, {})
                if fb_c == "TON":
                    inst = str(c.get("_ton_instance", "")).strip()
                    if not inst and "." in token_c:
                        inst = token_c.split(".", 1)[0]
                    in_expr = str(c.get("_ton_in_expr", "")).strip() or ton_in_by_instance.get(inst, "")
                    q_req = _to_boolish(constraints_c.get(f"{inst}.Q")) if inst else None

                    if token_c.endswith(".PT") and "abgelaufen" in val_c.lower() and q_req is False:
                        continue

                    if (token_c.endswith(".Q") and _to_boolish(val_c) is True) or (
                        token_c.endswith(".PT") and "abgelaufen" in val_c.lower()
                    ):
                        in_val = _eval_simple_expr(in_expr, constraints_c)
                        if in_val is False:
                            continue

                filtered_candidates.append(c)
            candidates = filtered_candidates

            best_by_token: Dict[str, Dict[str, Any]] = {}
            for c in candidates:
                tok = str(c.get("token", ""))
                prev = best_by_token.get(tok)
                if prev is None:
                    best_by_token[tok] = c
                    continue
                if _safe_int(c.get("_score", 0), 0) > _safe_int(prev.get("_score", 0), 0):
                    best_by_token[tok] = c
                elif _safe_int(c.get("_score", 0), 0) == _safe_int(prev.get("_score", 0), 0):
                    if _safe_int(c.get("depth", 9999), 9999) < _safe_int(prev.get("depth", 9999), 9999):
                        best_by_token[tok] = c

            out = list(best_by_token.values())
            out.sort(
                key=lambda x: (
                    _phase_rank(str(x.get("phase", "t0"))),
                    _safe_int(x.get("depth", 9999), 9999),
                    -_safe_int(x.get("_score", 0), 0),
                    str(x.get("token", "")),
                )
            )
            compact: List[Dict[str, Any]] = []
            for row in out[:max_rows]:
                compact.append(
                    {
                        "token": row.get("token", ""),
                        "value": row.get("value", ""),
                        "pou": row.get("pou", ""),
                        "assignment": row.get("assignment", ""),
                        "fb_type": row.get("fb_type", ""),
                        "fb_type_description": row.get("fb_type_description", ""),
                        "reason": row.get("reason", ""),
                        "source": row.get("source", ""),
                        "kind": row.get("kind", ""),
                        "phase": row.get("phase", "t0"),
                    }
                )
            return compact
        return []

    def run(
        self,
        last_skill: str = "",
        last_gemma_state: str = "",
        process_name: str = "",
        trigger_var: str = "OPCUA.TriggerD2",
        state_name: str = "D2",
        skill_case: str = "",
        max_depth: int = 18,
        assumed_false_states: str = "D1,D2,D3",
        **kwargs,
    ) -> Dict[str, Any]:
        if "g" not in globals():
            return {"error": "Global graph 'g' nicht gesetzt. build_bot(...) zuerst aufrufen."}
        graph = globals()["g"]
        if not isinstance(graph, Graph):
            return {"error": "Global 'g' ist kein rdflib.Graph."}

        if isinstance(assumed_false_states, (list, tuple, set)):
            assumed = [str(x).strip() for x in assumed_false_states if str(x).strip()]
        else:
            assumed = [x.strip() for x in str(assumed_false_states or "").split(",") if x.strip()]

        skill_name = str(last_skill or skill_case or "").strip()
        core = EvD2DiagnosisTool().run(
            last_skill=last_skill,
            last_gemma_state=last_gemma_state,
            trigger_var=trigger_var,
            event_name="evD2",
            port_name_contains=state_name,
            max_rows=250,
        )
        preferred_pous = core.get("skill_setter_pous") if isinstance(core.get("skill_setter_pous"), list) else []
        overlap_pous = (
            core.get("intersection_d2callers_and_skillsetters")
            if isinstance(core.get("intersection_d2callers_and_skillsetters"), list)
            else []
        )
        scope_hints = _collect_scope_hints(
            skill_name,
            process_name,
            preferred_pous,
            overlap_pous,
            core.get("selected_gemma_pou_name", ""),
        )
        preferred_gemma_pous: List[str] = []
        selected_gemma = str(core.get("selected_gemma_pou_name", "") or "")
        if selected_gemma:
            preferred_gemma_pous.append(selected_gemma)
        unified = run_unified_set_and_condition_trace(
            graph,
            last_gemma_state=last_gemma_state,
            state_name=state_name,
            target_var="",
            max_depth=int(max_depth),
            trace_each_truth_path=True,
            assumed_false_states_in_betriebsarten=assumed,
            preferred_gemma_pou_names=preferred_gemma_pous,
            scope_hints=scope_hints,
            verbose_trace=False,
        )
        req = build_requirement_tables(unified)

        preferred_trigger_pous: List[str] = []
        if isinstance(unified.get("origin"), dict):
            origin_pou = str(unified.get("origin", {}).get("caller_pou_name", "") or "")
            if origin_pou:
                preferred_trigger_pous.append(origin_pou)
        if isinstance(unified.get("context"), dict):
            ctx_pou = str(unified.get("context", {}).get("pou_name", "") or "")
            if ctx_pou:
                preferred_trigger_pous.append(ctx_pou)
        core["scope_hints"] = scope_hints
        core["preferred_trigger_pou_names"] = preferred_trigger_pous
        trigger_info = self._select_trigger_setter(core, trigger_var)
        trigger_chain_raw: Dict[str, Any] = {}
        if trigger_info.get("pou_name") and trigger_info.get("condition_expr"):
            trigger_overrides: Dict[str, str] = {}
            auto_port = unified.get("auto_port") if isinstance(unified, dict) else {}
            candidates = auto_port.get("next_trace_candidates") if isinstance(auto_port, dict) else []
            if isinstance(candidates, list):
                cands = [str(c).strip() for c in candidates if str(c).strip()]
                if cands:
                    trigger_overrides[str(state_name)] = " AND ".join(cands)
            assumed_for_trigger = [s for s in assumed if str(s).strip().upper() != str(state_name).strip().upper()]
            trigger_chain_raw = trace_condition_paths_from_pou(
                graph,
                pou_name=str(trigger_info.get("pou_name", "")),
                condition_expr=str(trigger_info.get("condition_expr", "")),
                max_depth=int(max_depth),
                assumed_false_states=assumed_for_trigger,
                token_true_overrides=trigger_overrides or None,
                verbose_trace=False,
            )
        trigger_info["condition_chain"] = self._compact_trigger_condition_trace(trigger_chain_raw)
        skill_identity = self._resolve_skill_identity(skill_name) if skill_name else {}
        skill_trace = self._build_skill_trace(
            last_skill=str(skill_case or skill_name),
            preferred_pous=[str(x) for x in preferred_pous if str(x)],
            process_name=process_name,
        )
        fit = self._score_state_paths(
            req,
            skill_trace if isinstance(skill_trace, dict) else {},
            self.snapshot_index,
        )

        best_paths = fit.get("best_paths", []) if isinstance(fit, dict) else []
        selected_path = best_paths[0] if best_paths else ""
        compact_selected = (
            self._compact_state_path(req, selected_path, state_fit=fit, skill_trace=skill_trace)
            if selected_path
            else []
        )

        point_2 = {
            "last_skill": skill_name,
            "process_name": process_name,
            "abort_statement": (
                f"Letzter ausgeführter Skill war '{skill_name}'. "
                + (f"Der Prozess '{process_name}' wurde danach durch evD2 unterbrochen." if process_name else "Danach wurde die Ausführung durch evD2 unterbrochen.")
            ).strip(),
            "skill_identity": skill_identity,
        }

        return {
            "context": {
                "trigger_var": trigger_var,
                "state_name": state_name,
                "last_skill": skill_name,
                "last_gemma_state_before_failure": last_gemma_state,
                "process_name": process_name,
            },
            "point_1_trigger_setter": trigger_info,
            "point_2_last_skill": point_2,
            "point_3_path_trace": {
                "timeline": [
                    {"phase": "t0", "desc": "Zyklus, in dem TriggerD2/Fehler gesetzt wurde."},
                    {"phase": "t-1", "desc": "Unmittelbar davor (typisch LastGEMMAStateBeforeFailure, Skill-Kontext)."},
                    {"phase": "t-2", "desc": "Vorgeschichte (z. B. vor Toggle / vor Flankenereignis)."},
                ],
                "skill_trace": skill_trace,
                "state_fit": fit,
                "selected_state_path": selected_path,
                "selected_state_path_rows": compact_selected,
            },
            "raw": {
                "core": core,
                "unified_trace": unified,
                "requirement_paths": req,
            },
        }


class DeterministicPathSearchTool(EvD2PathTracingTool):
    name = "deterministic_path_search"
    description = (
        "Generische deterministische Pfadrekonstruktion fuer Ursacheketten. "
        "Unterstuetzt evD2-Bootstrap sowie allgemeine Setter-/Bedingungspfade "
        "fuer andere Variablen oder Hardware-nahe Ursachen."
    )
    usage_guide = (
        "Nutzen, wenn ohne LLM eine konkrete Ursachekette rekonstruiert werden soll. "
        "preset='evd2_bootstrap' fuer den evD2-Startpfad, "
        "preset='assignment_trace' fuer eine Variable in einer bekannten POU, "
        "preset='condition_trace' fuer einen bereits bekannten Bedingungsausdruck."
    )

    @staticmethod
    def _normalize_preset(preset: str) -> str:
        text = str(preset or "evd2_bootstrap").strip().lower()
        aliases = {
            "evd2": "evd2_bootstrap",
            "evd2_path_trace": "evd2_bootstrap",
            "bootstrap": "evd2_bootstrap",
            "assignment": "assignment_trace",
            "var_assignment": "assignment_trace",
            "variable_assignment": "assignment_trace",
            "condition": "condition_trace",
        }
        return aliases.get(text, text or "evd2_bootstrap")

    @staticmethod
    def _normalize_assumed_false_states(assumed_false_states: Any) -> List[str]:
        if isinstance(assumed_false_states, (list, tuple, set)):
            return [str(x).strip() for x in assumed_false_states if str(x).strip()]
        return [x.strip() for x in str(assumed_false_states or "").split(",") if x.strip()]

    @staticmethod
    def _boolish_literal(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().upper()
        if text == "TRUE":
            return True
        if text == "FALSE":
            return False
        return None

    @classmethod
    def _assignment_matches_required_value(cls, assignment: Dict[str, Any], required_value: Any) -> bool:
        req = cls._boolish_literal(required_value)
        rhs = cls._boolish_literal(assignment.get("rhs", ""))
        if req is None:
            return True
        return rhs is req

    @staticmethod
    def _fetch_pou_context(pou_name: str) -> Dict[str, Any]:
        esc = DeterministicPathSearchTool._sparql_escape(pou_name)
        q = f"""
        SELECT ?pou ?code ?lang WHERE {{
          ?pou rdf:type ag:class_POU ;
               dp:hasPOUName "{esc}" ;
               dp:hasPOUCode ?code .
          OPTIONAL {{ ?pou dp:hasPOULanguage ?lang . }}
        }}
        """
        rows = sparql_select_raw(q, max_rows=5)
        if not rows:
            return {"error": f"POU '{pou_name}' nicht im KG gefunden."}
        row = rows[0] if isinstance(rows[0], dict) else {}
        return {
            "pou_name": pou_name,
            "pou_uri": str(row.get("pou", "") or ""),
            "code": str(row.get("code", "") or ""),
            "language": str(row.get("lang", "") or ""),
        }

    def _run_condition_trace(
        self,
        *,
        pou_name: str,
        condition_expr: str,
        max_depth: int,
        assumed_false_states: Any,
        verbose_trace: bool,
    ) -> Dict[str, Any]:
        if "g" not in globals():
            return {"error": "Global graph 'g' nicht gesetzt. build_bot(...) zuerst aufrufen."}
        graph = globals()["g"]
        if not isinstance(graph, Graph):
            return {"error": "Global 'g' ist kein rdflib.Graph."}

        raw = trace_condition_paths_from_pou(
            graph,
            pou_name=str(pou_name or "").strip(),
            condition_expr=str(condition_expr or "").strip(),
            max_depth=int(max_depth),
            assumed_false_states=self._normalize_assumed_false_states(assumed_false_states),
            verbose_trace=bool(verbose_trace),
        )
        compact = self._compact_trigger_condition_trace(raw)
        return {
            "preset": "condition_trace",
            "context": {
                "pou_name": str(pou_name or "").strip(),
                "condition_expr": str(condition_expr or "").strip(),
                "max_depth": int(max_depth),
            },
            "condition_trace": compact,
            "raw": {"condition_trace": raw},
        }

    def _run_assignment_trace(
        self,
        *,
        pou_name: str,
        target_var: str,
        required_value: Any,
        max_depth: int,
        assumed_false_states: Any,
        verbose_trace: bool,
    ) -> Dict[str, Any]:
        ctx = self._fetch_pou_context(str(pou_name or "").strip())
        if ctx.get("error"):
            return ctx

        code = str(ctx.get("code", "") or "")
        if not code:
            return {"error": f"Kein Code fuer POU '{pou_name}' im KG gefunden."}

        assignments = extract_assignments_st(code, str(target_var or "").strip(), trace=None)
        if not assignments:
            return {"error": f"Keine Assignments fuer '{target_var}' in POU '{pou_name}' gefunden."}

        matching = [
            a
            for a in assignments
            if isinstance(a, dict) and self._assignment_matches_required_value(a, required_value)
        ]
        chosen = matching[-1] if matching else assignments[-1]
        condition_expr = self._conditions_to_expr(chosen.get("conditions", []))
        if not condition_expr:
            return {
                "preset": "assignment_trace",
                "context": {
                    "pou_name": str(pou_name or "").strip(),
                    "target_var": str(target_var or "").strip(),
                    "required_value": str(required_value or "").strip() or "TRUE",
                    "max_depth": int(max_depth),
                },
                "assignment": chosen,
                "condition_expr": "",
                "condition_trace": {
                    "error": f"Assignment fuer '{target_var}' hat keine tracebare Bedingung."
                },
                "raw": {"condition_trace": {}},
            }

        raw = self._run_condition_trace(
            pou_name=str(pou_name or "").strip(),
            condition_expr=condition_expr,
            max_depth=int(max_depth),
            assumed_false_states=assumed_false_states,
            verbose_trace=bool(verbose_trace),
        )
        return {
            "preset": "assignment_trace",
            "context": {
                "pou_name": str(pou_name or "").strip(),
                "target_var": str(target_var or "").strip(),
                "required_value": str(required_value or "").strip() or "TRUE",
                "language": str(ctx.get("language", "") or ""),
                "max_depth": int(max_depth),
            },
            "assignment": chosen,
            "condition_expr": condition_expr,
            "condition_trace": raw.get("condition_trace", {}),
            "raw": {
                "condition_trace": raw.get("raw", {}).get("condition_trace", {})
                if isinstance(raw.get("raw"), dict)
                else {},
            },
        }

    def run(
        self,
        *,
        preset: str = "evd2_bootstrap",
        last_skill: str = "",
        last_gemma_state: str = "",
        process_name: str = "",
        trigger_var: str = "OPCUA.TriggerD2",
        state_name: str = "D2",
        skill_case: str = "",
        max_depth: int = 18,
        assumed_false_states: str = "D1,D2,D3",
        pou_name: str = "",
        target_var: str = "",
        required_value: Any = "TRUE",
        condition_expr: str = "",
        verbose_trace: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        preset_norm = self._normalize_preset(preset)
        if preset_norm == "condition_trace":
            return self._run_condition_trace(
                pou_name=pou_name,
                condition_expr=condition_expr,
                max_depth=max_depth,
                assumed_false_states=assumed_false_states,
                verbose_trace=verbose_trace,
            )
        if preset_norm == "assignment_trace":
            return self._run_assignment_trace(
                pou_name=pou_name,
                target_var=target_var,
                required_value=required_value,
                max_depth=max_depth,
                assumed_false_states=assumed_false_states,
                verbose_trace=verbose_trace,
            )
        if preset_norm != "evd2_bootstrap":
            return {
                "error": (
                    "Unbekanntes preset fuer deterministic_path_search: "
                    f"{preset}. Erlaubt sind evd2_bootstrap, assignment_trace, condition_trace."
                )
            }
        return super().run(
            last_skill=last_skill,
            last_gemma_state=last_gemma_state,
            process_name=process_name,
            trigger_var=trigger_var,
            state_name=state_name,
            skill_case=skill_case,
            max_depth=max_depth,
            assumed_false_states=assumed_false_states,
            **kwargs,
        )


# ----------------------------
# ChatBot Planner
# ----------------------------

class ChatBot:
    def __init__(
        self,
        registry: ToolRegistry,
        llm_invoke_fn: Callable,
        planner_llm_invoke_fn: Optional[Callable] = None,
    ):
        self.registry = registry
        self.llm = llm_invoke_fn
        # Falls nicht angegeben, nutzt der Planner dasselbe Modell wie die Synthese
        self._planner_llm = planner_llm_invoke_fn if planner_llm_invoke_fn is not None else llm_invoke_fn
        self.history: List[Dict[str, Any]] = []
        self.sticky_context: Dict[str, Any] = {}

    @staticmethod
    def _extract_current_user_question(user_msg: str) -> str:
        text = str(user_msg or "")
        marker = "User Frage:\n"
        if marker in text:
            return text.rsplit(marker, 1)[-1].strip()
        return text.strip()

    @staticmethod
    def _short_text(text: Any, max_chars: int = 700) -> str:
        s = "" if text is None else str(text)
        s = re.sub(r"\s+", " ", s).strip()
        if len(s) <= max_chars:
            return s
        return s[:max_chars] + " ..."

    @staticmethod
    def _question_wants_chain(question: str) -> bool:
        q = (question or "").lower()
        keys = [
            "woher",
            "herkommt",
            "wo kommt",
            "wo gesetzt",
            "gesetzt wird",
            "ursprung",
            "origin",
            "kette",
            "trace",
            "call chain",
        ]
        return any(k in q for k in keys)

    @staticmethod
    def _question_wants_root_cause(question: str, *, first_turn: bool) -> bool:
        q = (question or "").lower()
        if first_turn and not q.strip():
            return True
        keys = [
            "root-cause",
            "root cause",
            "ursache",
            "gesamtanalyse",
            "warum evd2",
            "warum triggerd2",
            "triggerd2",
            "opcua.triggerd2",
            "gemma",
        ]
        return any(k in q for k in keys)

    @staticmethod
    def _extract_focus_symbols(question: str, max_symbols: int = 4) -> List[str]:
        text = question or ""
        tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_.]*", text)
        stop = {
            "ok",
            "bitte",
            "wo",
            "wie",
            "warum",
            "was",
            "und",
            "oder",
            "die",
            "der",
            "das",
            "von",
            "mit",
            "kette",
            "komplette",
            "frage",
            "user",
            "diagnose",
            "trigger",
            "root",
            "cause",
            "false",
            "true",
        }
        out: List[str] = []
        seen: Set[str] = set()
        for t in tokens:
            tl = t.lower()
            if tl in stop:
                continue
            keep = (
                ("_" in t)
                or ("." in t)
                or t.startswith("OPCUA")
                or t.startswith("GVL")
                or bool(re.search(r"\d", t))                 # z.B. Schritt1, rStep1
                or bool(re.search(r"[a-z][A-Z]", t))         # camelCase, z.B. lastSkillIsOne
                or bool(re.search(r"[A-Z][a-z]+[A-Z]", t))   # Pascal/camel mixed
            )
            if not keep:
                continue
            if t not in seen:
                seen.add(t)
                out.append(t)
            if len(out) >= max_symbols:
                break
        return out

    @staticmethod
    def _extract_symbol_from_question(question: str) -> str:
        q = question or ""
        m = re.search(r"([A-Za-z_][A-Za-z0-9_.]*)\s*=", q)
        if m:
            return m.group(1)
        m = re.search(r"(?i)(?:woher\s+kommt|wo\s+kommt|ursprung\s+von)\s+([A-Za-z_][A-Za-z0-9_.]*)", q)
        if m:
            return m.group(1)
        return ""

    def _recent_history_text(self, max_turns: int = 3) -> str:
        if not self.history:
            return "Keine vorherigen Turns."
        tail = self.history[-max_turns:]
        lines: List[str] = []
        for i, h in enumerate(tail, start=1):
            prev_user = self._extract_current_user_question(str(h.get("user", "")))
            prev_ans = self._short_text(h.get("resp", {}).get("answer", ""), max_chars=500)
            lines.append(f"{i}. Q: {self._short_text(prev_user, max_chars=350)}")
            lines.append(f"{i}. A: {prev_ans}")
        return "\n".join(lines)

    def remember_turn(self, user_msg: str, resp: Dict[str, Any]) -> None:
        self.history.append({"user": user_msg, "resp": resp})

    def set_sticky_context(self, sticky_context: Any) -> None:
        self.sticky_context = sticky_context if isinstance(sticky_context, dict) else {}

    def _augment_plan_for_followup(self, plan: Dict[str, Any], question: str) -> Dict[str, Any]:
        if not isinstance(plan, dict):
            return plan
        if str(plan.get("ask_user", "") or plan.get("clarification_question", "")).strip():
            return plan
        steps = plan.get("steps")
        if not isinstance(steps, list):
            return plan

        if not self._question_wants_chain(question):
            return plan

        question_l = question.lower()
        asks_full_d2 = any(k in question_l for k in ["evd2", "triggerd2", "gemma", "root-cause", "ursache"])

        filtered_steps = steps
        if not asks_full_d2:
            filtered_steps = [
                s
                for s in steps
                if str(s.get("tool", "")) not in {
                    "evd2_diagnoseplan",
                    "evd2_unified_trace",
                    "evd2_requirement_paths",
                    "evd2_path_trace",
                    "deterministic_path_search",
                }
            ]

        focus_symbols = self._extract_focus_symbols(question)
        target = focus_symbols[0] if focus_symbols else self._extract_symbol_from_question(question)
        existing_tools = {str(s.get("tool", "")) for s in steps if isinstance(s, dict)}
        question_l = question.lower()
        wants_runtime_value = any(
            k in question_l
            for k in ["snapshot", "runtime", "laufzeit", "aktuell", "wert", "beobachtet", "timer", "plcsnapshot"]
        )

        prepend_steps: List[Dict[str, Any]] = []
        if target and wants_runtime_value and "plc_snapshot_search" not in existing_tools:
            prepend_steps.append(
                {
                    "tool": "plc_snapshot_search",
                    "args": {"name_contains": target, "include_rows": False, "max_hits": 12},
                }
            )
        if target and "search_variables" not in existing_tools:
            prepend_steps.append({"tool": "search_variables", "args": {"name_contains": target}})
        if target and "variable_trace" not in existing_tools:
            prepend_steps.append({"tool": "variable_trace", "args": {"var_name": target}})
        if target and "graph_investigate" not in existing_tools:
            prepend_steps.append(
                {
                    "tool": "graph_investigate",
                    "args": {
                        "seed_terms": [target, "OPCUA.TriggerD2", "OPCUA.lastExecutedSkill"],
                        "target_terms": [target],
                        "max_iters": 30,
                        "max_nodes": 220,
                    },
                }
            )

        if not prepend_steps and filtered_steps:
            new_plan = dict(plan)
            new_plan["steps"] = filtered_steps
            return new_plan

        if not prepend_steps and not filtered_steps:
            fallback_term = target or "Schritt1"
            new_plan = dict(plan)
            new_plan["steps"] = [{"tool": "general_search", "args": {"name_contains": fallback_term}}]
            return new_plan

        if not prepend_steps:
            return plan

        new_plan = dict(plan)
        new_plan["steps"] = prepend_steps + filtered_steps
        return new_plan

    def _prefer_evd2_path_trace_plan(self, plan: Dict[str, Any], question: str) -> Dict[str, Any]:
        if not isinstance(plan, dict):
            return plan
        if str(plan.get("ask_user", "") or plan.get("clarification_question", "")).strip():
            return plan
        steps = plan.get("steps")
        if not isinstance(steps, list) or not steps:
            return plan

        evd2_tools = {
            "evd2_diagnoseplan",
            "evd2_unified_trace",
            "evd2_requirement_paths",
            "evd2_path_trace",
            "deterministic_path_search",
        }
        has_evd2_tool = any(str(s.get("tool", "")) in evd2_tools for s in steps if isinstance(s, dict))
        q = (question or "").lower()
        asks_evd2 = any(k in q for k in ["evd2", "triggerd2", "opcua.triggerd2", "gemma", "root-cause", "ursache"])
        if not has_evd2_tool and not asks_evd2:
            return plan

        merged_args: Dict[str, Any] = {
            "trigger_var": "OPCUA.TriggerD2",
            "state_name": "D2",
            "max_depth": 18,
            "assumed_false_states": "D1,D2,D3",
        }

        for s in steps:
            if not isinstance(s, dict):
                continue
            tool = str(s.get("tool", ""))
            args = s.get("args", {}) if isinstance(s.get("args"), dict) else {}
            if tool == "deterministic_path_search":
                merged_args.update(args)
            elif tool == "evd2_path_trace":
                merged_args.update(args)
            elif tool == "evd2_diagnoseplan":
                if args.get("last_skill"):
                    merged_args["last_skill"] = args.get("last_skill")
                if args.get("last_gemma_state"):
                    merged_args["last_gemma_state"] = args.get("last_gemma_state")
                if args.get("trigger_var"):
                    merged_args["trigger_var"] = args.get("trigger_var")
                if args.get("event_name") and not merged_args.get("event_name"):
                    merged_args["event_name"] = args.get("event_name")
            elif tool in {"evd2_unified_trace", "evd2_requirement_paths"}:
                if args.get("last_gemma_state"):
                    merged_args["last_gemma_state"] = args.get("last_gemma_state")
                if args.get("state_name"):
                    merged_args["state_name"] = args.get("state_name")
                if args.get("max_depth"):
                    merged_args["max_depth"] = args.get("max_depth")
                if args.get("assumed_false_states"):
                    merged_args["assumed_false_states"] = args.get("assumed_false_states")

        new_plan = dict(plan)
        new_plan["steps"] = [
            {
                "tool": "deterministic_path_search",
                "args": {"preset": "evd2_bootstrap", **merged_args},
            }
        ]
        return new_plan

    def _get_dynamic_planner_prompt(self, retry_hint: str = "") -> str:
        tool_docs = self.registry.get_system_prompt_part()

        heuristics = []
        for tool in self.registry._tools.values():
            if tool.usage_guide:
                heuristics.append(f"- {tool.usage_guide} -> {tool.name}")

        retry_msg = f"\nACHTUNG - VORHERIGER VERSUCH GESCHEITERT:\n{retry_hint}\n" if retry_hint else ""

        return f"""
Du bist ein Planner für einen PLC Knowledge-Graph ChatBot.
Zerlege die Anfrage in Tool-Aufrufe.

{tool_docs}

STRATEGIE BEI PUNKTEN (z.B. "GVL.Start"):
- Ein Punkt deutet oft auf Variable, Port oder Instanz hin.
- Nutze 'general_search', um herauszufinden, was es ist (POU vs. Variable).
- Wenn du sicher bist, dass es eine Variable ist -> 'variable_trace' oder 'search_variables'.

ROOT-CAUSE STRATEGIE (Fixpoint-Search):
- Wenn der User eine echte Root-Cause-Kette will (Setter -> Guard -> Upstream-Signale), nutze 'graph_investigate'.
- Gib als seed_terms die wichtigsten Startknoten: Trigger-Variable(n), lastSkill, und Symbole aus Setter-Guards.
- Nutze die returned evidence/edges, um konkret zu erklären, welche Bedingung den Setter ausführt.
- Wenn bereits POU + Variable/Bedingung bekannt sind und du einen deterministischen Signal-/Hardwarepfad willst, nutze 'deterministic_path_search'.
- Bei evD2-Analysen zuerst nur 'deterministic_path_search' mit `preset="evd2_bootstrap"` planen (Single-Source-Analyse), nicht mehrere evd2_* Tools parallel.

FOLGEFRAGEN:
- Nutze fuer Folgefragen zuerst die bisherige Chat-Historie und fruehere Tool-Ergebnisse.
- Plane fuer schmale Folgefragen lieber kleine, gezielte Tools als erneut einen kompletten evD2-Bootstrap.
- Auf den plcSnapshot nur bei Bedarf zugreifen; dafuer ist 'plc_snapshot_search' da.

UNSICHERHEIT / RUECKFRAGEN:
- Wenn die Frage mehrdeutig ist oder ohne weitere Eingrenzung kein sinnvoller Tool-Plan moeglich ist, stelle eine Rueckfrage.
- Gib dann KEINE steps aus, sondern JSON in dieser Form:
  {{"ask_user": "Deine gezielte Rueckfrage"}}

RE-PLANNING NACH TOOL-FEHLERN:
- Wenn bisherige Tool-Ergebnisse Fehler oder Luecken zeigen, nutze diese Informationen aktiv fuer einen neuen, angepassten Plan.
- Wiederhole denselben gescheiterten Plan nicht blind.

Heuristiken:
{chr(10).join(heuristics)}
- Wenn nach mehreren Tool Aufrufen keine Treffer kommen, nutze string_triple_search(term) als letzten Fallback
- Sonst -> text2sparql_select

{retry_msg}

Ausgabeformat (NUR JSON):
{{
  "steps": [
    {{"tool": "tool_name", "args": {{"arg1": "wert1"}} }}
  ]
}}
"""

    @staticmethod
    def _extract_clarification_question(plan: Dict[str, Any]) -> str:
        if not isinstance(plan, dict):
            return ""
        return str(plan.get("ask_user", "") or plan.get("clarification_question", "") or "").strip()

    @staticmethod
    def _tool_result_has_payload(value: Any) -> bool:
        if isinstance(value, list):
            return len(value) > 0
        if isinstance(value, dict):
            if "error" in value:
                return False
            for v in value.values():
                if isinstance(v, list) and len(v) > 0:
                    return True
                if isinstance(v, dict) and v:
                    return True
                if v not in (None, "", [], {}, False):
                    return True
            return False
        return value not in (None, "", [], {}, False)

    def _has_useful_results(self, results: Dict[str, Any]) -> bool:
        if not isinstance(results, dict) or not results:
            return False
        return any(self._tool_result_has_payload(v) for v in results.values())

    @staticmethod
    def _tool_result_has_error(value: Any) -> bool:
        return isinstance(value, dict) and "error" in value

    def _has_tool_errors(self, results: Dict[str, Any]) -> bool:
        if not isinstance(results, dict):
            return False
        return any(self._tool_result_has_error(v) for v in results.values())

    def _needs_replan(self, results: Dict[str, Any]) -> bool:
        if not isinstance(results, dict) or not results:
            return True
        if self._has_tool_errors(results):
            return True
        return not self._has_useful_results(results)

    def _tool_failure_summary(self, results: Dict[str, Any]) -> str:
        if not isinstance(results, dict):
            return ""
        lines: List[str] = []
        for step_name, value in results.items():
            if isinstance(value, dict) and "error" in value:
                lines.append(f"- {step_name}: {value.get('error')}")
                continue
            if isinstance(value, dict):
                nested = self._tool_failure_summary(value)
                if nested:
                    for line in nested.splitlines():
                        lines.append(f"- {step_name}.{line.lstrip('- ').strip()}")
        return "\n".join(lines)

    def _build_retry_hint(self, results: Dict[str, Any]) -> str:
        if not isinstance(results, dict) or not results:
            return "Keine Tool-Ergebnisse vorhanden. Plane breiter."
        if self._has_tool_errors(results):
            return (
                "Mindestens ein Tool ist fehlgeschlagen. "
                "Nutze die Fehlermeldungen und vorhandene Teiltreffer fuer einen angepassten Plan.\n"
                + self._tool_failure_summary(results)
            ).strip()
        return "Die bisherigen Tool-Ergebnisse waren leer oder nicht verwertbar. Plane gezielter oder breiter nach."

    @staticmethod
    def _json_preview(data: Any, max_chars: int = 12000) -> str:
        try:
            text = json.dumps(data, ensure_ascii=False, indent=2)
        except Exception:
            text = str(data)
        return text[:max_chars]

    def _recent_turn_channels(self, max_turns: int = 3) -> Dict[str, Any]:
        if not self.history:
            return {}

        tail = self.history[-max_turns:]
        turn_summaries: List[Dict[str, Any]] = []
        for entry in tail:
            if not isinstance(entry, dict):
                continue
            resp = entry.get("resp") if isinstance(entry.get("resp"), dict) else {}
            tool_results = resp.get("tool_results") if isinstance(resp.get("tool_results"), dict) else {}
            plan = resp.get("plan")

            successful_steps: List[str] = []
            failed_steps: List[Dict[str, str]] = []
            for step_name, value in tool_results.items():
                if isinstance(value, dict) and "error" in value:
                    failed_steps.append(
                        {
                            "step": str(step_name),
                            "error": self._short_text(value.get("error", ""), max_chars=220),
                        }
                    )
                    continue
                if self._tool_result_has_payload(value):
                    successful_steps.append(str(step_name))

            turn_summary: Dict[str, Any] = {
                "question": self._short_text(
                    self._extract_current_user_question(str(entry.get("user", ""))),
                    max_chars=220,
                ),
                "answer_summary": self._short_text(resp.get("answer", ""), max_chars=320),
            }
            if successful_steps:
                turn_summary["successful_steps"] = successful_steps[:4]
            if failed_steps:
                turn_summary["failed_steps"] = failed_steps[:3]
            if isinstance(plan, dict) and isinstance(plan.get("attempts"), list):
                turn_summary["replanned"] = True
            turn_summaries.append(turn_summary)

        return {"recent_turns": turn_summaries}

    def _build_persistent_context_block(self) -> str:
        sections: List[str] = []
        if self.sticky_context:
            sections.append(
                "Persistenter Analysekontext (JSON):\n"
                + self._json_preview(self.sticky_context, max_chars=9000)
            )

        turn_channels = self._recent_turn_channels(max_turns=3)
        recent_turns = turn_channels.get("recent_turns") if isinstance(turn_channels, dict) else None
        if isinstance(recent_turns, list) and recent_turns:
            sections.append(
                "Kanalisierte Zwischenergebnisse (JSON):\n"
                + self._json_preview(turn_channels, max_chars=5000)
            )
        return "\n\n".join(sections)

    def _build_planner_input(
        self,
        *,
        current_question: str,
        original_user_msg: str,
        first_turn: bool,
        wants_chain: bool,
        wants_root: bool,
        extra_context: str = "",
        prior_plan: Optional[Dict[str, Any]] = None,
        prior_results: Optional[Dict[str, Any]] = None,
    ) -> str:
        question_text = current_question if current_question else original_user_msg
        sections: List[str] = [f"Aktuelle Userfrage:\n{question_text}"]

        persistent_context = self._build_persistent_context_block()
        if persistent_context:
            sections.append(persistent_context)

        if extra_context:
            sections.append(f"Zusatzkontext:\n{extra_context}")

        if not first_turn and not (wants_chain and not wants_root):
            sections.append("Vorherige kurze Historie:\n" + self._recent_history_text(max_turns=2))

        if prior_plan:
            sections.append("Vorheriger Plan (JSON):\n" + self._json_preview(prior_plan, max_chars=4000))
        if prior_results:
            sections.append("Bisherige Tool-Ergebnisse (JSON):\n" + self._json_preview(prior_results, max_chars=12000))

        return "\n\n".join(sections)

    def _planner(self, user_msg: str, retry_hint: str = "") -> Dict[str, Any]:
        system = self._get_dynamic_planner_prompt(retry_hint=retry_hint)
        raw = self._planner_llm(system, user_msg)
        # Attempt 1: strip code fences and parse directly
        try:
            plan = json.loads(strip_code_fences(raw))
            if isinstance(plan, dict) and ("steps" in plan or self._extract_clarification_question(plan)):
                return plan
        except Exception:
            pass
        # Attempt 2: extract first {...} block from raw text (handles preamble text)
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            try:
                plan = json.loads(m.group(0))
                if isinstance(plan, dict) and ("steps" in plan or self._extract_clarification_question(plan)):
                    return plan
            except Exception:
                pass
        return {"error": "Planner JSON parse failed", "raw": raw}

    def _execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        steps = plan.get("steps", [])
        if not isinstance(steps, list):
            return {"error": "steps is not a list"}

        for i, step in enumerate(steps):
            tool_name = str(step.get("tool", ""))
            args = step.get("args", {})
            if not isinstance(args, dict):
                args = {}
            out[f"step_{i+1}:{tool_name}"] = self.registry.execute(tool_name, args)
        return out

    def _prepare_plan_for_execution(self, plan: Dict[str, Any], current_question: str, *, wants_root: bool) -> Dict[str, Any]:
        if not isinstance(plan, dict):
            return plan
        if self._extract_clarification_question(plan):
            return plan
        prepared = self._augment_plan_for_followup(plan, current_question)
        if wants_root:
            prepared = self._prefer_evd2_path_trace_plan(prepared, current_question)
        return prepared

    @staticmethod
    def _combine_tool_result_attempts(attempts: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not attempts:
            return {}
        if len(attempts) == 1:
            return attempts[0]
        return {f"attempt_{idx}": attempt for idx, attempt in enumerate(attempts, start=1)}

    def chat(self, user_msg: str, debug: bool = True, extra_context: str = "") -> Dict[str, Any]:
        current_question = self._extract_current_user_question(user_msg)
        first_turn = len(self.history) == 0
        wants_root = self._question_wants_root_cause(current_question, first_turn=first_turn)
        wants_chain = self._question_wants_chain(current_question)
        planner_input = self._build_planner_input(
            current_question=current_question,
            original_user_msg=user_msg,
            first_turn=first_turn,
            wants_chain=wants_chain,
            wants_root=wants_root,
            extra_context=extra_context,
        )

        raw_plan = self._planner(planner_input)
        if "error" in raw_plan:
            return {"answer": f"Planner error: {raw_plan.get('error')}", "plan": raw_plan, "tool_results": {}}

        clarification = self._extract_clarification_question(raw_plan)
        if clarification:
            resp = {"answer": clarification, "plan": raw_plan if debug else None, "tool_results": {}}
            self.remember_turn(user_msg, resp)
            return resp

        plan = self._prepare_plan_for_execution(raw_plan, current_question, wants_root=wants_root)

        attempt_plans: List[Dict[str, Any]] = [plan]
        attempt_results: List[Dict[str, Any]] = []

        max_replans = 2
        for _ in range(max_replans + 1):
            tool_results = self._execute_plan(plan)
            attempt_results.append(tool_results)

            if not self._needs_replan(tool_results):
                break

            retry_input = self._build_planner_input(
                current_question=current_question,
                original_user_msg=user_msg,
                first_turn=first_turn,
                wants_chain=wants_chain,
                wants_root=wants_root,
                extra_context=extra_context,
                prior_plan=plan,
                prior_results=tool_results,
            )
            retry_plan = self._planner(retry_input, retry_hint=self._build_retry_hint(tool_results))
            if "error" in retry_plan:
                break

            clarification = self._extract_clarification_question(retry_plan)
            if clarification:
                debug_plan = retry_plan if debug else None
                debug_results = self._combine_tool_result_attempts(attempt_results) if debug else {}
                resp = {"answer": clarification, "plan": debug_plan, "tool_results": debug_results}
                self.remember_turn(user_msg, resp)
                return resp

            prepared_retry_plan = self._prepare_plan_for_execution(retry_plan, current_question, wants_root=wants_root)
            if self._json_preview(prepared_retry_plan, max_chars=8000) == self._json_preview(plan, max_chars=8000):
                break
            plan = prepared_retry_plan
            attempt_plans.append(plan)

        tool_results = self._combine_tool_result_attempts(attempt_results)
        effective_plan: Dict[str, Any] = plan if len(attempt_plans) == 1 else {"attempts": attempt_plans}

        # 4) Final answer synthesis
        system = (
            "Du bist ein PLC Knowledge-Graph Assistant fuer Root-Cause-Analysen. "
            "Antworte ausschliesslich auf Deutsch. "
            "Prioritaet 1: Beantworte die AKTUELLE Userfrage direkt und spezifisch. "
            "Wiederhole keine generischen Standardabschnitte, wenn sie nicht explizit gefragt sind. "
            "Nutze nur Fakten aus Tool-Ergebnissen; keine erfundenen Ketten oder Variablenwege. "
            "Verweise explizit auf Tool-Ergebnisse (POU-Namen, Variablen, Ports, Snippets, Zeilennummern). "
            "Wenn etwas im KG nicht gefunden wird, sage das klar und nenne den naechsten konkreten Debug-Schritt. "
            "Wenn Tool-Aufrufe fehlgeschlagen sind, erwaehne das kurz und sachlich. "
            "Wenn die vorhandenen Fakten fuer eine belastbare Antwort nicht reichen, stelle eine gezielte Rueckfrage statt zu halluzinieren."
        )

        history_ctx = self._recent_history_text(max_turns=3)
        if wants_chain and not wants_root and self.history:
            last_q = self._extract_current_user_question(str(self.history[-1].get("user", "")))
            history_ctx = f"Letzte Userfrage davor: {self._short_text(last_q, max_chars=320)}"

        if wants_chain and not wants_root:
            answer_format = (
                "Antwortmodus: gezielte Follow-up Frage zur Herkunft/Kette.\n"
                "Liefer bitte in dieser Reihenfolge:\n"
                "1) Direkte Antwort auf die Frage in 1-3 Saetzen.\n"
                "2) Kette als konkrete Pfeilkette (A -> B -> C), nur mit belegten Kanten.\n"
                "3) Pro Kettenschritt: POU/Code-Stelle/Bedingung aus Tool-Ergebnissen.\n"
                "4) Falls eine Kante fehlt: explizit als Luecke markieren und 1-3 naechste Tool-Calls nennen.\n"
                "Wichtig: Keine Wiederholung der generischen evD2-Standardantwort."
            )
        elif wants_root:
            answer_format = (
                "Antwortmodus: Root-Cause-Analyse.\n"
                "Bitte liefere strukturiert:\n"
                "1) `OPCUA.TriggerD2 := TRUE` - wo genau (POU + Snippet) und unter welcher Bedingung?\n"
                "2) Kurz: letzter Skill + Prozessabbruch + Skill-Einordnung im KG "
                "(ag:class_Skill Treffer ja/nein, sonst Stringliteral-Setter im Code).\n"
                "3) Komplette Pfad-Rekonstruktion (rueckwaerts): Skill-Pfad -> State-Pfad -> Trigger-Pfad "
                "mit Zeitphasen t0/t-1/t-2, inklusive Timer/Flanken-Bedingungen und gewaehltem Best-Path.\n"
                "4) Konkrete naechste Debug-Schritte (welche Variablen beobachten, welche POUs oeffnen, welche Tool-Calls als naechstes)."
            )
        else:
            answer_format = (
                "Antwortmodus: normale Follow-up Frage.\n"
                "Beantworte nur die aktuelle Frage praezise und knapp. "
                "Keine Standard-Wiederholung aus vorherigen Antworten."
            )

        failure_summary = self._tool_failure_summary(tool_results)
        persistent_context_block = self._build_persistent_context_block()
        extra_context_block = f"Zusatzkontext:\n{extra_context}\n\n" if extra_context else ""
        sticky_block = f"{persistent_context_block}\n\n" if persistent_context_block else ""
        failure_block = f"Tool-Fehler:\n{failure_summary}\n\n" if failure_summary else ""
        user = (
            f"Aktuelle Frage:\n{current_question}\n\n"
            f"{sticky_block}"
            f"Vorherige Turns (Kurzkontext):\n{history_ctx}\n\n"
            f"{extra_context_block}"
            f"{failure_block}"
            f"Tool-Ergebnisse (JSON):\n{json.dumps(tool_results, ensure_ascii=False, indent=2)[:16000]}\n\n"
            f"{answer_format}\n"
        )
        answer = self.llm(system, user)

        resp = {"answer": answer, "plan": effective_plan if debug else None, "tool_results": tool_results if debug else None}
        self.remember_turn(user_msg, resp)
        return resp

# ----------------------------
# build_bot entry
# ----------------------------

def build_bot(
    *,
    kg_ttl_path: str,
    openai_model: str = "gpt-4o-mini",
    openai_temperature: float = 0,
    provider: str = "openai",
    planner_model: Optional[str] = None,
    planner_provider: Optional[str] = None,
    usage_accumulator: Optional["LLMUsage"] = None,
    pricing: Optional[Dict[str, float]] = None,
    planner_pricing: Optional[Dict[str, float]] = None,
    routine_index_dir: Optional[str] = None,
    enable_rag: bool = True,
    plc_snapshot: Any = None,
) -> ChatBot:
    """Baut einen ChatBot mit optional getrennten Modellen für Planner und Synthese.

    planner_model / planner_provider: falls abweichend vom Synthese-Modell.
    usage_accumulator: LLMUsage-Objekt, in das Tokens kumuliert werden.
    pricing / planner_pricing: {"input_per_1k": ..., "output_per_1k": ...} in USD.
    """
    _provider = (provider or "openai").lower().strip()
    if _provider not in ("ollama",):
        env_var = _PROVIDER_ENV_VARS.get(_provider, "OPENAI_API_KEY")
        if not os.environ.get(env_var):
            raise RuntimeError(f"{env_var} ist nicht gesetzt (ENV).")

    ttl = Path(kg_ttl_path).expanduser().resolve()
    if not ttl.exists():
        raise FileNotFoundError(f"KG TTL nicht gefunden: {ttl}")

    # Global graph setzen (damit sparql_select_raw wie im Notebook funktioniert)
    globals()["g"] = Graph()
    globals()["g"].parse(str(ttl), format="turtle")

    kg_store = KGStore(globals()["g"])
    sc = schema_card(globals()["g"], top_n=15)

    # Routine index
    idx_dir = Path(routine_index_dir).expanduser().resolve() if routine_index_dir else ttl.parent
    idx_dir.mkdir(parents=True, exist_ok=True)
    idx_path = idx_dir / (ttl.stem + "_routine_index.json")

    if idx_path.exists() and idx_path.stat().st_size > 0:
        routine_index = RoutineIndex.load(str(idx_path))
    else:
        routine_index = RoutineIndex.build_from_kg(kg_store)
        routine_index.save(str(idx_path))

    llm_invoke = get_llm_invoke(
        model=openai_model,
        temperature=openai_temperature,
        provider=_provider,
        usage_accumulator=usage_accumulator,
        pricing=pricing,
    )

    # Planner-Modell (optional abweichend)
    _planner_provider = (planner_provider or _provider).lower().strip()
    _planner_model = planner_model or openai_model
    if planner_provider or planner_model:
        if _planner_provider not in ("ollama",):
            env_var_p = _PROVIDER_ENV_VARS.get(_planner_provider, "OPENAI_API_KEY")
            if not os.environ.get(env_var_p):
                raise RuntimeError(f"Planner: {env_var_p} ist nicht gesetzt (ENV).")
        planner_invoke = get_llm_invoke(
            model=_planner_model,
            temperature=openai_temperature,
            provider=_planner_provider,
            usage_accumulator=usage_accumulator,
            pricing=planner_pricing,
        )
    else:
        planner_invoke = None  # ChatBot fällt auf llm_invoke zurück

    registry = ToolRegistry()
    registry.register(ListProgramsTool())
    registry.register(EvD2DiagnosisTool())
    registry.register(EvD2UnifiedTraceTool())
    registry.register(EvD2RequirementPathsTool())
    registry.register(DeterministicPathSearchTool(plc_snapshot))
    registry.register(EvD2PathTracingTool(plc_snapshot))
    registry.register(CalledPousTool())
    registry.register(PouCallersTool())
    registry.register(PouCodeTool())
    registry.register(SearchVariablesTool())
    registry.register(VariableTraceTool())
    registry.register(PLCSnapshotSearchTool(plc_snapshot))
    registry.register(GeneralSearchTool())
    registry.register(GraphInvestigateTool())
    registry.register(StringTripleSearchTool(kg_store))
    registry.register(ExceptionAnalysisTool(kg_store, routine_index))
    registry.register(Text2SparqlTool(llm_invoke, sc))

    if enable_rag:
        vs = build_vector_index(kg_store, registry)
        if vs is not None:
            registry.register(SemanticSearchTool(vs))

    return ChatBot(registry, llm_invoke, planner_llm_invoke_fn=planner_invoke)


