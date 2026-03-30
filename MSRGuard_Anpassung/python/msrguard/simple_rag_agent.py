from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from .chatbot_core import get_llm_invoke


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _as_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _normalize_event_input(obj: Dict[str, Any]) -> Dict[str, Any]:
    if "event" in obj and isinstance(obj["event"], dict):
        return obj["event"]
    return obj


def _get_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _pick(payload: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _load_json(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\", "\\\\"))


def _shorten_json(x: Any, max_chars: int = 3200) -> str:
    try:
        text = json.dumps(x, ensure_ascii=False, indent=2)
    except Exception:
        text = str(x)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... (gekuerzt)"


def _resolve_export_xml_from_pipeline_config(config_path: str) -> str:
    if not config_path:
        return ""
    path = Path(config_path).expanduser().resolve()
    if not path.exists():
        return ""
    try:
        cfg = _load_json(path)
    except Exception:
        return ""
    project_dir = str(cfg.get("project_dir") or "").strip()
    if not project_dir:
        return ""
    export_xml = Path(project_dir).expanduser().resolve() / "export.xml"
    return str(export_xml)


def _resolve_xml_path(
    obj: Dict[str, Any],
    *,
    explicit_xml_path: str = "",
    pipeline_config_path: str = "",
) -> str:
    event = _normalize_event_input(obj)
    payload = _get_payload(event)

    candidates = [
        _pick(payload, ["plcopen_xml_path", "plcopenXmlPath", "export_xml_path", "exportXmlPath", "xml_path", "xmlPath"]),
        explicit_xml_path,
        os.environ.get("MSRGUARD_PLCOPEN_XML", ""),
        _resolve_export_xml_from_pipeline_config(pipeline_config_path),
    ]

    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve()
        if path.exists():
            return str(path)

    raise RuntimeError(
        "Kein gueltiger PLCOpenXML-Pfad gefunden. "
        "Setze payload.plcopen_xml_path, rag.xml_path, ENV MSRGUARD_PLCOPEN_XML "
        "oder nutze eine gueltige pipeline config mit export.xml."
    )


@dataclass
class RagIncidentContext:
    correlationId: str = ""
    processName: str = ""
    summary: str = ""
    triggerEvent: str = ""
    lastSkill: str = ""
    xml_path: str = ""
    project_root: str = ""
    event: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_input(obj: Dict[str, Any], *, xml_path: str) -> "RagIncidentContext":
        event = _normalize_event_input(obj)
        payload = _get_payload(event)
        xml_resolved = Path(xml_path).expanduser().resolve()
        return RagIncidentContext(
            correlationId=_pick(payload, ["correlationId", "corr", "correlation_id"]),
            processName=_pick(payload, ["processName", "process", "lastProcessName"]),
            summary=_pick(payload, ["summary", "summary_text", "message", "error"]),
            triggerEvent=_pick(payload, ["triggerEvent", "event", "trigger"]),
            lastSkill=_pick(payload, ["lastSkill", "lastSkillName", "interruptedSkill", "interruptedSkillName"]),
            xml_path=str(xml_resolved),
            project_root=str(xml_resolved.parent),
            event=event,
        )

    @staticmethod
    def from_event(event: Dict[str, Any], *, xml_path: str) -> "RagIncidentContext":
        return RagIncidentContext.from_input(event, xml_path=xml_path)

    def compact_event_context(self) -> Dict[str, Any]:
        event = _as_dict(self.event)
        payload = _get_payload(event)
        compact: Dict[str, Any] = {
            "correlationId": self.correlationId,
            "triggerEvent": self.triggerEvent,
            "processName": self.processName,
            "summary": self.summary,
            "lastSkill": self.lastSkill,
        }
        for key in ("ingestion", "plcSnapshot"):
            value = payload.get(key)
            if value is not None:
                compact[key] = value
        return compact

    def runtime_context_note(self) -> str:
        payload = _get_payload(_as_dict(self.event))
        snapshot = _as_dict(payload.get("plcSnapshot"))
        vars_rows = _as_list(snapshot.get("vars"))
        browse_rows = _as_list(snapshot.get("rows"))

        observed_vars: List[str] = []
        for row in vars_rows[:12]:
            if not isinstance(row, dict):
                continue
            var_id = str(row.get("id") or "-")
            var_t = str(row.get("t") or "-")
            value = row.get("v")
            observed_vars.append(f"- {var_id} = {value!r} (typ={var_t})")

        note_parts = [
            "Interpretation des Event-/Snapshot-Kontexts:",
            "- triggerEvent ist das ausloesende Diagnose-/Fehler-Event.",
            "- processName und lastSkill beschreiben den zuletzt aktiven fachlichen Ablauf.",
            "- summary ist eine externe Kurzbeschreibung und kann unvollstaendig oder ungenau sein.",
            "- ingestion enthaelt nur Metadaten zur vorgeschalteten Ingestion, keine PLC-Logik.",
            "- plcSnapshot.vars sind zur Diagnosezeit ueber OPC UA gelesene Laufzeitwerte von PLC-Variablen.",
            "- In plcSnapshot.vars bedeutet 'id' der Variablenname bzw. OPC-UA-Knoten, 't' der Datentyp und 'v' der beobachtete Wert.",
            "- plcSnapshot.rows beschreibt zusatzlich im OPC-UA-Adressraum gefundene Knoten und Typinformationen; das sind Strukturhinweise, nicht zwingend aktuelle Werte.",
            "- Die PLCOpenXML ist der statische Programmkontext. Verbinde deshalb beobachtete Laufzeitwerte aus dem JSON vorsichtig mit POU/GVL/DUT/FB-Kontext aus der PLCOpenXML.",
        ]

        if observed_vars:
            note_parts.append("")
            note_parts.append("Beobachtete Beispielwerte aus plcSnapshot.vars:")
            note_parts.extend(observed_vars)

        if browse_rows:
            note_parts.append("")
            note_parts.append(
                f"Es liegen ausserdem {len(browse_rows)} browse rows in plcSnapshot.rows vor."
            )

        return "\n".join(note_parts)

    def default_question(self) -> str:
        err = self.summary or self.triggerEvent or "dieser Fehler"
        return (
            f"Analysiere bitte, warum das Fehlerbild '{err}' aufgetreten sein koennte. "
            "Nutze die PLCOpenXML als statischen Programmkontext und den Event-/Snapshot-Kontext als beobachteten Laufzeitkontext. "
            "Die Werte in plcSnapshot.vars sind ueber OPC UA gelesene Variablenwerte zum Diagnosezeitpunkt. "
            "Ordne diese Werte moeglichst passenden Variablen, POU, GVL, DUT oder Funktionsbausteinen in der PLCOpenXML zu. "
            "Erklaere eine technisch plausible Ursache, nenne die wichtigsten beteiligten Variablen oder Programmteile und gib eine vorsichtige Empfehlung, "
            "wie man den Fehler eingrenzen oder beheben koennte. Wenn der Kontext nicht reicht, sage klar, was fehlt."
        )


@dataclass
class RagChunk:
    chunk_id: str
    kind: str
    name: str
    text: str
    metadata: Dict[str, Any]

    def preview(self, max_chars: int = 320) -> str:
        text = re.sub(r"\s+", " ", self.text).strip()
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + " ..."


class PLCOpenXmlSource:
    @staticmethod
    def load_text(xml_path: str) -> str:
        path = Path(xml_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"PLCOpenXML nicht gefunden: {path}")
        return path.read_text(encoding="utf-8", errors="replace")


class XmlChunker:
    _interesting_tags = {
        "project",
        "configuration",
        "resource",
        "task",
        "pou",
        "program",
        "functionblock",
        "function",
        "gvl",
        "globalvars",
        "globalvarlists",
        "datatype",
        "dut",
        "transition",
        "action",
    }

    def __init__(self, max_chunk_chars: int = 3200, overlap_chars: int = 240):
        self.max_chunk_chars = max(800, int(max_chunk_chars))
        self.overlap_chars = max(80, int(overlap_chars))

    @staticmethod
    def _local_name(tag: str) -> str:
        if "}" in tag:
            return tag.rsplit("}", 1)[-1]
        return tag

    @staticmethod
    def _elem_name(elem: ET.Element, fallback: str) -> str:
        for key in ("name", "Name"):
            value = elem.attrib.get(key)
            if value:
                return str(value)
        for child in list(elem):
            child_tag = XmlChunker._local_name(child.tag).lower()
            if child_tag == "name":
                text = "".join(child.itertext()).strip()
                if text:
                    return text
        return fallback

    def _split_large_text(self, text: str) -> List[str]:
        text = text.strip()
        if not text:
            return []
        if len(text) <= self.max_chunk_chars:
            return [text]

        parts: List[str] = []
        start = 0
        step = self.max_chunk_chars - self.overlap_chars
        while start < len(text):
            end = min(len(text), start + self.max_chunk_chars)
            parts.append(text[start:end].strip())
            if end >= len(text):
                break
            start += max(1, step)
        return parts

    def _fallback_windows(self, text: str, xml_path: str) -> List[RagChunk]:
        chunks: List[RagChunk] = []
        for idx, part in enumerate(self._split_large_text(text), start=1):
            chunks.append(
                RagChunk(
                    chunk_id=f"xml-window-{idx}",
                    kind="XML_WINDOW",
                    name=f"window_{idx}",
                    text=part,
                    metadata={"xml_path": xml_path, "fallback": True, "part": idx},
                )
            )
        return chunks

    def chunk_text(self, text: str, *, xml_path: str) -> List[RagChunk]:
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return self._fallback_windows(text, xml_path)

        chunks: List[RagChunk] = []
        seen: set[tuple[str, str, str]] = set()

        for elem in root.iter():
            tag = self._local_name(elem.tag)
            tag_lower = tag.lower()
            if tag_lower not in self._interesting_tags:
                continue

            serialized = ET.tostring(elem, encoding="unicode", method="xml").strip()
            serialized = re.sub(r">\s+<", ">\n<", serialized)
            if len(serialized) < 80:
                continue

            elem_name = self._elem_name(elem, fallback=tag)
            parts = self._split_large_text(serialized)
            for part_idx, part in enumerate(parts, start=1):
                identity = (tag_lower, elem_name, part[:180])
                if identity in seen:
                    continue
                seen.add(identity)
                suffix = f"-part{part_idx}" if len(parts) > 1 else ""
                chunks.append(
                    RagChunk(
                        chunk_id=f"{tag_lower}:{elem_name}{suffix}",
                        kind=tag.upper(),
                        name=elem_name,
                        text=part,
                        metadata={"xml_path": xml_path, "tag": tag, "part": part_idx},
                    )
                )

        if chunks:
            return chunks
        return self._fallback_windows(text, xml_path)


class SimpleRagIndex:
    _token_re = re.compile(r"[A-Za-z_][A-Za-z0-9_\.]*|\d+")

    def __init__(self, chunks: List[RagChunk]):
        self.chunks = chunks
        self._chunk_tokens: List[Counter[str]] = [Counter(self._tokenize(c.name + " " + c.text)) for c in chunks]

    @classmethod
    def _tokenize(cls, text: str) -> List[str]:
        return [tok.lower() for tok in cls._token_re.findall(text or "")]

    def search(self, question: str, k: int = 6) -> List[Dict[str, Any]]:
        q_counter = Counter(self._tokenize(question))
        if not q_counter:
            return []

        hits: List[Dict[str, Any]] = []
        q_tokens = set(q_counter.keys())
        q_text = " ".join(q_counter.keys())

        for idx, chunk in enumerate(self.chunks):
            c_counter = self._chunk_tokens[idx]
            overlap = q_tokens & set(c_counter.keys())
            if not overlap:
                continue

            score = 0.0
            for token in overlap:
                weight = 4.0 if token in (chunk.name or "").lower() else 1.0
                score += min(c_counter[token], 5) * weight

            compact = re.sub(r"\s+", " ", chunk.text).lower()
            if q_text and q_text in compact:
                score += 6.0
            if chunk.kind.lower().startswith("pou") and any(tok in chunk.name.lower() for tok in overlap):
                score += 2.5

            hits.append(
                {
                    "score": round(score, 3),
                    "chunk": chunk,
                    "overlap": sorted(overlap),
                }
            )

        hits.sort(key=lambda item: (item["score"], len(item["overlap"]), len(item["chunk"].text)), reverse=True)
        return hits[: max(1, int(k))]


class SimpleRagAgent:
    def __init__(
        self,
        *,
        xml_path: str,
        openai_model: str = "gpt-4o-mini",
        openai_temperature: float = 0.0,
        top_k: int = 6,
    ):
        self.xml_path = str(Path(xml_path).expanduser().resolve())
        self.top_k = max(1, int(top_k))
        self.llm = get_llm_invoke(model=openai_model, temperature=openai_temperature)

        source = PLCOpenXmlSource()
        xml_text = source.load_text(self.xml_path)
        self.chunks = XmlChunker().chunk_text(xml_text, xml_path=self.xml_path)
        self.index = SimpleRagIndex(self.chunks)

    def debug_summary(self) -> Dict[str, Any]:
        sample = [
            {
                "chunk_id": chunk.chunk_id,
                "kind": chunk.kind,
                "name": chunk.name,
            }
            for chunk in self.chunks[:8]
        ]
        return {
            "xml_path": self.xml_path,
            "chunk_count": len(self.chunks),
            "sample_chunks": sample,
        }

    def ask(self, question: str, ctx: RagIncidentContext, *, extra_context: str = "") -> Dict[str, Any]:
        question = str(question or "").strip()
        if not question:
            return {"answer": "Bitte stelle eine Frage zur PLCOpenXML.", "retrieved_chunks": []}

        hits = self.index.search(question, k=self.top_k)
        if not hits:
            return {
                "answer": "Ich habe in der PLCOpenXML keine passenden Kontextstellen gefunden.",
                "retrieved_chunks": [],
            }

        context_blocks: List[str] = []
        retrieved_chunks: List[Dict[str, Any]] = []
        for pos, hit in enumerate(hits, start=1):
            chunk = hit["chunk"]
            retrieved_chunks.append(
                {
                    "rank": pos,
                    "score": hit["score"],
                    "kind": chunk.kind,
                    "name": chunk.name,
                    "chunk_id": chunk.chunk_id,
                    "overlap": hit["overlap"],
                    "preview": chunk.preview(),
                }
            )
            context_blocks.append(
                f"[Chunk {pos}] kind={chunk.kind} name={chunk.name} score={hit['score']}\n"
                f"{chunk.text[:4200]}"
            )

        system = (
            "Du bist ein bewusst einfacher RAG-Agent fuer PLCOpenXML. "
            "Du darfst nur mit dem gegebenen Kontext antworten. "
            "Deine Aufgabe ist, statischen PLCOpenXML-Kontext mit einem kleinen Laufzeit-Snapshot zu verbinden. "
            "Interpretiere plcSnapshot.vars als ueber OPC UA gelesene Laufzeitwerte von PLC-Variablen zum Diagnosezeitpunkt. "
            "Interpretiere plcSnapshot.rows nur als Struktur- und Typinformationen aus dem OPC-UA-Adressraum. "
            "Trenne sauber zwischen Beobachtungen aus dem JSON, Struktur aus der PLCOpenXML und deinen vorsichtigen Schlussfolgerungen. "
            "Wenn Informationen fehlen, sage das klar. "
            "Keine Root-Cause erfinden, keine Datenfluesse behaupten, die nicht im Kontext stehen. "
            "Antworte auf Deutsch und nenne konkrete POU/GVL/DUT/FB-/Variablennamen, wenn vorhanden."
        )
        user = (
            "Hinweise zur Interpretation des JSON-Kontexts:\n"
            "- plcSnapshot.vars: ueber OPC UA gelesene Variablenwerte; id=Name, t=Datentyp, v=Wert.\n"
            "- plcSnapshot.rows: im OPC-UA-Adressraum gefundene Knoten, Typen und Methoden; eher Strukturhinweise als aktuelle Werte.\n"
            "- summary: externe Kurzbeschreibung, moeglicherweise unvollstaendig.\n"
            "- ingestion: nur Metadaten zum Ingestion-Lauf.\n\n"
            f"Kontext zum Event:\n"
            f"- triggerEvent: {ctx.triggerEvent or '-'}\n"
            f"- processName: {ctx.processName or '-'}\n"
            f"- lastSkill: {ctx.lastSkill or '-'}\n"
            f"- summary: {ctx.summary or '-'}\n"
            f"- xml_path: {ctx.xml_path}\n\n"
            + (f"Zusatzkontext (JSON):\n{extra_context}\n\n" if extra_context else "")
            +
            f"Gefundene PLCOpenXML-Kontextstellen:\n\n"
            f"{chr(10).join(context_blocks)}\n\n"
            f"Frage:\n{question}\n\n"
            "Antwortformat:\n"
            "1) Direkte technische Einordnung in 3-7 Saetzen.\n"
            "2) Danach: 'Relevante Laufzeitwerte:' mit den wichtigsten JSON-Beobachtungen.\n"
            "3) Danach: 'Relevanter Codekontext:' mit den wichtigsten POU/GVL/DUT/FB-/Chunk-Namen.\n"
            "4) Danach: 'Unsicherheit / naechster Check:' und die wichtigste verbleibende Luecke oder ein vorsichtiger naechster Pruefschritt."
        )
        answer = self.llm(system, user)
        return {
            "answer": answer,
            "retrieved_chunks": retrieved_chunks,
            "debug": {"question": question, "xml_path": self.xml_path},
        }


class SimpleRagSession:
    def __init__(self, agent: SimpleRagAgent, ctx: RagIncidentContext):
        self.agent = agent
        self.ctx = ctx
        self.history: List[Dict[str, Any]] = []

    def debug_summary(self) -> Dict[str, Any]:
        summary = self.agent.debug_summary()
        summary["context"] = {
            "correlationId": self.ctx.correlationId,
            "triggerEvent": self.ctx.triggerEvent,
            "processName": self.ctx.processName,
            "lastSkill": self.ctx.lastSkill,
        }
        return summary

    def ask(self, user_msg: str, debug: bool = True) -> Dict[str, Any]:
        result = self.agent.ask(user_msg, self.ctx)
        response = {
            "answer": result.get("answer", ""),
            "retrieved_chunks": result.get("retrieved_chunks", []) if debug else None,
            "debug": result.get("debug", {}) if debug else None,
        }
        self.history.append({"user": user_msg, "resp": response})
        return response

    def build_default_prompt(self) -> Dict[str, str]:
        compact = self.ctx.compact_event_context()
        compact_json = _shorten_json(compact, max_chars=3200)
        question = self.ctx.default_question()
        context_note = self.ctx.runtime_context_note()
        extra_context = context_note + "\n\nEvent-/Snapshot-Kontext (JSON):\n" + compact_json
        user_message = question + "\n\n" + extra_context
        return {
            "question": question,
            "user_message": user_message,
            "context_json": extra_context,
        }

    def run_initial_analysis(self, debug: bool = True) -> Dict[str, Any]:
        prompt = self.build_default_prompt()
        result = self.agent.ask(
            prompt["question"],
            self.ctx,
            extra_context=prompt["context_json"],
        )
        response = {
            "default_question": prompt["user_message"],
            "answer": result.get("answer", ""),
            "retrieved_chunks": result.get("retrieved_chunks", []) if debug else None,
            "debug": result.get("debug", {}) if debug else None,
        }
        self.history.append({"user": prompt["user_message"], "resp": response})
        return response


def build_session_from_input(
    obj: Dict[str, Any],
    *,
    xml_path: str = "",
    pipeline_config_path: str = "",
    openai_model: str = "gpt-4o-mini",
    openai_temperature: float = 0.0,
    top_k: int = 6,
) -> SimpleRagSession:
    resolved_xml = _resolve_xml_path(
        obj,
        explicit_xml_path=xml_path,
        pipeline_config_path=pipeline_config_path,
    )
    ctx = RagIncidentContext.from_input(obj, xml_path=resolved_xml)
    agent = SimpleRagAgent(
        xml_path=resolved_xml,
        openai_model=openai_model,
        openai_temperature=openai_temperature,
        top_k=top_k,
    )
    return SimpleRagSession(agent=agent, ctx=ctx)
