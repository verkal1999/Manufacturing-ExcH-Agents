# UML-Diagrams

Dieser Ordner enthaelt alle PlantUML-Diagramme des Projekts, gegliedert nach Diagrammart und Agent.

## Struktur

```
UML-Diagrams/
├── project_class_diagram.puml          # Gesamtueberblick Klassen (C++, Python, Ingestion)
├── ExcH-KG-Agent/                      # Diagramme fuer den KG-basierten ExcH-Agenten
├── ExcH-RAG-Agent/                     # Diagramme fuer den einfachen RAG-Agenten
└── Use_Cases/                          # Use-Case-Diagramme nach Engineering- und Runtime-Szenarien
```

---

## ExcH-KG-Agent (`ExcH-KG-Agent/`)

Diagramme fuer den toolbasierten KG-Diagnosagenten.

| Datei | Inhalt |
| --- | --- |
| `msrguard_agent_class_diagram.plantuml` | Gesamtgrafik der Python-Agentenkomponenten im Paket `msrguard` |
| `msrguard_agent_overview_slide.plantuml` | Folienueberblick ueber UI, Core, Session und Bot-Orchestrierung |
| `msrguard_chatbot_core_tools_slide.plantuml` | KG-, Registry- und Tool-Komponenten |
| `msrguard_evd2_trace_slide.plantuml` | evD2-Bootstrap, Trace-Tools und `d2_trace_analysis` |
| `evd2_sequence_diagram.puml` | Sequenzdiagramm fuer den typischen evD2-Fehlerfall |
| `Comp_Diag.plantuml` | Komponentendiagramm des ExcH-KG-Agenten (Gesamtsicht) |

### Komponenten-Unterdiagramme (`Komponenten_ExcH-KG-Agent/`)

Detaillierte Komponentendiagramme je Teilbereich:

| Datei | Inhalt |
| --- | --- |
| `01_artefakt_und_ingestion_komponente.plantuml` | Artefakt-Quellen und Ingestion-Pipeline |
| `02_ontologie_und_wissensgraph_komponente.plantuml` | Ontologie, KG-Loader und RDF-Graph |
| `03_kontext_und_sitzungs_komponente.plantuml` | IncidentContext, Session und Event-Extraktion |
| `04_modell_planner_und_werkzeug_komponente.plantuml` | LLM-Planner, Tool-Registry und Tools |
| `05_interaktions_und_ausgabe_komponente.plantuml` | UI, Chat, Ergebnis-JSON und Operator-Interaktion |

---

## ExcH-RAG-Agent (`ExcH-RAG-Agent/`)

Diagramme fuer den einfachen, XML-basierten RAG-Agenten.

| Datei | Inhalt |
| --- | --- |
| `msrguard_rag_agent_class_diagram.plantuml` | Klassendiagramm mit UI, Session, Chunking und Retrieval |
| `msrguard_rag_agent_overview_slide.plantuml` | Folienueberblick ueber UI, Session, RAG-Core und externe Inputs |
| `msrguard_rag_ui_session_slide.plantuml` | UI-Startpfad, Config und Session-Aufbau |
| `msrguard_rag_retrieval_slide.plantuml` | XML-Laden, Chunking, Retrieval und LLM-Aufruf |
| `msrguard_rag_context_prompt_slide.plantuml` | Event-/Snapshot-Kontext, Promptaufbau und Initialanalyse |
| `msrguard_rag_component_overview_a4.plantuml` | Komponentenueberblick (A4-Format) |
| `rag_initial_diagnosis_sequence_diagram.puml` | Sequenzdiagramm fuer die initiale RAG-Diagnose |

### Komponenten-Unterdiagramme (`Komponenten_ExcH-RAG-Agent/`)

| Datei | Inhalt |
| --- | --- |
| `01_foundation_modell_komponente.plantuml` | LLM-Anbindung und Modellkonfiguration |
| `02_kontext_komponente.plantuml` | RagIncidentContext und Event-Extraktion |
| `03_artefakt_und_dokumenten_komponente.plantuml` | PLCOpenXML-Quelle, Chunker und RagChunk |
| `04_retrieval_komponente.plantuml` | SimpleRagIndex und lexikalisches Retrieval |
| `05_generierungs_und_leitplanken_komponente.plantuml` | Promptaufbau, LLM-Aufruf und Guardrails |
| `06_interaktions_und_sitzungs_komponente.plantuml` | SimpleRagSession, Chat und UI |

---

## Use Cases (`Use_Cases/`)

Use-Case-Diagramme gegliedert nach Szenario.

### Engineering-Szenarien (`Use_Cases/Engineering/`)

| Datei | Inhalt |
| --- | --- |
| `UC_Ekomplett.plantuml` | Gesamtuebersicht aller Engineering Use Cases |
| `UC_E1.plantuml` | UC E1 |
| `UC_E2.plantuml` | UC E2 |
| `UC_E4.plantuml` | UC E4 |
| `UC_E5.plantuml` | UC E5 |
| `UC_E6.plantuml` | UC E6 |

### Runtime-Szenarien (`Use_Cases/Runtime/`)

| Datei | Inhalt |
| --- | --- |
| `UC_R1.plantuml` | UC R1 |
| `UC_R2.plantuml` | UC R2 |
| `UC_R3.plantuml` | UC R3 |
| `UC_R4.plantuml` | UC R4 |
| `UC_R5.plantuml` | UC R5 |

---

## Rendern

PlantUML CLI (Beispiel fuer einen Unterordner):
```bash
plantuml UML-Diagrams/ExcH-KG-Agent/*.plantuml UML-Diagrams/ExcH-KG-Agent/*.puml
plantuml UML-Diagrams/ExcH-RAG-Agent/*.plantuml UML-Diagrams/ExcH-RAG-Agent/*.puml
plantuml UML-Diagrams/Use_Cases/Engineering/*.plantuml
plantuml UML-Diagrams/Use_Cases/Runtime/*.plantuml
```

VS Code: PlantUML Extension + Preview (Shortcut: `Alt+D`).

Generierte PNG/SVG-Dateien landen im `out/`-Verzeichnis (gitignored).
