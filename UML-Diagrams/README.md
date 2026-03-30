# UML-Diagrams

Dieser Ordner enthaelt neue, konsolidierte PlantUML-Diagramme fuer das Gesamtprojekt.

## Dateien
- `project_class_diagram.puml`: Gesamtueberblick ueber Hauptklassen/Module (C++, Python-Agent, Ingestion).
- `msrguard_agent_class_diagram.plantuml`: Gesamtgrafik der Python-Agentenkomponenten im Paket `msrguard`.
- `msrguard_rag_agent_class_diagram.plantuml`: Klassendiagramm des neuen minimalen RAG-Agenten mit UI, Session, Chunking und Retrieval.
- `msrguard_rag_agent_overview_slide.plantuml`: Folienfreundliche Uebersicht ueber UI, Session, RAG-Core und externe Inputs.
- `msrguard_rag_ui_session_slide.plantuml`: Folienfreundliche Sicht auf UI-Startpfad, Config und Session-Aufbau.
- `msrguard_rag_retrieval_slide.plantuml`: Folienfreundliche Sicht auf XML-Laden, Chunking, Retrieval und LLM-Aufruf.
- `msrguard_rag_context_prompt_slide.plantuml`: Folienfreundliche Sicht auf Event-/Snapshot-Kontext, Promptaufbau und Initialanalyse.
- `msrguard_agent_overview_slide.plantuml`: Folienfreundliche Uebersicht ueber UI, Core, Session und Bot-Orchestrierung.
- `msrguard_chatbot_core_tools_slide.plantuml`: Folienfreundliche Sicht auf KG-, Registry- und allgemeine Tool-Komponenten.
- `msrguard_evd2_trace_slide.plantuml`: Folienfreundliche Sicht auf evD2-Bootstrap, Trace-Tools und `d2_trace_analysis`.
- `evd2_sequence_diagram.puml`: Sequenzdiagramm fuer den typischen evD2-Fehlerfall.

## Rendern (Beispiel)
- PlantUML CLI: `plantuml project_class_diagram.puml evd2_sequence_diagram.puml msrguard_agent_class_diagram.plantuml msrguard_rag_agent_class_diagram.plantuml msrguard_rag_agent_overview_slide.plantuml msrguard_rag_ui_session_slide.plantuml msrguard_rag_retrieval_slide.plantuml msrguard_rag_context_prompt_slide.plantuml msrguard_agent_overview_slide.plantuml msrguard_chatbot_core_tools_slide.plantuml msrguard_evd2_trace_slide.plantuml`
- VS Code: PlantUML Extension + Preview.
