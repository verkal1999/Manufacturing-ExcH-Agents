# MA_Python_Agent

Dieses Repository enthaelt den Python-Agenten und die zugehoerigen Datenpipelines fuer das MSRGuard-Exception-Handling im Gesamtsystem.

## Ziel des Agenten im Framework
Der Agent ist die "Human-in-the-loop"-Diagnoseebene fuer den Fall, dass die C++-Runtime keinen eindeutigen Failure Mode bestimmen kann.

Konkret:
- Die Runtime erkennt einen Fehler-Trigger (insbesondere `evD2` via `OPCUA.TriggerD2`).
- Wenn KG-Kandidaten fehlen oder mehrdeutig sind, wird nicht blind reagiert, sondern der Agent gestartet.
- Der Agent rekonstruiert den softwareseitigen Ursachepfad (Setter -> Bedingungen -> Skill-/GEMMA-Kontext), bereitet ihn fuer den Operator auf und liefert ein strukturiertes Ergebnis (`continue`/`abort` + Diagnosekontext) an die Runtime zurueck.

Damit ist das Hauptziel: **sichere, nachvollziehbare Entscheidungsunterstuetzung statt unkontrollierter Automatik bei unklaren Fehlerlagen**.

## Systemkontext in 30 Sekunden
1. C++-Runtime (`MSRGuard_Anpassung`) ueberwacht OPC UA und erzeugt Events.
2. Ingestion-Pipeline (`Pipelines/IngestionPipeline`) baut/aktualisiert den Knowledge Graph (KG) aus TwinCAT-/PLCopen-Artefakten.
3. Python-Agent (`MSRGuard_Anpassung/python/msrguard`) nutzt den KG fuer Diagnose und Chatbot-Erklaerung.
4. Streamlit-UI ist die Bedienoberflaeche fuer Analyse, Nachfragen und die finale Continue/Abort-Entscheidung.

## End-to-End Ablauf (evD2-Fall)
Der praktische Lauf im Code sieht so aus:

1. `PLCMonitor` sieht eine steigende Flanke auf `OPCUA.TriggerD2`.
2. `main.cpp` erzeugt Snapshot + Korrelation und postet `evD2` auf den `EventBus`.
3. `ReactionManager` versucht KG-basierte Zuordnung:
   - bei eindeutigem Winner: direkte Reaktionskette (Monitoring/SystemReaction),
   - bei 0 oder >1 Winnern: `evUnknownFM` + Fallback-Plan wird nur zwischengespeichert.
4. `AgentStartCoordinator` emittiert `evAgentStart`, sobald beide Signale fuer den Incident da sind:
   - `evUnknownFM`
   - `evIngestionDone`
5. `ExcHUiObserver` schreibt `<corr>_event.json`, startet `excH_kg_agent_ui.py` und wartet auf `<corr>_result.json`.
6. UI/Agent-Seite:
   - optional Pipeline-Lauf,
   - `excH_kg_agent_core.handle_event(...)` liefert strukturierte Basisdiagnose,
   - Chatbot initialisiert KG-Tools und startet initiale evD2-Analyse.
7. Operator entscheidet in der UI "Weiter" oder "Abbrechen":
   - Ergebnis wird als `out_json` persistiert.
8. C++ liest das Ergebnis:
   - `evAgentDone` (Weiter),
   - `evAgentAbort` (Abbruch),
   - `evAgentFail` (Fehler beim Agentlauf).
9. Bei `evAgentDone` wird der vorher geparkte Fallback-Plan (DiagnoseFinished-Puls) kontrolliert ausgefuehrt.

## Was der Python-Agent genau macht
### 1) Input normalisieren
`excH_chatbot.IncidentContext` extrahiert robust Felder aus Event/Snapshot:
- `correlationId`, `processName`, `summary`, `triggerEvent`
- `lastSkill` (u. a. aus `OPCUA.lastExecutedSkill`)
- `lastGEMMAStateBeforeFailure`
- `triggerD2` Status
- KG-Pfad (`kg_ttl_path`)

### 2) Deterministische evD2-Kernanalyse
`build_evd2_diagnoseplan(...)` kombiniert vier Tools:
- `evd2_diagnoseplan`: Trigger-Setter, GEMMA-Port/Call-Infos, Skill-Setter
- `evd2_unified_trace`: tiefe Bedingungs-/Wahrheitspfad-Analyse
- `evd2_requirement_paths`: tabellarische Pfadbedingungen
- `evd2_path_trace`: zusammengefuehrte Punkt-1/2/3 Hauptanalyse mit Zeitphasen (`t0`, `t-1`, `t-2`)

Ergebnis ist ein konsistentes JSON-Buendel (`evd2_compact_v1`) fuer reproduzierbare Erklaerungen.

### 3) LLM-gestuetzte Erklaerung auf Basis harter Fakten
Der Chatbot plant Toolaufrufe, fuehrt sie aus und formuliert Antworten auf Deutsch.
Wichtig: Root-Cause-Modus priorisiert `evd2_path_trace`, damit die Antwort auf einer einheitlichen Faktengrundlage bleibt.

### 4) Ergebnis zur Runtime
Die UI schreibt ein Result-JSON mit u. a.:
- `continue` / `reason`
- `agent_result`
- `chatbot_transcript`
- Pipeline-Status (inkl. stdout/stderr tails)
- Event-Context

## Ingestion-Pipeline (TwinCAT -> KG)
`Pipelines/IngestionPipeline/ingestion_pipeline.py` orchestriert die Schritte:

1. Parser initialisieren
2. Projekt scannen (`*_objects.json`)
3. PLCopen XML exportieren (`export.xml`)
4. Program-I/O Mapping bauen
5. Mapping mit Typen/Code anreichern
6. Variable-Traces erzeugen
7. GVL-Globals erzeugen
8. I/O-HW-Mappings erzeugen
9. Traces mit HW markieren
10. KG laden (`kg_loader.py`)
11. KG semantisch analysieren/anreichern (`kg_manager_new.py`)

Typische KG-Artefakte stehen danach in den konfigurierten TTL-Pfaden (`kg_after_loader_path`, `kg_final_path`).

## Zentrale Verzeichnisse
- `MSRGuard_Anpassung/`: C++-Runtime + Python-Agent + KGs
- `MSRGuard_Anpassung/python/msrguard/`: Agent-Kern, Chatbot, evD2-Trace-Logik, UI
- `Pipelines/IngestionPipeline/`: KG-Aufbaupipeline
- `UML-Diagrams/`: konsolidierte Architektur- und Sequenzdiagramme
- `Notebooks/`: Experiment-/Prototyping-Umgebung

## Relevante Einstiege
- Runtime: `MSRGuard_Anpassung/src/main.cpp`
- UI/Agent (KG): `MSRGuard_Anpassung/python/msrguard/excH_kg_agent_ui.py`
- UI/Agent (RAG): `MSRGuard_Anpassung/python/msrguard/rag_agent_ui.py`
- Chatbot-Engine: `MSRGuard_Anpassung/python/msrguard/chatbot_core.py`
- evD2-Trace-Logik: `MSRGuard_Anpassung/python/msrguard/d2_trace_analysis.py`
- Ingestion-Runner: `Pipelines/IngestionPipeline/run_ingestion.py`

## Startbeispiele
### Ingestion lokal
```powershell
python Pipelines/IngestionPipeline/run_ingestion.py
```

### Agent-UI mit Event-Datei
```powershell
streamlit run MSRGuard_Anpassung/python/msrguard/excH_kg_agent_ui.py -- --event_json_path <pfad_zum_event.json> --out_json <pfad_zum_result.json>
```

RAG-Agent:
```powershell
streamlit run MSRGuard_Anpassung/python/msrguard/rag_agent_ui.py -- --event_json_path <pfad_zum_event.json>
```

## Bekannte Grenzen (Stand im aktuellen Code)
- `excH_kg_agent_core.py` ist absichtlich MVP-lastig (strukturierte Basisantwort, noch keine tiefe Auto-Aktionsplanung).
- Teile von `KG_Interface.py` enthalten feste Dateipfade und Legacy-Zugriffe.
- Die hohe Diagnosequalitaet haengt stark von der Aktualitaet und Vollstaendigkeit des KG aus der Ingestion ab.
