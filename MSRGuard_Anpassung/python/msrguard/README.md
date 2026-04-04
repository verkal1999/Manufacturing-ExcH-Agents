# msrguard

Dieses Paket enthaelt die Python-Seite von MSRGuard. Im Kern liegen hier zwei unterschiedliche Agentenansaetze:

- der `ExcH`-Agent fuer wissensgraph- und toolgestuetzte Diagnose
- der einfache `RAG`-Agent fuer PLCOpenXML-basierte Analyse
- einige Legacy- bzw. Hilfsmodule, die historisch mitgewachsen sind

Die beiden Agenten loesen aehnliche Aufgaben, sind aber intern sehr verschieden aufgebaut:

- Der `ExcH`-Agent arbeitet mit einer Tool-Registry, einem Planner, einem RDF-Knowledge-Graph und mehreren spezialisierten Analysewerkzeugen.
- Der `RAG`-Agent ist absichtlich klein gehalten: XML laden, in Chunks zerlegen, lexikalisch relevante Stellen suchen, eine LLM-Antwort erzeugen.


## Schnellstart

Beispiele aus `MSRGuard_Anpassung/python`:

```bash
streamlit run msrguard/excH_agent_ui.py -- --event_json_path <event.json>
streamlit run msrguard/rag_agent_ui.py -- --event_json_path <event.json>
```

Wichtige Voraussetzungen:

- `OPENAI_API_KEY` muss gesetzt sein oder ueber die jeweilige `*_agent_config.json` geladen werden koennen.
- Fuer den `ExcH`-Agenten muss ein gueltiger KG-Pfad vorhanden sein, entweder im Event (`payload.kg_ttl_path`) oder ueber `MSRGUARD_KG_TTL`.
- Fuer den `RAG`-Agenten muss eine gueltige `export.xml` gefunden werden, entweder im Event, in `rag_agent_config.json`, ueber `MSRGUARD_PLCOPEN_XML` oder ueber die Pipeline-Konfiguration.


## Dateien Und Verantwortung

| Datei | Rolle | Was die Datei konkret macht |
| --- | --- | --- |
| `chatbot_core.py` | Kernlogik des KG-Agenten | Baut den KG-Zugriff auf, registriert alle Tools, enthaelt den Planner, fuehrt Toolplaene aus und erzeugt finale Antworten fuer den interaktiven Chat. |
| `d2_trace_analysis.py` | Deterministische Analysebibliothek | Enthaelt die eigentliche ST/FBD-Trace- und Bedingungslogik fuer evD2, Truth Paths, Requirement-Pfade und Rueckverfolgung von Signalen. |
| `excH_chatbot.py` | ExcH-Sitzung und Diagnoseorchestrierung | Extrahiert Event-Kontext, baut die Session, fuehrt fuer evD2 zuerst eine deterministische Bootstrap-Analyse aus und kapselt den Chatbot fuer Folgefragen. |
| `excH_agent_ui.py` | Streamlit-Oberflaeche fuer den ExcH-Agenten | Laedt Event-JSON, startet optional die Ingestion-Pipeline, initialisiert den KG-Chatbot, zeigt Chatverlauf und Debug-Informationen an und schreibt Ergebnis-JSONs weg. |
| `excH_agent_core.py` | Minimaler MVP-Core | Stellt mit `handle_event(...)` einen sehr kleinen, nicht-chatbasierten Einstiegspunkt fuer strukturierte Agent-Ergebnisse bereit. |
| `simple_rag_agent.py` | Gesamte RAG-Logik | Enthaelt Kontextmodell, XML-Loader, Chunking, Retrieval, Agent, Session und Session-Builder fuer den einfachen RAG-Agenten. |
| `rag_agent_ui.py` | Streamlit-Oberflaeche fuer den RAG-Agenten | Laedt Event-JSON, startet optional die Pipeline fuer `export.xml`, baut die RAG-Session und startet die initiale Analyse. |
| `KG_Interface.py` | Legacy-KG-Zugriff | Aelteres RDF-Interface fuer FailureMode-, MonitoringAction- und SystemReaction-Abfragen sowie Ingestion von aufgetretenen Fehlern. |
| `excH_agent_config.json` | ExcH-UI-Konfiguration | Definiert API-Key-Datei, Pipeline-Pfade und Modellparameter fuer den KG-Chatbot. |
| `rag_agent_config.json` | RAG-UI-Konfiguration | Definiert API-Key-Datei, Pipeline-Pfade sowie XML-, Modell- und Retrieval-Parameter fuer den RAG-Agenten. |
| `__init__.py` | Paketexport | Exportiert aktuell nur `KGInterface` nach aussen. |

Hinweis:

- `__pycache__` ist nur generierter Python-Bytecode und fachlich nicht relevant.


## Agent 1: ExcH-Agent

### Wofuer der Agent gedacht ist

Der `ExcH`-Agent ist der maechtigere der beiden Ansaetze. Er soll:

- Ereignisse aus der Anlage im Kontext des Knowledge Graphs interpretieren
- bei `evD2` moeglichst genau rekonstruieren, warum `OPCUA.TriggerD2` auf `TRUE` gegangen ist
- Call-Chains, Variablen, Setter, Guards und POU-Zusammenhaenge erklaeren
- auf Rueckfragen im Chat mit gezielten Tool-Aufrufen nacharbeiten
- optional konkrete Vermeidungs- oder Aenderungsvorschlaege erzeugen

Kurz gesagt: Dieser Agent ist fuer tiefere Root-Cause-Analyse gedacht, nicht nur fuer eine oberflaechliche Textantwort.


### Aufbau des ExcH-Agenten

Der Laufzeitpfad ist im Normalfall:

1. `excH_agent_ui.py` laedt die UI, das Event und optional die Ingestion-Pipeline.
2. `IncidentContext` in `excH_chatbot.py` extrahiert aus dem Event die fuer die Diagnose wichtigen Felder.
3. `build_bot(...)` in `chatbot_core.py` laedt den KG, baut den Tool-Stack auf und erstellt den ChatBot.
4. `ExcHChatBotSession` kapselt Bot plus Incident-Kontext.
5. `run_initial_analysis(...)` in `excH_chatbot.py` startet zuerst eine initiale Analyse.
6. Bei evD2 wird vor dem eigentlichen Chat eine deterministische Bootstrap-Analyse berechnet.
7. Folgefragen des Nutzers laufen dann ueber den Planner im `ChatBot`.


### Was `excH_chatbot.py` genau macht

Diese Datei ist die Bruecke zwischen UI und dem eigentlichen KG-Chatbot.

Wichtige Bestandteile:

- `IncidentContext`
  Extrahiert `correlationId`, `processName`, `summary`, `triggerEvent`, `lastSkill`, `plcSnapshot`, `lastGEMMAStateBeforeFailure`, `kg_ttl_path` und weitere Metadaten aus dem Event.

- `ExcHChatBotSession`
  Haelt den ChatBot und den Event-Kontext zusammen. Die Session weiss auch, ob bereits ein evD2-Bootstrap-Plan berechnet wurde.

- `build_session_from_input(...)`
  Baut aus einem Event direkt eine komplette ExcH-Session.

- `build_evd2_diagnoseplan(...)`
  Startet eine deterministische Composite-Analyse fuer evD2. Dabei werden mehrere spezialisierte Tools nacheinander aufgerufen:
  `evd2_diagnoseplan`, `evd2_unified_trace`, `evd2_requirement_paths` und `evd2_path_trace`.

- `run_initial_analysis(...)`
  Nutzt den Bootstrap-Plan fuer eine erste Antwort. Wenn ein evD2-Fall erkannt wird, wird moeglichst viel bereits ohne freie Planner-Entscheidung rekonstruiert.

Die Datei ist also nicht nur Prompt-Glue, sondern die eigentliche Sitzungs- und Ablaufsteuerung fuer den ExcH-Agenten.


### Was `chatbot_core.py` genau macht

`chatbot_core.py` ist das zentrale Maschinenhaus des toolbasierten Agenten.

Die Datei kuemmert sich um:

- den globalen KG-Zugriff mit `rdflib`
- Guardrails fuer SPARQL
- Hilfsklassen fuer KG-Signaturen und Routinenindizes
- die Definition aller Agent-Tools
- die Tool-Registry
- den Planner
- die Planausfuehrung
- die finale Antwortsynthese
- den Einstiegspunkt `build_bot(...)`

Wichtige Bausteine:

- `BaseAgentTool`
  Abstrakte Basisklasse fuer alle Tools.

- `ToolRegistry`
  Registriert Tools, erzeugt Tool-Dokumentation fuer den Planner und fuehrt einzelne Tools aus.

- `ChatBot`
  Nimmt eine Nutzerfrage, laesst ein LLM einen Tool-Plan erzeugen, fuehrt diesen aus, probiert bei Bedarf breitere Fallbacks und laesst anschliessend eine finale Antwort aus den Tool-Ergebnissen formulieren.

- `build_bot(...)`
  Baut alles zusammen: KG laden, Routine-Index erzeugen oder laden, LLM vorbereiten, Tools registrieren, optional einfachen Vektorindex fuer semantische Suche anlegen.


### Welche Tool-Gruppen der ExcH-Agent hat

In `chatbot_core.py` liegen viele Werkzeuge. Praktisch lassen sie sich in Gruppen einteilen:

- Struktur- und Codeabfragen
  `list_programs`, `called_pous`, `pou_callers`, `pou_code`

- Variablen- und Symbolsuche
  `search_variables`, `variable_trace`, `general_search`, `string_triple_search`

- Diagnose und Root-Cause-Suche
  `graph_investigate`, `exception_prep`

- evD2-spezifische Tiefenanalyse
  `evd2_diagnoseplan`, `evd2_unified_trace`, `evd2_requirement_paths`, `evd2_path_trace`

- Fallbacks und Zusatzsuche
  `text2sparql_select`

- Optionales semantisches Retrieval
  `semantic_search`, falls die benoetigten Abhaengigkeiten vorhanden sind und ein Vektorindex gebaut werden kann

Wichtig fuer das Verstaendnis:

- Der ExcH-Agent ist kein klassischer "ein Prompt rein, eine Antwort raus"-Chatbot.
- Er ist ein Tool-Agent mit Planner. Das LLM entscheidet also oft zuerst, welche Werkzeuge gebraucht werden.
- Fuer evD2 wird dieser offene Planner-Ansatz beim Start bewusst durch einen deterministischen Bootstrap-Pfad ergaenzt.


### Was `d2_trace_analysis.py` genau macht

Diese Datei enthaelt die eigentliche technische Detailanalyse fuer ST/FBD-Logik. Wenn im ExcH-Agenten "tiefer" diagnostiziert wird, landet viel davon hier.

Die Datei macht unter anderem:

- Parsing und Analyse von ST-Zuweisungen
- Extraktion und Umformung boolescher Bedingungen
- Berechnung von Truth Paths
- Rueckverfolgung von Ports, FB-Instanzen und Variablen
- Rekonstruktion von Requirement-Pfaden
- evD2-spezifische Pfadrekonstruktion ueber GEMMA-/D2-Zustaende

Wenn in der UI spaeter sehr konkrete Aussagen ueber Setter, innere `IF`-Bedingungen, Upstream-Signale oder Zeitphasen `t0`, `t-1`, `t-2` auftauchen, stammen die relevanten Fakten im Kern aus dieser Datei.


### Rolle von `excH_agent_ui.py`

Die UI-Datei ist fuer die Bedienung da, nicht fuer die Fachlogik.

Sie uebernimmt:

- Konfiguration laden
- Event-JSON von Pfad oder Upload einlesen
- optional die Ingestion-Pipeline starten
- OpenAI-Key aus Datei setzen
- Session-State in Streamlit verwalten
- ExcH-Session initialisieren
- Initialantwort anzeigen
- Folgefragen im Chat an die Session weiterreichen
- Debug-Informationen, Tool-Ergebnisse und Ausgabe-JSON anzeigen oder speichern

Wichtig:

- Die Datei baut die Agentenlogik nicht selbst nach.
- Sie ruft dafuer gezielt `IncidentContext`, `build_bot(...)`, `ExcHChatBotSession` und `run_initial_analysis(...)` auf.


### Rolle von `excH_agent_core.py`

`excH_agent_core.py` ist ein sehr kleiner MVP-Einstiegspunkt mit `handle_event(...)`.

Die Datei soll:

- ein Event entgegennehmen
- ein strukturiertes Ergebnisobjekt zurueckgeben
- spaeter ein einfacher Integrationspunkt fuer nicht-interaktive Aufrufe sein

Aktuell ist das eher ein Platzhalter bzw. frueher MVP-Stand:

- nur wenige Event-Typen werden direkt behandelt
- die Antwort ist generisch
- der interaktive ExcH-Chat nutzt in der Praxis stattdessen `excH_chatbot.py` plus `chatbot_core.py`


## Agent 2: RAG-Agent

### Wofuer der Agent gedacht ist

Der RAG-Agent ist die bewusst einfache Alternative. Er soll:

- eine PLCOpenXML als statischen Programmkontext laden
- ein Event oder einen Snapshot als Laufzeitkontext hinzunehmen
- die relevantesten XML-Stellen fuer eine Frage finden
- daraus eine vorsichtige, textuelle Analyse erzeugen

Er soll gerade nicht:

- einen Tool-Plan bauen
- einen KG traversieren
- komplexe agentische Mehrschritt-Orchestrierung machen

Der RAG-Agent ist also eher ein klar nachvollziehbarer Baseline-Agent.


### Aufbau des RAG-Agenten

Der Laufzeitpfad ist im Normalfall:

1. `rag_agent_ui.py` laedt Event, Konfiguration und optional die Pipeline.
2. `build_session_from_input(...)` in `simple_rag_agent.py` bestimmt den XML-Pfad.
3. `RagIncidentContext` extrahiert den Event-Kontext.
4. `SimpleRagAgent` laedt die PLCOpenXML, zerlegt sie in Chunks und baut einen einfachen Suchindex.
5. `SimpleRagSession.run_initial_analysis(...)` formuliert automatisch eine erste Diagnosefrage.
6. Die Antwort entsteht aus genau einem Retrieval-Schritt und genau einem LLM-Aufruf.
7. Rueckfragen laufen ueber dieselbe Session und denselben Retrieval-Mechanismus.


### Was `simple_rag_agent.py` genau macht

Diese Datei enthaelt praktisch den kompletten RAG-Agenten.

Wichtige Bestandteile:

- `RagIncidentContext`
  Speichert den fachlichen Laufzeitkontext des Events, zum Beispiel `correlationId`, `processName`, `summary`, `triggerEvent`, `lastSkill`, `xml_path` und das Original-Event.

- `PLCOpenXmlSource`
  Laedt die XML-Datei als Text.

- `XmlChunker`
  Zerlegt die XML in sinnvolle Chunks. Dabei werden interessante Tags wie `pou`, `gvl`, `datatype`, `configuration`, `task`, `transition` oder `action` bevorzugt. Wenn das XML nicht sauber parsebar ist, wird auf Textfenster als Fallback umgestellt.

- `RagChunk`
  Das Datenmodell fuer einen einzelnen Chunk.

- `SimpleRagIndex`
  Ein einfacher lexikalischer Index. Hier werden keine Embeddings genutzt. Die Suche basiert auf Token-Ueberschneidung und einer einfachen Scoring-Logik.

- `SimpleRagAgent`
  Laedt XML, baut Chunks und Index und fuehrt `ask(...)` aus. In `ask(...)` werden die besten Treffer gesucht, als Kontext an das LLM gegeben und als Antwort zusammengefasst.

- `SimpleRagSession`
  Haelt Agent und Kontext zusammen, speichert den Verlauf und baut eine Default-Frage fuer die erste Analyse.

- `build_session_from_input(...)`
  Baut aus Event plus optionalen Parametern direkt eine komplette Session.


### Wie der XML-Pfad fuer den RAG-Agenten gefunden wird

Die XML wird in `simple_rag_agent.py` nicht blind vorausgesetzt, sondern aus mehreren moeglichen Quellen aufgeloest.

Die Reihenfolge ist:

1. Pfad aus dem Event-Payload, zum Beispiel `plcopen_xml_path`
2. expliziter `xml_path` aus der UI-Konfiguration
3. Umgebungsvariable `MSRGUARD_PLCOPEN_XML`
4. `project_dir/export.xml` aus der Pipeline-Konfiguration

Wenn nichts davon auf eine existierende Datei zeigt, bricht der RAG-Agent bewusst mit einer klaren Fehlermeldung ab.


### Wie das Retrieval im RAG-Agenten funktioniert

Wichtig fuer jeden, der die Datei aendern will:

- Das ist kein Embedding-RAG.
- Das Retrieval ist absichtlich einfach und nachvollziehbar.
- Gesucht wird lexikalisch ueber Tokens aus Frage, Chunk-Name und Chunk-Text.
- Treffer im Namen eines Chunks werden staerker gewichtet als normale Treffer.
- POU-nahe Chunks bekommen zusaetzliche Punkte.

Die Antwortqualitaet haengt deshalb stark davon ab:

- wie gut die XML gechunked wird
- wie gut die Frage die relevanten Begriffe enthaelt
- ob Event- und Snapshot-Kontext sauber in den Prompt eingebettet werden


### Rolle von `rag_agent_ui.py`

Die RAG-UI kuemmert sich um:

- Konfiguration laden
- Event-JSON einlesen
- optional die Pipeline starten, damit `export.xml` aktualisiert wird
- Session-Builder importieren und ausfuehren
- Initialanalyse starten
- Chatverlauf und Debug-Infos anzeigen

Die Fachlogik des RAG-Agenten liegt nicht in der UI, sondern in `simple_rag_agent.py`.


## Legacy- Und Hilfsmodule

### `KG_Interface.py`

Diese Datei ist ein aelteres, direktes Interface auf den RDF-Knowledge-Graphen.

Sie kann:

- Failure Modes zu einem unterbrochenen Skill abfragen
- Monitoring Actions zu einem Failure Mode lesen
- System Reactions zu einem Failure Mode lesen
- aufgetretene Fehler wieder in den KG zurueckschreiben

Wichtige Einordnung:

- Die Datei arbeitet direkt mit `rdflib`.
- Der Pfad zur Ontologie ist aktuell hart in der Klasse verdrahtet.
- Der neue ExcH-Chatbot baut fuer die interaktive Diagnose eher auf `chatbot_core.py` auf.

Die Datei ist also eher Legacy- oder Integrationscode als das Herz der aktuellen Agentenarchitektur.


### `__init__.py`

Aktuell exportiert das Paket nach aussen nur `KGInterface`.

Das ist historisch erklaerbar, aber nicht unbedingt ein Abbild der wichtigsten heutigen Einstiegspunkte. Die eigentlichen Agenten leben in den UI-, Session- und Core-Modulen.


## Konfigurationsdateien

### `excH_agent_config.json`

Steuert den KG-Agenten:

- `openai_api_key_file`
- `pipeline.enabled`, `pipeline.dir`, `pipeline.runner`, `pipeline.config`, `pipeline.timeout_sec`
- `chatbot.model`
- `chatbot.temperature`


### `rag_agent_config.json`

Steuert den RAG-Agenten:

- `openai_api_key_file`
- `pipeline.enabled`, `pipeline.dir`, `pipeline.runner`, `pipeline.config`, `pipeline.timeout_sec`
- `rag.xml_path`
- `rag.model`
- `rag.temperature`
- `rag.top_k`


## Welche Datei Muss Ich Aendern?

Wenn du Verhalten anpassen willst, ist meist diese Zuordnung sinnvoll:

- UI-Verhalten, Uploads, Streamlit-Darstellung:
  `excH_agent_ui.py` oder `rag_agent_ui.py`

- ExcH-Diagnosefluss, Initialantwort, evD2-Bootstrap:
  `excH_chatbot.py`

- Tooling, Planner, KG-Chatlogik:
  `chatbot_core.py`

- Tiefere ST/FBD- und evD2-Pfadlogik:
  `d2_trace_analysis.py`

- Einfaches RAG, Chunking, Retrieval, Prompting:
  `simple_rag_agent.py`

- Legacy-KG-Abfragen oder KG-Ingestion:
  `KG_Interface.py`

- Nicht-interaktiver MVP-Einstiegspunkt:
  `excH_agent_core.py`


## Kurzvergleich Der Beiden Agenten

| Thema | ExcH-Agent | RAG-Agent |
| --- | --- | --- |
| Hauptdatenquelle | RDF Knowledge Graph | PLCOpenXML |
| Architektur | Tool-Agent mit Planner | einfacher Session-Agent |
| Tiefenanalyse | hoch | mittel |
| evD2-Speziallogik | ja | nein |
| Deterministische Voranalyse | ja, fuer evD2 | nein |
| Retrieval | Tool-/KG-basiert, optional semantisch | lexikalische XML-Suche |
| Komplexitaet | hoch | niedrig |
| Ziel | Root-Cause-Analyse mit konkreten Belegen | schnelle, nachvollziehbare XML-Kontextanalyse |


## Praktische Einordnung

Wenn ein Fall tief rekonstruiert werden soll, inklusive Setter, Guard, State-Pfad und Skill-Kontext, ist der `ExcH`-Agent die richtige Stelle.

Wenn nur eine statische PLCOpenXML und ein Event-/Snapshot-Kontext zusammengebracht werden sollen, ohne Tool-Orchestrierung und ohne KG-Abhaengigkeit, ist der `RAG`-Agent die einfachere und besser kontrollierbare Wahl.
