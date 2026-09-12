# MA_Python_Agent

This repository contains the implementation, experiments, documentation, and evaluation artifacts for a master's thesis on LLM-assisted exception handling and root-cause analysis for PLC/TwinCAT-based systems.

The thesis compares two diagnosis strategies:

- a knowledge-graph-driven Exception Handling agent (`ExcH` / KG agent)
- a simpler PLCOpenXML-based retrieval agent (`RAG` agent)

The repository combines runtime integration, knowledge-graph ingestion, thesis experiments, and benchmark results in one place.

## Related exception-handling framework

The adapted MSRGuard runtime in this repository provides the integration basis for the diagnosis agents. For a more detailed description of MSRGuard as a runtime exception-handling framework—including OPC UA monitoring, event dispatch, monitoring and system reactions, failure recording, and the PFMEA-MSR knowledge-graph bridge—see the [FMEA-MSR Event System](https://github.com/verkal1999/FMEA-MSR-EventSystem) repository.

## Repository structure

- `MSRGuard_Anpassung/`: adapted MSRGuard runtime, Python agent package, local knowledge graphs, certificates, and internal UML diagrams
- `Pipelines/`: ingestion pipeline that converts TwinCAT and PLCopen artifacts into structured knowledge-graph assets
- `Evaluation/`: benchmark configurations, pricing tables, generated result JSON files, and thesis comparison tables
- `Notebooks/`: exploratory notebooks, prototypes, prompt drafts, and helper artifacts used during the thesis
- `UML-Diagrams/`: project-level PlantUML diagrams for architecture, agents, integration, and use cases
- `PyLC_Anpassung/`: helper scripts for converting PLCopen XML into Python-like intermediate code during earlier experimentation
- `PLC2Skill_Dateien/`: external PLC2Skill binary used in supporting experiments
- `scripts/`: small entry-point scripts, including the evaluation wrapper
- `out/`: generated diagram exports and other transient outputs

Local virtual environments and hidden tool folders such as `.venv311/`, `plcrex_venv39/`, `.claude/`, and `.claude-flow/` are workspace support artifacts rather than core thesis deliverables.

## Main architecture

At a high level, the repository is organized around one end-to-end diagnosis workflow:

1. The adapted MSRGuard runtime monitors PLC and OPC UA signals.
2. Runtime events and engineering artifacts are prepared for analysis.
3. The ingestion pipeline builds or refreshes the knowledge graph.
4. The KG-based or RAG-based Python agent analyzes incidents.
5. Results are stored as JSON artifacts and reused for evaluation in the thesis.

## Key entry points

- Runtime entry point: `MSRGuard_Anpassung/src/main.cpp`
- KG agent UI: `MSRGuard_Anpassung/python/msrguard/excH_kg_agent_ui.py`
- RAG agent UI: `MSRGuard_Anpassung/python/msrguard/rag_agent_ui.py`
- KG agent internals: `MSRGuard_Anpassung/python/msrguard/excH_chatbot.py`
- Ingestion runner: `Pipelines/IngestionPipeline/run_ingestion.py`
- Evaluation wrapper: `scripts/run_eval.py`

## Evaluation assets

The current thesis evaluation is documented in `Evaluation/`.

The benchmark is based on two PLC/TwinCAT engineering examples. The second and central evaluation example uses the Fischertechnik high-bay warehouse simulation from the [Fischertechnik I4.0 Modules Simulator for TwinCAT](https://github.com/verkal1999/Fischertechnik_I4.0_Modules_Simulator_TwinCAT) repository.

| Evaluation example | Test cases | System under analysis | Evaluation material |
| --- | --- | --- | --- |
| Example 1 | `TC-001`, `TC-002` | Smaller TwinCAT test project | `config_ingestion.json` and the corresponding local event, KG, configuration, and result artifacts |
| Example 2 | `TC-003`, `TC-004` | Fischertechnik I4.0 module simulation in TwinCAT | `config_ingestion2.json` and the corresponding local event, KG, configuration, and result artifacts |

See [`Evaluation/README.md`](Evaluation/README.md) for the scenario descriptions, test-case mapping, repository boundaries, and artifact locations.

- `Evaluation/configs/` stores per-test-case benchmark configurations.
- `Evaluation/results/` stores generated result JSON files for KG and RAG runs.
- `Evaluation/pricing/` stores provider pricing tables used for cost reporting.

Recent comparison summaries are stored in:

- `Evaluation/TC-003_TC-004_Vergleichstabellen.md`

## Quick start

Run the ingestion pipeline:

```powershell
python Pipelines/IngestionPipeline/run_ingestion.py
```

Run one evaluation case from the repository root:

```powershell
python scripts/run_eval.py --from-config Evaluation/configs/TC-003_kg_openai.json
```

Start the KG agent UI:

```powershell
streamlit run MSRGuard_Anpassung/python/msrguard/excH_kg_agent_ui.py -- --event_json_path <path_to_event.json> --out_json <path_to_result.json>
```

Start the RAG agent UI:

```powershell
streamlit run MSRGuard_Anpassung/python/msrguard/rag_agent_ui.py -- --event_json_path <path_to_event.json>
```

## Python environment and dependencies

The local `.venv311/` environment was created with Python 3.12.1. The following package snapshot was exported from that environment with `pip freeze`:

```text
aiohappyeyeballs==2.6.1
aiohttp==3.13.3
aiosignal==1.4.0
altair==6.0.0
annotated-types==0.7.0
anthropic==0.93.0
anyio==4.11.0
asttokens==3.0.0
astutils==0.0.6
attrs==23.1.0
blinker==1.9.0
cachetools==6.2.6
certifi==2025.10.5
charset-normalizer==3.4.4
click==8.1.6
colorama==0.4.6
comm==0.2.3
coverage==6.5.0
coverage-badge==1.1.0
customtkinter==5.2.2
darkdetect==0.8.0
dataclasses-json==0.6.7
dd==0.5.7
debugpy==1.8.17
decorator==5.2.1
dicttoxml2==2.1.0
distro==1.9.0
docstring_parser==0.17.0
elementpath==3.0.2
exceptiongroup==1.2.0
executing==2.2.1
faiss-cpu==1.13.2
filetype==1.2.0
frozenlist==1.8.0
gitdb==4.0.12
GitPython==3.1.46
google-auth==2.47.0
google-genai==1.59.0
greenlet==3.3.0
groq==0.36.0
h11==0.16.0
httpcore==1.0.9
httpx==0.28.1
httpx-sse==0.4.3
idna==3.11
iniconfig==2.0.0
ipykernel==7.0.1
ipython==9.6.0
ipython_pygments_lexers==1.1.1
ipywidgets==8.1.8
jedi==0.19.2
Jinja2==3.1.6
jiter==0.11.1
jsonpatch==1.33
jsonpointer==3.0.0
jsonschema==4.26.0
jsonschema-specifications==2025.9.1
jupyter_client==8.6.3
jupyter_core==5.9.1
jupyterlab_widgets==3.0.16
langchain==1.2.6
langchain-anthropic==1.4.0
langchain-classic==1.0.1
langchain-community==0.4.1
langchain-core==1.2.28
langchain-google-genai==4.2.1
langchain-groq==1.1.1
langchain-openai==1.1.12
langchain-text-splitters==1.1.0
langchain-together==0.4.0
langgraph==1.0.6
langgraph-checkpoint==4.0.0
langgraph-prebuilt==1.0.6
langgraph-sdk==0.3.3
langsmith==0.6.4
lark==1.1.7
lxml==6.0.2
markdown-it-py==3.0.0
MarkupSafe==3.0.3
marshmallow==3.26.2
matplotlib-inline==0.1.7
mdurl==0.1.2
multidict==6.7.0
mypy_extensions==1.1.0
narwhals==2.16.0
nest-asyncio==1.6.0
networkx==3.6
numpy==2.3.5
openai==2.31.0
orjson==3.11.5
ormsgpack==1.12.2
packaging==25.0
pandas==2.3.3
parso==0.8.5
pathlib==1.0.1
pillow==12.1.1
platformdirs==4.5.0
pluggy==1.3.0
ply==3.10
prompt_toolkit==3.0.52
propcache==0.4.1
protobuf==6.33.5
psutil==7.1.1
pure_eval==0.2.3
pyarrow==23.0.0
pyasn1==0.6.2
pyasn1_modules==0.4.2
pydantic==2.12.5
pydantic-settings==2.12.0
pydantic_core==2.41.5
pydeck==0.9.1
pydot==1.4.2
pyeda==0.29.0
Pygments==2.19.2
pyparsing==3.2.5
pytest==7.2.0
python-dateutil==2.9.0.post0
python-dotenv==1.2.1
python-pptx==1.0.2
pytz==2025.2
pywin32==311
PyYAML==6.0.3
pyzmq==27.1.0
rdflib==7.5.0
referencing==0.37.0
regex==2026.1.15
requests==2.32.5
requests-toolbelt==1.0.0
rich==13.5.1
rpds-py==0.30.0
rsa==4.9.1
setuptools==80.9.0
six==1.17.0
smmap==5.0.2
sniffio==1.3.1
SQLAlchemy==2.0.45
stack-data==0.6.3
streamlit==1.54.0
tenacity==9.1.2
termcolor==2.3.0
tiktoken==0.12.0
toml==0.10.2
tomli==2.0.1
tornado==6.5.2
tqdm==4.67.1
traitlets==5.14.3
typer==0.9.0
typing-inspect==0.9.0
typing-inspection==0.4.2
typing_extensions==4.15.0
tzdata==2025.2
urllib3==2.6.3
uuid_utils==0.13.0
watchdog==6.0.0
wcwidth==0.2.14
websockets==15.0.1
widgetsnbextension==4.0.15
xlsxwriter==3.2.9
xmlschema==2.1.1
xxhash==3.6.0
yarl==1.22.0
z3-solver==4.12.2.0
zstandard==0.25.0
```

## Notes

- This repository mixes production-oriented runtime code, thesis experiments, and exploratory prototypes.
- Third-party upstream components under `MSRGuard_Anpassung/open62541/` and `MSRGuard_Anpassung/extern/` keep their own upstream documentation and are not rewritten as thesis-specific project docs.
