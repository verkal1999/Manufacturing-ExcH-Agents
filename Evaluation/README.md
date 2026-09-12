# Evaluation

This folder is part of the `MA_Python_Agent` master's thesis repository. It contains the benchmark setup and the generated evaluation artifacts used to compare the KG-based ExcH agent with the PLCOpenXML-based RAG agent.

## Evaluation examples

The evaluation uses two PLC/TwinCAT engineering examples. Each example is represented by two test-case IDs. Test cases belonging to the same example use the same technical scenario and ground truth, while their configuration files select the agent variant, model provider, and model used for a particular run.

| Example | Test cases | Scenario | PLCOpenXML context | Main local artifacts |
| --- | --- | --- | --- | --- |
| Example 1 | `TC-001`, `TC-002` | A smaller TwinCAT test project in which `TriggerD2` is reached through nested function blocks and a periodic timer | Approximately 184 KB; ingested with `config_ingestion.json` | `TestEvents.ttl`, `evD2-1760790156023800_event.json`, and the matching configs and results |
| Example 2 | `TC-003`, `TC-004` | The Fischertechnik I4.0 high-bay warehouse simulation and its sensor-related failure path | Approximately 1.44 MB; ingested with `config_ingestion2.json` | `TestSIM_filled.ttl`, `evD2-665098719018800_event.json`, and the matching configs and results |

### Example 1: smaller TwinCAT test project

Example 1 is the compact reference scenario used by `TC-001` and `TC-002`. The evaluation question asks why `TriggerD2` was set and which conditions and POUs led to it. The ground truth traces the event through `FB_Automatikbetrieb_F1` and `FB_Diagnose_D2` to a periodic 60-second timer.

This example uses:

- [`config_ingestion.json`](../Pipelines/IngestionPipeline/config_ingestion.json) for ingestion
- [`TestEvents.ttl`](../MSRGuard_Anpassung/KGs/TestEvents.ttl) as the knowledge graph
- [`evD2-1760790156023800_event.json`](../MSRGuard_Anpassung/python/agent_results/evD2-1760790156023800_event.json) as the event snapshot
- the `TC-001_*.json` and `TC-002_*.json` files in [`configs/`](configs/) as run configurations
- the corresponding `TC-001_*` and `TC-002_*` files in [`results/`](results/) as generated outputs

### Example 2: Fischertechnik I4.0 high-bay warehouse

Example 2 is the central evaluation scenario used by `TC-003` and `TC-004`. The underlying PLC and simulation project is maintained separately in the [Fischertechnik I4.0 Modules Simulator for TwinCAT](https://github.com/verkal1999/Fischertechnik_I4.0_Modules_Simulator_TwinCAT) repository.

The scenario simulates a failure in the high-bay warehouse workflow. A missing workpiece, or equivalently a false outer light-barrier signal, causes the relevant sensor value to become false. The evaluation follows the resulting diagnostic path through `VSG_ErrorDetected` and the participating POUs and variables. A correct diagnosis should identify the sensor or missing workpiece as the root cause, distinguish that cause from the downstream error trigger, and propose appropriate hardware or simulation checks.

This example uses:

- [`config_ingestion2.json`](../Pipelines/IngestionPipeline/config_ingestion2.json) for ingestion
- [`TestSIM_filled.ttl`](../MSRGuard_Anpassung/KGs/TestSIM_filled.ttl) as the knowledge graph
- [`evD2-665098719018800_event.json`](../MSRGuard_Anpassung/python/agent_results/evD2-665098719018800_event.json) as the event snapshot
- the `TC-003_*.json` and `TC-004_*.json` files in [`configs/`](configs/) as run configurations
- the corresponding `TC-003_*` and `TC-004_*` files in [`results/`](results/) as generated outputs
- [`TC-003_TC-004_Vergleichstabellen.md`](TC-003_TC-004_Vergleichstabellen.md) for the detailed KG-versus-RAG comparison

`TC-003` contains provider-specific KG and RAG configurations. `TC-004` adds a directly paired KG/RAG evaluation using OpenAI GPT-4o. Both test cases examine the same Example 2 event and ground truth.

## Relationship to the Fischertechnik simulator repository

The two repositories have different roles:

- [Fischertechnik I4.0 Modules Simulator for TwinCAT](https://github.com/verkal1999/Fischertechnik_I4.0_Modules_Simulator_TwinCAT) contains the PLC application and simulation environment for Example 2.
- This repository contains the ingestion pipeline, diagnostic agents, captured event snapshot, derived knowledge graph, evaluation configurations, result JSON files, and comparison tables used in the thesis.

Keeping these roles separate makes it possible to inspect the system under analysis in the simulator repository and then follow the evaluation artifacts and agent comparison in this repository.

## What this folder contains

- `configs/`: per-test-case evaluation configurations
- `pricing/`: provider-specific pricing tables used to derive token-based cost estimates
- `results/`: generated result JSON files for completed evaluation runs
- `TC-003_TC-004_Vergleichstabellen.md`: manually prepared thesis comparison tables and summary text

## Folder details

### `configs/`

This folder stores one JSON configuration per test case and agent variant.

Typical naming pattern:

- `TC-001_kg_openai.json`
- `TC-003_rag_anthropic.json`

Each config links an event JSON, a knowledge-graph path, a PLCOpenXML export path, model settings, pricing settings, and the target output folder.

### `pricing/`

This folder stores pricing snapshots for the providers used in the thesis evaluation:

- `anthropic_pricing.json`
- `groq_pricing.json`
- `openai_pricing.json`
- `together_pricing.json`

### `results/`

This folder stores the generated evaluation outputs. Each result file usually contains:

- the executed question
- model metadata
- agent answer text
- token usage
- runtime
- cost estimate
- judge verdict and summary

Typical naming pattern:

- `TC-003_kg_openai_gpt-4o-mini.json`
- `TC-004_rag_openai_gpt-4o.json`

## How evaluations are started

The repository provides a thin wrapper script in `scripts/run_eval.py`.

Example:

```powershell
python scripts/run_eval.py --from-config Evaluation/configs/TC-003_kg_openai.json
```

## Current thesis usage

The currently documented benchmark set covers `TC-001` to `TC-004`. The PLCOpenXML input for Example 2 is approximately 7.8 times larger than the input for Example 1. This difference is important for the thesis discussion of scaling behavior because it makes the resource usage and answer quality of the KG and RAG approaches comparable across two substantially different context sizes.
