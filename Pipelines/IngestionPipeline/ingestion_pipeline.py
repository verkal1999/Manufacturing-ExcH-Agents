from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------
# Config + Context
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class PipelineConfig:
    project_dir: Path
    sln_path: Path

    kg_cleaned_path: Path
    kg_after_loader_path: Path
    kg_final_path: Path

    write_objects_json: bool = True
    build_hw_mappings: bool = True
    mark_traces_with_hw: bool = True
    simulation_mode: bool = False


@dataclass
class PipelineContext:
    cfg: PipelineConfig
    logger: logging.Logger
    artifacts: Dict[str, Any] = field(default_factory=dict)

    def set(self, key: str, value: Any) -> None:
        self.artifacts[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self.artifacts.get(key, default)


# ---------------------------------------------------------------------
# Step base classes
# ---------------------------------------------------------------------
class StepError(RuntimeError):
    pass


class Step:
    name: str = "unnamed-step"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        """Files that should exist after step runs (used for skip logic)."""
        return []

    def should_skip(self, ctx: PipelineContext, force: bool) -> bool:
        if force:
            return False
        outs = self.outputs(ctx)
        if not outs:
            return False
        return all(p.exists() for p in outs)

    def run(self, ctx: PipelineContext) -> None:
        raise NotImplementedError


class Pipeline:
    def __init__(self, steps: List[Step], force: bool = False) -> None:
        self.steps = steps
        self.force = force

    def run(self, ctx: PipelineContext) -> None:
        for step in self.steps:
            t0 = time.time()
            if step.should_skip(ctx, self.force):
                ctx.logger.info("SKIP  %s (outputs exist)", step.name)
                continue

            ctx.logger.info("START %s", step.name)
            try:
                step.run(ctx)
            except Exception as exc:
                ctx.logger.exception("FAIL  %s", step.name)
                raise StepError(f"Step failed: {step.name}") from exc
            finally:
                dt = time.time() - t0
                ctx.logger.info("DONE  %s (%.2fs)", step.name, dt)


# ---------------------------------------------------------------------
# Utility: logger
# ---------------------------------------------------------------------
def build_logger(verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("oop-pipeline")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(fmt)

    # avoid duplicate handlers when running in notebooks
    if not logger.handlers:
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# ---------------------------------------------------------------------
# Steps: A) PLCopen/TwinCAT extraction
# ---------------------------------------------------------------------
class InitParserStep(Step):
    name = "init-parser"

    def run(self, ctx: PipelineContext) -> None:
        # Import here so that the script can be parsed even if modules aren't on path yet.
        from plcopen_withHW import PLCOpenXMLParserWithHW  # :contentReference[oaicite:6]{index=6}

        parser = PLCOpenXMLParserWithHW(
            project_dir=ctx.cfg.project_dir,
            sln_path=ctx.cfg.sln_path,
        )
        ctx.set("parser", parser)


class ScanProjectStep(Step):
    name = "scan-project"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        return [parser.objects_json_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        parser = ctx.get("parser")
        parser.scan_project_and_write_objects_json(write_json=ctx.cfg.write_objects_json)


class ExportPLCopenXMLStep(Step):
    name = "export-plcopen-xml"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        return [parser.export_xml_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        parser = ctx.get("parser")
        parser.export_plcopen_xml()


class BuildProgramIOMappingStep(Step):
    name = "build-program-io-mapping"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        return [parser.program_io_mapping_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        parser = ctx.get("parser")
        parser.build_program_io_mapping()


class EnrichMappingStep(Step):
    name = "enrich-mapping-with-types-and-code"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        return [parser.program_io_mapping_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        parser = ctx.get("parser")
        parser.enrich_mapping_with_types_and_code()


class BuildVariableTracesStep(Step):
    name = "build-variable-traces"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        return [parser.variable_traces_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        parser = ctx.get("parser")
        parser.build_variable_traces()


class BuildGVLGlobalsStep(Step):
    name = "build-gvl-globals"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        return [parser.gvl_globals_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        parser = ctx.get("parser")
        parser.build_gvl_globals()


class BuildIOMappingsStep(Step):
    name = "build-io-mappings"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        parser = ctx.get("parser")
        if not parser:
            return []
        return [parser.io_mapping_json_path]

    def run(self, ctx: PipelineContext) -> None:
        if not ctx.cfg.build_hw_mappings:
            ctx.logger.info("HW mappings disabled by config.")
            return
        parser = ctx.get("parser")
        parser.build_io_mappings()


class MarkTracesWithHWStep(Step):
    name = "mark-traces-with-hw"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        # This step modifies variable_traces.json in place
        parser = ctx.get("parser")
        return [parser.variable_traces_path] if parser else []

    def run(self, ctx: PipelineContext) -> None:
        if not ctx.cfg.mark_traces_with_hw:
            ctx.logger.info("Mark traces with HW disabled by config.")
            return
        parser = ctx.get("parser")
        parser.mark_variable_traces_with_hw()


# ---------------------------------------------------------------------
# Steps: B) KG Load
# ---------------------------------------------------------------------
class KGLoadStep(Step):
    name = "kg-load"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        return [ctx.cfg.kg_after_loader_path]

    def run(self, ctx: PipelineContext) -> None:
        from kg_loader import KGLoader, KGConfig  # :contentReference[oaicite:7]{index=7}

        kg_cfg = KGConfig(
            twincat_folder=ctx.cfg.project_dir,
            slnfile_str=ctx.cfg.sln_path,
            kg_cleaned_path=ctx.cfg.kg_cleaned_path,
            kg_to_fill_path=ctx.cfg.kg_after_loader_path,
        )
        loader = KGLoader(kg_cfg)

        loader.build_gvl_index_from_objects()
        loader.ingest_programs_from_mapping_json()
        loader.ingest_io_mappings()
        loader.ingest_gvl_globals()

        if ctx.cfg.simulation_mode:
            result = loader.mirror_hw_to_simulation_gvls()
            ctx.logger.info(
                "Simulation HW mirror applied (%s variables on %s _Sim lists).",
                result.mapped_variables,
                result.mapped_lists,
            )

            if result.missing_source_variables:
                ctx.logger.warning(
                    "Simulation HW mirror missing source variables: %s",
                    ", ".join(result.missing_source_variables[:10]),
                )
            if result.missing_target_variables:
                ctx.logger.warning(
                    "Simulation HW mirror missing target variables: %s",
                    ", ".join(result.missing_target_variables[:10]),
                )
            if result.source_without_hw_data:
                ctx.logger.warning(
                    "Simulation HW mirror source variables without HW data: %s",
                    ", ".join(result.source_without_hw_data[:10]),
                )
            ctx.set("simulation_mirror_result", result)

        loader.save()

        ctx.set("kg_loader", loader)


# ---------------------------------------------------------------------
# Steps: C) KG Analyze
# ---------------------------------------------------------------------
class KGAnalyzeStep(Step):
    name = "kg-analyze"

    def outputs(self, ctx: PipelineContext) -> List[Path]:
        return [ctx.cfg.kg_final_path]

    def run(self, ctx: PipelineContext) -> None:
        from kg_manager_new import KGManager  # :contentReference[oaicite:8]{index=8}

        mgr = KGManager(
            ttl_path=ctx.cfg.kg_after_loader_path,
            debug=False,
            case_sensitive=False,
        )
        mgr.load()

        mgr.enrich_default_values_from_declaration_headers(
            overwrite=True,
            create_missing_variables=True,
        )
        
        mgr.analyze_calls()
        mgr.save(ctx.cfg.kg_final_path)
        ctx.set("kg_manager", mgr)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def run_pipeline(cfg: PipelineConfig, force: bool = False, verbose: bool = False) -> PipelineContext:
    logger = build_logger(verbose=verbose)
    ctx = PipelineContext(cfg=cfg, logger=logger)

    steps: List[Step] = [
        InitParserStep(),
        ScanProjectStep(),
        ExportPLCopenXMLStep(),
        BuildProgramIOMappingStep(),
        EnrichMappingStep(),
        BuildVariableTracesStep(),
        BuildGVLGlobalsStep(),
        BuildIOMappingsStep(),
        MarkTracesWithHWStep(),
        KGLoadStep(),
        KGAnalyzeStep(),
    ]

    Pipeline(steps=steps, force=force).run(ctx)
    return ctx

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OOP Pipeline: TwinCAT -> PLCopen -> KG -> KG Analysis")
    p.add_argument("--project-dir", type=str, required=True)
    p.add_argument("--sln-path", type=str, required=True)

    p.add_argument("--kg-cleaned", type=str, required=True)
    p.add_argument("--kg-after-loader", type=str, required=True)
    p.add_argument("--kg-final", type=str, required=True)

    p.add_argument("--force", action="store_true", help="Run all steps even if outputs exist")
    p.add_argument("--verbose", action="store_true")

    p.add_argument("--no-objects-json", action="store_true")
    p.add_argument("--no-hw-mappings", action="store_true")
    p.add_argument("--no-mark-traces", action="store_true")
    p.add_argument("--simulation-mode", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logger = build_logger(verbose=args.verbose)

    cfg = PipelineConfig(
        project_dir=Path(args.project_dir),
        sln_path=Path(args.sln_path),
        kg_cleaned_path=Path(args.kg_cleaned),
        kg_after_loader_path=Path(args.kg_after_loader),
        kg_final_path=Path(args.kg_final),
        write_objects_json=not args.no_objects_json,
        build_hw_mappings=not args.no_hw_mappings,
        mark_traces_with_hw=not args.no_mark_traces,
        simulation_mode=args.simulation_mode,
    )

    ctx = PipelineContext(cfg=cfg, logger=logger)

    steps: List[Step] = [
        InitParserStep(),
        ScanProjectStep(),
        ExportPLCopenXMLStep(),
        BuildProgramIOMappingStep(),
        EnrichMappingStep(),
        BuildVariableTracesStep(),
        BuildGVLGlobalsStep(),
        BuildIOMappingsStep(),
        MarkTracesWithHWStep(),
        KGLoadStep(),
        KGAnalyzeStep(),
    ]

    Pipeline(steps=steps, force=args.force).run(ctx)

    logger.info("Pipeline finished.")
    logger.info("KG after loader: %s", cfg.kg_after_loader_path)
    logger.info("KG final:        %s", cfg.kg_final_path)
    return 0
