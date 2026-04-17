import json
import sys
from pathlib import Path

# -------------------------------------------------------------------------
# FIX 1: Pfad zum "Parent"-Ordner (Pipelines oder Root) hinzufügen, 
# falls Python Module nicht findet. Aber meistens reicht der einfache Import:
# -------------------------------------------------------------------------

# Wenn IngestionPipeline.py im SELBEN Ordner liegt wie dieses Skript:
try:
    # Versuche den direkten Import (Sibling Import)
    from ingestion_pipeline import PipelineConfig, run_pipeline
except ImportError:
    # Falls das fehlschlägt (z.B. wegen Namenskonflikten mit dem Ordnernamen),
    # fügen wir das aktuelle Verzeichnis explizit zum Pfad hinzu:
    sys.path.append(str(Path(__file__).parent))
    from ingestion_pipeline import PipelineConfig, run_pipeline


def load_cfg(p: Path) -> PipelineConfig:
    # Datei als rohen Text lesen
    raw_content = p.read_text(encoding="utf-8")
    
    # Backslashes für JSON fixen
    fixed_content = raw_content.replace("\\", "\\\\")
    
    d = json.loads(fixed_content)
    
    return PipelineConfig(
        project_dir=Path(d["project_dir"]),
        sln_path=Path(d["sln_path"]),
        kg_cleaned_path=Path(d["kg_cleaned_path"]),
        kg_after_loader_path=Path(d["kg_after_loader_path"]),
        kg_final_path=Path(d["kg_final_path"]),
        write_objects_json=bool(d.get("write_objects_json", True)),
        build_hw_mappings=bool(d.get("build_hw_mappings", True)),
        mark_traces_with_hw=bool(d.get("mark_traces_with_hw", True)),
        simulation_mode=bool(d.get("simulation_mode", False)),
    )

def main():
    # ---------------------------------------------------------------------
    # FIX 2: Relativen Pfad zur Config-Datei sicherstellen
    # ---------------------------------------------------------------------
    script_dir = Path(__file__).parent.resolve()
    config_path = script_dir / "config_ingestion2.json"
    
    if not config_path.exists():
        print(f"FEHLER: Konfigurationsdatei nicht gefunden unter: {config_path}")
        return

    print(f"Lade Konfiguration von: {config_path}")
    cfg = load_cfg(config_path)
    run_pipeline(cfg, force=True, verbose=True)

if __name__ == "__main__":
    main()
