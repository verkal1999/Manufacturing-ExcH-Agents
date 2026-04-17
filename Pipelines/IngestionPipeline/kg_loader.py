from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, List, Optional, Any
import xml.etree.ElementTree as ET
import json
import re
from rdflib import Graph, Namespace, RDF, URIRef, Literal, OWL
from rdflib.namespace import XSD, RDFS


@dataclass
class KGConfig:
    twincat_folder: Path
    slnfile_str: Path
    kg_cleaned_path: Path
    kg_to_fill_path: Path

    @property
    def objects_path(self) -> Path:
        return Path(str(self.slnfile_str).replace(".sln", "_objects.json"))

    @property
    def prog_io_mappings_path(self) -> Path:
        return self.twincat_folder / "program_io_with_mapping.json"

    @property
    def io_map_path(self) -> Path:
        return self.twincat_folder / "io_mappings.json"

    @property
    def gvl_globals_path(self) -> Path:
        return self.twincat_folder / "gvl_globals.json"


@dataclass(frozen=True)
class SimulationMirrorResult:
    mapped_lists: int
    mapped_variables: int
    missing_source_variables: tuple[str, ...]
    missing_target_variables: tuple[str, ...]
    source_without_hw_data: tuple[str, ...]
    skipped_control_variables: tuple[str, ...]


class KGLoader:
    """
    KGLoader mit integrierter Definition von IEC 61131-3 Standard-Bausteinen.
    Stellt sicher, dass StandardFBType_TON etc. samt Ports existieren.
    """

    # Definition der Standard-FBs und ihrer Ports
    IEC_STANDARD_FBS = {
        "SR": {"inputs": {"S1": "BOOL", "R": "BOOL"}, "outputs": {"Q1": "BOOL"}},
        "RS": {"inputs": {"S": "BOOL", "R1": "BOOL"}, "outputs": {"Q1": "BOOL"}},
        "R_TRIG": {"inputs": {"CLK": "BOOL"}, "outputs": {"Q": "BOOL"}},
        "F_TRIG": {"inputs": {"CLK": "BOOL"}, "outputs": {"Q": "BOOL"}},
        "CTU": {"inputs": {"CU": "BOOL", "R": "BOOL", "PV": "INT"}, "outputs": {"Q": "BOOL", "CV": "INT"}},
        "CTD": {"inputs": {"CD": "BOOL", "LD": "BOOL", "PV": "INT"}, "outputs": {"Q": "BOOL", "CV": "INT"}},
        "CTUD": {"inputs": {"CU": "BOOL", "CD": "BOOL", "R": "BOOL", "LD": "BOOL", "PV": "INT"}, "outputs": {"QU": "BOOL", "QD": "BOOL", "CV": "INT"}},
        "TP": {"inputs": {"IN": "BOOL", "PT": "TIME"}, "outputs": {"Q": "BOOL", "ET": "TIME"}},
        "TON": {"inputs": {"IN": "BOOL", "PT": "TIME"}, "outputs": {"Q": "BOOL", "ET": "TIME"}},
        "TOF": {"inputs": {"IN": "BOOL", "PT": "TIME"}, "outputs": {"Q": "BOOL", "ET": "TIME"}}
    }
    _gvl_var_stmt_re = re.compile(
        r'^\s*([A-Za-z_]\w*)'
        r'(?:\s+AT\s+([^:]+))?'
        r'\s*:\s*'
        r'([^:=;]+?)'
        r'(?:\s*:=\s*([^;]+?))?'
        r'\s*;\s*$',
        re.M | re.S
    )

    def __init__(self, config: KGConfig):
        self.config = config

        self.AG = Namespace('http://www.semanticweb.org/AgentProgramParams/')
        self.DP = Namespace('http://www.semanticweb.org/AgentProgramParams/dp_')
        self.OP = Namespace('http://www.semanticweb.org/AgentProgramParams/op_')
        self.pou_decl_headers: Dict[str, str] = {}
        self.kg = Graph()
        if self.config.kg_cleaned_path.exists():
            with open(self.config.kg_cleaned_path, "r", encoding="utf-8") as fkg:
                self.kg.parse(file=fkg, format="turtle")
        else:
            print(f"WARNUNG: Basis-KG {self.config.kg_cleaned_path} nicht gefunden. Starte leer.")

        self.prog_uris: Dict[str, URIRef] = {}
        self.var_uris: Dict[Tuple[str, str], URIRef] = {}
        self.hw_var_uris: Dict[str, URIRef] = {}
        self.pending_ext_hw_links: List[Tuple[URIRef, str]] = []
        self.plc_project_uris: Dict[str, URIRef] = {}
        self.gvl_short_to_full: Dict[str, set[str]] = {}
        self.gvl_full_to_type: Dict[str, str] = {}

        # Set für schnelle Abfrage
        self.standard_fbs_set = set(self.IEC_STANDARD_FBS.keys())

        # WICHTIG: Standard-FBs direkt beim Start initialisieren
        self._ensure_standard_fbs_exist()

    # -------------------------------------------------
    # Standard FBs Erzeugung (NEU)
    # -------------------------------------------------
    def _ensure_standard_fbs_exist(self):
        """Erzeugt die Definitionen (Typen + Ports) für Standard-FBs im Graphen."""
        for fb_name, ports in self.IEC_STANDARD_FBS.items():
            # Typ URI
            fb_uri = self.make_uri(f"StandardFBType_{fb_name}")
            self.kg.add((fb_uri, RDF.type, self.AG.class_StandardFBType))
            self.kg.add((fb_uri, RDF.type, self.AG.class_FBType))
            self.kg.add((fb_uri, self.DP.hasPOUName, Literal(fb_name)))
            self.kg.add((fb_uri, self.DP.hasPOUType, Literal("FunctionBlock")))
            self.kg.add((fb_uri, self.DP.hasPOULanguage, Literal("ST")))
            
            # Cache aktualisieren damit get_fb_uri schnell zugreifen kann
            self.prog_uris[fb_name] = fb_uri

            # Ports erzeugen
            all_ports = []
            for pname, ptype in ports["inputs"].items():
                all_ports.append((pname, ptype, "Input"))
            for pname, ptype in ports["outputs"].items():
                all_ports.append((pname, ptype, "Output"))

            for pname, ptype, pdir in all_ports:
                port_uri = self.make_uri(f"Port_{fb_name}_{pname}")
                self.kg.add((port_uri, RDF.type, self.AG.class_Port))
                self.kg.add((port_uri, self.DP.hasPortName, Literal(pname)))
                self.kg.add((port_uri, self.DP.hasPortDirection, Literal(pdir)))
                self.kg.add((port_uri, self.DP.hasPortType, Literal(ptype)))
                
                # Verknüpfung
                self.kg.add((fb_uri, self.OP.hasPort, port_uri))

    # -------------------------------------------------
    # URI-Helfer
    # -------------------------------------------------

    def make_uri(self, name: str) -> URIRef:
        safe = (
            name
            .replace('^', '__dach__')
            .replace('.', '__dot__')
            .replace(' ', '__leerz__')
        )
        return URIRef(self.AG + safe)

    def _add_program_name(self, prog_uri: URIRef, raw_name: str) -> None:
        self.kg.add((prog_uri, self.DP.hasProgramName, Literal(raw_name)))

    def _add_variable_name(self, var_uri: URIRef, raw_name: str) -> None:
        vname = raw_name.replace("__dot__",".")
        self.kg.add((var_uri, self.DP.hasVariableName, Literal(vname)))

    def _clean_expression(self, expr: str) -> str:
        if not expr: return ""
        clean = expr.replace('NOT ', '').replace('(', '').replace(')', '').strip()
        return clean

    def get_program_uri(self, prog_name: str) -> URIRef:
        uri = self.prog_uris.get(prog_name)
        if uri is None:
            uri = self.make_uri(f"Program_{prog_name}")
            self.prog_uris[prog_name] = uri
            self._add_program_name(uri, prog_name)
        return uri
    
    def get_fb_uri(self, fb_name: str) -> URIRef:
        """Holt URI für FB-Typ. Standard-FBs werden bevorzugt."""
        uri = self.prog_uris.get(fb_name)
        if uri is None:
            # Falls Standard FB noch nicht im Cache (sollte durch __init__ eigentlich da sein)
            if fb_name.upper() in self.standard_fbs_set:
                uri = self.make_uri(f"StandardFBType_{fb_name.upper()}")
            else:
                uri = self.make_uri(f"FBType_{fb_name}")
                self.kg.add((uri, RDF.type, self.AG.class_FBType))
            
            self.prog_uris[fb_name] = uri
        return uri

    def get_local_var_uri(self, prog_name: str, var_name: str) -> URIRef:
        key = (prog_name, var_name)
        uri = self.var_uris.get(key)
        if uri is None:
            raw_id = f"Var_{prog_name}_{var_name}"
            uri = self.make_uri(raw_id)
            self.kg.add((uri, RDF.type, self.AG.class_Variable))
            self.var_uris[key] = uri
            self._add_variable_name(uri, var_name)
        return uri

    def get_port_uri(self, pou_name: str, port_name: str) -> URIRef:
        safe_pou = pou_name.replace('.', '__dot__')
        safe_port = port_name.replace('.', '__dot__')
        uri = self.make_uri(f"Port_{safe_pou}_{safe_port}")
        return uri

    def get_fb_instance_uri(self, parent_prog: str, var_name: str) -> URIRef:
        uri = self.make_uri(f"FBInst_{parent_prog}_{var_name}")
        return uri

    def get_port_instance_uri(self, parent_prog: str, fb_var_name: str, port_name: str) -> URIRef:
        uri = self.make_uri(f"PortInst_{parent_prog}_{fb_var_name}_{port_name}")
        return uri
        
    def _is_standard_type(self, type_name: str) -> bool:
        standards = {'BOOL', 'INT', 'DINT', 'REAL', 'LREAL', 'TIME', 'STRING', 'WSTRING', 'BYTE', 'WORD', 'DWORD', 'LWORD', 'UDINT', 'UINT', 'SINT', 'USINT'}
        return type_name.upper() in standards

    # -------------------------------------------------
    # Schritt 1: GVL-Index
    # -------------------------------------------------

    def build_gvl_index_from_objects(self) -> None:
        objects_path = self.config.objects_path
        objects_data = []

        if objects_path.exists():
            objects_data = json.loads(objects_path.read_text(encoding="utf-8"))
        else:
            # Kein early return, damit wir später zumindest export.xml als Fallback nutzen können
            objects_data = []

        gvl_short_to_full: Dict[str, set[str]] = {}
        gvl_full_to_type: Dict[str, str] = {}
        pou_decl_headers: Dict[str, str] = {}

        for obj in objects_data:
            if obj.get("kind") == "GVL":
                gvl_name = obj.get("name")
                for glob in obj.get("globals", []):
                    short = glob.get("name")
                    if not short:
                        continue
                    if gvl_name == "GVL":
                        full = f"GVL.{short}"
                    else:
                        full = f"{gvl_name}.{short}"
                    gvl_short_to_full.setdefault(short, set()).add(full)
                    vtype = glob.get("type")
                    if vtype:
                        gvl_full_to_type[full] = vtype

            # NEU: POU Declaration Header aus TcPOU Scan übernehmen
            if obj.get("kind") == "POU":
                name = obj.get("name")
                decl = obj.get("declaration")
                if name and decl and str(decl).strip():
                    pou_decl_headers[name.lower()] = str(decl).strip()

        self.gvl_short_to_full = gvl_short_to_full
        self.gvl_full_to_type = gvl_full_to_type
        self.pou_decl_headers = pou_decl_headers

        # NEU: Fallback/Ergänzung über export.xml
        self._merge_pou_decl_headers_from_export_xml()


        self.gvl_short_to_full = gvl_short_to_full
        self.gvl_full_to_type = gvl_full_to_type
    
    def _merge_pou_decl_headers_from_export_xml(self) -> None:
        export_path = self.config.twincat_folder / "export.xml"
        if not export_path.exists():
            return

        xml_text = export_path.read_text(encoding="utf-8", errors="replace")
        try:
            root = ET.fromstring(xml_text)
        except Exception:
            return

        NS = {"ns": "http://www.plcopen.org/xml/tc6_0200"}

        for pou in root.findall(".//ns:pou", NS):
            name = pou.get("name")
            if not name:
                continue

            key = name.lower()
            if key in self.pou_decl_headers and self.pou_decl_headers[key].strip():
                continue

            decl = self._build_decl_header_from_export_pou(pou, NS)
            if decl.strip():
                self.pou_decl_headers[key] = decl.strip()


    def _build_decl_header_from_export_pou(self, pou_elem: ET.Element, NS: Dict[str, str]) -> str:
        interface = pou_elem.find("ns:interface", NS)
        if interface is None:
            return ""

        sections = [
            ("inputVars", "VAR_INPUT"),
            ("outputVars", "VAR_OUTPUT"),
            ("inOutVars", "VAR_IN_OUT"),
            ("localVars", "VAR"),
            ("tempVars", "VAR_TEMP"),
        ]

        parts: List[str] = []

        for sect_tag, st_kw in sections:
            vars_: List[ET.Element] = []
            for sect in interface.findall(f"ns:{sect_tag}", NS):
                vars_.extend(sect.findall("ns:variable", NS))

            if not vars_:
                continue

            parts.append(st_kw)
            for var in vars_:
                line = self._export_var_to_st_line(var, NS)
                if line:
                    parts.append(line)
            parts.append("END_VAR")
            parts.append("")

        return "\n".join(parts).strip()


    def _export_var_to_st_line(self, var: ET.Element, NS: Dict[str, str]) -> str:
        name = var.get("name")
        if not name:
            return ""

        vtype = self._get_export_var_type(var, NS) or "UNKNOWN"
        init = self._get_export_var_init(var, NS)

        if init is not None and str(init).strip() != "":
            return f"    {name} : {vtype} := {init};"
        return f"    {name} : {vtype};"


    def _get_export_var_type(self, var: ET.Element, NS: Dict[str, str]) -> Optional[str]:
        tnode = var.find("ns:type", NS)
        if tnode is None:
            return None

        derived = tnode.find("ns:derived", NS)
        if derived is not None:
            return derived.attrib.get("name")

        for child in tnode:
            tag = child.tag
            local = tag.split("}", 1)[1] if "}" in tag else tag
            return local
        return None


    def _get_export_var_init(self, var: ET.Element, NS: Dict[str, str]) -> Optional[str]:
        init_node = var.find("ns:initialValue", NS)
        if init_node is None:
            return None

        simple = init_node.find("ns:simpleValue", NS)
        if simple is not None:
            return simple.attrib.get("value")

        # generischer Fallback falls andere Struktur
        for n in init_node.iter():
            if "value" in getattr(n, "attrib", {}):
                return n.attrib.get("value")
        return None


    def _pick_var(self, item: Dict[str, Any]) -> Optional[str]:
        ext = item.get("external")
        return ext.split('.')[-1] if ext else item.get('internal')

    def _get_ext_var_uri(self, external_raw: Optional[str], caller_prog: str) -> Optional[URIRef]:
        if not external_raw:
            return None
        
        external = self._clean_expression(external_raw)

        if external.startswith('GVL') or external.startswith('GV') or external.startswith('OPCUA'):
            uri = self.hw_var_uris.get(external)
            if uri is None:
                uri = self.make_uri(external)
                self.kg.add((uri, RDF.type, self.AG.class_Variable))
                self.hw_var_uris[external] = uri
                self._add_variable_name(uri, external)
                self.kg.add((uri, self.DP.hasVariableScope, Literal("global")))
            return uri

        if '.' in external:
            parts = external.split('.')
            if len(parts) == 2:
                instance_name, port_name = parts
                fb_var_uri = self.get_local_var_uri(caller_prog, instance_name)
                fb_inst_uri = self.get_fb_instance_uri(caller_prog, instance_name)
                p_inst_uri = self.get_port_instance_uri(caller_prog, instance_name, port_name)
                self.kg.add((p_inst_uri, RDF.type, self.AG.class_PortInstance))
                self.kg.add((p_inst_uri, self.OP.isPortOfInstance, fb_inst_uri))
                self.kg.add((p_inst_uri, self.DP.hasExpressionText, Literal(f"{instance_name}.{port_name}", datatype=XSD.string)))
                return p_inst_uri

        return self.get_local_var_uri(caller_prog, external)

    # -------------------------------------------------
    # Schritt 2: Programme
    # -------------------------------------------------

    def ingest_programs_from_mapping_json(self) -> None:
        prog_data = json.loads(self.config.prog_io_mappings_path.read_text(encoding="utf-8"))

        # NEUE Property definieren (Sicherstellen, dass sie existiert)
        self.kg.add((self.OP.implementsPort, RDF.type, OWL.ObjectProperty))
        #Neue Property für POU Declaration Header
        self.kg.add((self.DP.hasPOUDeclarationHeader, RDF.type, OWL.DatatypeProperty))
        # Neue Property für FB-Instanznamen (wichtig für spätere SPARQL-Auflösung)
        self.kg.add((self.DP.hasFBInstanceName, RDF.type, OWL.DatatypeProperty))
        self.kg.add((self.DP.hasFBInstanceName, RDFS.domain, self.AG.class_FBInstance))
        self.kg.add((self.DP.hasFBInstanceName, RDFS.range, XSD.string))

        for entry in prog_data:
            prog_name = entry.get("Programm_Name")
            if not prog_name: continue
            
            pou_type = entry.get("pou_type")
            pou_uri = None

            if pou_type == "functionBlock":
                pou_uri = self.get_fb_uri(prog_name)
                self.kg.add((pou_uri, RDF.type, self.AG.class_FBType))
            elif pou_type == "program":
                pou_uri = self.get_program_uri(prog_name)
                self.kg.add((pou_uri, RDF.type, self.AG.class_Program))
            else:
                pou_uri = self.get_program_uri(prog_name)

            self.kg.add((pou_uri, self.DP.hasPOUName, Literal(prog_name)))
            decl = self.pou_decl_headers.get(prog_name.lower())
            if decl:
                self.kg.add((pou_uri, self.DP.hasPOUDeclarationHeader, Literal(decl, datatype=XSD.string)))
            if pou_uri is None: continue

            project_name = entry.get("PLCProject_Name")
            if project_name:
                project_uri = self.get_or_create_plc_project(project_name)
                self.kg.add((project_uri, self.OP.consistsOfPOU, pou_uri))

            # A. PORTS & INTERNE VARIABLEN (Inputs / Outputs)
            for sec in ("inputs", "outputs"):
                direction = "Input" if sec == "inputs" else "Output"
                for var in entry.get(sec, []):
                    vname = self._pick_var(var)
                    if not vname: continue
                    
                    # 1. Der Port (Schnittstelle nach außen)
                    port_uri = self.get_port_uri(prog_name, vname)
                    self.kg.add((port_uri, RDF.type, self.AG.class_Port))
                    self.kg.add((port_uri, self.DP.hasPortName, Literal(vname)))
                    self.kg.add((port_uri, self.DP.hasPortDirection, Literal(direction)))
                    self.kg.add((pou_uri, self.OP.hasPort, port_uri))

                    vtype = var.get("internal_type")
                    if vtype:
                        self.kg.add((port_uri, self.DP.hasPortType, Literal(vtype)))

                    # 2. Die interne Variable (Implementierung für den Code)
                    # Name: Var_<POU>_<PortName>
                    internal_var_uri = self.get_local_var_uri(prog_name, vname)
                    self.kg.add((pou_uri, self.OP.usesVariable, internal_var_uri)) # FB nutzt diese Variable intern
                    self.kg.add((pou_uri, self.OP.hasInternalVariable, internal_var_uri))
                    
                    # Verknüpfung: Variable implementiert Port
                    self.kg.add((internal_var_uri, self.OP.implementsPort, port_uri))
                    
                    if vtype:
                        self.kg.add((internal_var_uri, self.DP.hasVariableType, Literal(vtype)))

                    # Mappings (External - Bindings im MAIN etc.)
                    # Das Binding gehört eigentlich zum Port, aber oft wird es im JSON an die Variable gehängt.
                    external = var.get("external")
                    if external:
                        target_uri = self._get_ext_var_uri(external, prog_name)
                        if target_uri:
                            self.kg.add((target_uri, self.OP.isBoundToPort, port_uri))
                            clean_ext = self._clean_expression(external)
                            self.pending_ext_hw_links.append((target_uri, clean_ext))
                    
                    if var.get("opcua_da"):
                        self.kg.add((internal_var_uri, self.DP.hasOPCUADataAccess, Literal(True)))
                    if var.get("opcua_write"):
                        self.kg.add((internal_var_uri, self.DP.hasOPCUAWriteAccess, Literal(True)))

            # B. TEMPS (Lokale Variablen, keine Ports)
            for temp in entry.get("temps", []):
                vname = temp.get("name")
                if not vname: continue
                
                v_uri = self.get_local_var_uri(prog_name, vname)
                self.kg.add((pou_uri, self.OP.usesVariable, v_uri))
                self.kg.add((pou_uri, self.OP.hasInternalVariable, v_uri))
                if temp.get("opcua_da"):
                    self.kg.add((v_uri, self.DP.hasOPCUADataAccess, Literal(True)))
                if temp.get("opcua_write"):
                    self.kg.add((v_uri, self.DP.hasOPCUAWriteAccess, Literal(True)))
                
                ttype = temp.get("type")
                if ttype:
                    self.kg.add((v_uri, self.DP.hasVariableType, Literal(ttype)))
                    
                    if not self._is_standard_type(ttype):
                        fb_inst_uri = self.get_fb_instance_uri(prog_name, vname)
                        self.kg.add((fb_inst_uri, RDF.type, self.AG.class_FBInstance))
                        self.kg.add((fb_inst_uri, self.DP.hasFBInstanceName, Literal(vname, datatype=XSD.string)))
                        self.kg.add((v_uri, self.OP.representsFBInstance, fb_inst_uri))
                        
                        fb_type_uri = self.get_fb_uri(ttype) 
                        self.kg.add((fb_inst_uri, self.OP.isInstanceOfFBType, fb_type_uri))
            # Code
            code = entry.get("program_code")
            lang = entry.get("programming_lang")
            if code:
                self.kg.add((pou_uri, self.DP.hasPOUCode, Literal(code)))
            if lang:
                self.kg.add((pou_uri, self.DP.hasPOULanguage, Literal(lang)))

    # -------------------------------------------------
    # Schritt 3 & 4
    # -------------------------------------------------

    def ingest_io_mappings(self) -> None:
        if not self.config.io_map_path.exists(): return
        io_data = json.loads(self.config.io_map_path.read_text(encoding="utf-8"))

        for entry in io_data:
            plc_var = entry.get("plc_var")
            if not plc_var: continue

            var_uri = self.hw_var_uris.get(plc_var)
            if var_uri is None:
                var_uri = self.make_uri(plc_var)
                self.kg.add((var_uri, RDF.type, self.AG.class_Variable))
                self.hw_var_uris[plc_var] = var_uri
                self._add_variable_name(var_uri, plc_var)

            hw_addr = entry.get("ea_address")
            if hw_addr:
                self.kg.add((var_uri, self.DP.hasHardwareAddress, Literal(hw_addr)))

            io_path = entry.get("io_path")
            if io_path:
                io_uri = self.make_uri(f"IOChannel_{io_path}")
                self.kg.add((io_uri, RDF.type, self.AG.class_IOChannel))
                self.kg.add((var_uri, self.OP.isBoundToChannel, io_uri))

            raw_xml = entry.get("io_raw_xml")
            if raw_xml:
                self.kg.add((var_uri, self.DP.ioRawXml, Literal(raw_xml, datatype=XSD.string)))

        for ext_uri, external_full in self.pending_ext_hw_links:
            hw_uri = self.hw_var_uris.get(external_full)
            if hw_uri is not None and ext_uri != hw_uri:
                self.kg.add((ext_uri, self.OP.isMappedToVariable, hw_uri))

    def ingest_gvl_globals(self) -> None:
        if not self.config.gvl_globals_path.exists(): return
        gvl_data = json.loads(self.config.gvl_globals_path.read_text(encoding="utf-8"))

        for gvl in gvl_data:
            self._add_gvl_definition_to_graph(gvl)

    def _add_gvl_definition_to_graph(self, gvl: Dict[str, Any]) -> None:
        gvl_name = gvl["name"]
        list_uri = self.make_uri(f"GVLList_{gvl_name}")
        self.kg.add((list_uri, RDF.type, self.AG.class_GlobalVariableList))
        self.kg.add((list_uri, self.DP.hasGlobalVariableListName, Literal(gvl_name)))

        for gv in gvl.get("globals", []):
            base_name = gv["name"]
            var_local = f"{gvl_name}__dot__{base_name}"
            locvname = var_local.replace("__dot__", ".")
            var_uri = self.AG[var_local]

            self.kg.add((var_uri, RDF.type, self.AG.class_Variable))
            self.kg.add((var_uri, self.DP.hasVariableName, Literal(locvname)))
            self._add_variable_name(var_uri, base_name)
            self.kg.add((list_uri, self.OP.listsGlobalVariable, var_uri))

            if gv.get("type"):
                self.kg.add((var_uri, self.DP.hasVariableType, Literal(gv["type"])))
            if gv.get("init") is not None:
                self.kg.add((var_uri, self.DP.hasInitialValue, Literal(gv["init"])))
            if gv.get("address"):
                self.kg.add((var_uri, self.DP.hasHardwareAddress, Literal(gv["address"])))
            if gv.get("opcua_da"):
                self.kg.add((var_uri, self.DP.hasOPCUADataAccess, Literal(True)))
            if gv.get("opcua_write"):
                self.kg.add((var_uri, self.DP.hasOPCUAWriteAccess, Literal(True)))

    @staticmethod
    def _strip_st_comments(text: str) -> str:
        text = re.sub(r'\(\*.*?\*\)', '', text, flags=re.S)
        text = re.sub(r'//.*', '', text)
        return text

    @classmethod
    def _extract_global_vars_from_declaration(cls, declaration: str) -> List[Dict[str, Any]]:
        text = cls._strip_st_comments(declaration or "")
        match = re.search(r'VAR_GLOBAL\b.*?\n(.*?)END_VAR', text, flags=re.S | re.I)
        if not match:
            return []

        globals_: List[Dict[str, Any]] = []
        for var_match in cls._gvl_var_stmt_re.finditer(match.group(1)):
            name, address, typ, init = [g.strip() if g else None for g in var_match.groups()]
            globals_.append({
                "name": name,
                "type": re.sub(r'\s+', ' ', typ).strip() if typ else None,
                "init": init.strip() if init else None,
                "address": address,
            })
        return globals_

    def _load_standalone_sim_gvls(self) -> List[Dict[str, Any]]:
        discovered: List[Dict[str, Any]] = []
        seen_names: set[str] = set()

        for gvl_path in self.config.twincat_folder.rglob("*_Sim.TcGVL"):
            try:
                root = ET.fromstring(gvl_path.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue

            gvl_elem = root.find(".//GVL")
            if gvl_elem is None:
                continue

            gvl_name = gvl_elem.get("Name")
            if not gvl_name or gvl_name in seen_names:
                continue

            decl_elem = root.find(".//Declaration")
            declaration = (decl_elem.text or "") if decl_elem is not None else ""
            discovered.append({
                "name": gvl_name,
                "globals": self._extract_global_vars_from_declaration(declaration),
            })
            seen_names.add(gvl_name)

        return discovered

    def _find_variable_uri_by_full_name(self, full_name: str) -> Optional[URIRef]:
        cached = self.hw_var_uris.get(full_name)
        if cached is not None and (cached, None, None) in self.kg:
            return cached

        candidate = self.make_uri(full_name)
        if (candidate, None, None) in self.kg:
            return candidate

        literal_name = Literal(full_name)
        for subj in self.kg.subjects(self.DP.hasVariableName, literal_name):
            if isinstance(subj, URIRef):
                return subj
        return None

    def mirror_hw_to_simulation_gvls(self) -> SimulationMirrorResult:
        gvl_by_name: Dict[str, Dict[str, Any]] = {}

        if self.config.gvl_globals_path.exists():
            gvl_data = json.loads(self.config.gvl_globals_path.read_text(encoding="utf-8"))
            for gvl in gvl_data:
                gvl_name = gvl.get("name")
                if gvl_name:
                    gvl_by_name[gvl_name] = gvl

        for sim_gvl in self._load_standalone_sim_gvls():
            gvl_name = sim_gvl["name"]
            gvl_by_name[gvl_name] = sim_gvl
            self._add_gvl_definition_to_graph(sim_gvl)

        hw_predicates = (
            self.DP.hasHardwareAddress,
            self.DP.ioRawXml,
            self.OP.isBoundToChannel,
        )
        control_variables = {"bSimMode"}

        mapped_lists = 0
        mapped_variables = 0
        missing_source_variables: set[str] = set()
        missing_target_variables: set[str] = set()
        source_without_hw_data: set[str] = set()
        skipped_control_variables: set[str] = set()

        for gvl in gvl_by_name.values():
            sim_gvl_name = gvl.get("name")
            if not sim_gvl_name or not sim_gvl_name.endswith("_Sim"):
                continue

            source_gvl_name = sim_gvl_name[:-4]
            mapped_in_this_list = 0

            for gv in gvl.get("globals", []):
                var_name = gv.get("name")
                if not var_name:
                    continue

                if var_name in control_variables:
                    skipped_control_variables.add(f"{sim_gvl_name}.{var_name}")
                    continue

                source_full_name = f"{source_gvl_name}.{var_name}"
                target_full_name = f"{sim_gvl_name}.{var_name}"

                source_uri = self._find_variable_uri_by_full_name(source_full_name)
                if source_uri is None:
                    missing_source_variables.add(source_full_name)
                    continue

                target_uri = self._find_variable_uri_by_full_name(target_full_name)
                if target_uri is None:
                    missing_target_variables.add(target_full_name)
                    continue

                for pred in hw_predicates:
                    for obj in list(self.kg.objects(target_uri, pred)):
                        self.kg.remove((target_uri, pred, obj))

                copied = 0
                for pred in hw_predicates:
                    for obj in self.kg.objects(source_uri, pred):
                        self.kg.add((target_uri, pred, obj))
                        copied += 1

                if copied == 0:
                    source_without_hw_data.add(source_full_name)
                    continue

                mapped_variables += 1
                mapped_in_this_list += 1

            if mapped_in_this_list > 0:
                mapped_lists += 1

        return SimulationMirrorResult(
            mapped_lists=mapped_lists,
            mapped_variables=mapped_variables,
            missing_source_variables=tuple(sorted(missing_source_variables)),
            missing_target_variables=tuple(sorted(missing_target_variables)),
            source_without_hw_data=tuple(sorted(source_without_hw_data)),
            skipped_control_variables=tuple(sorted(skipped_control_variables)),
        )

    def get_or_create_plc_project(self, project_name: str) -> URIRef:
        proj_uri = self.make_uri(f"PLCProject__{project_name}")
        if (proj_uri, RDF.type, self.AG.class_PLCProject) not in self.kg:
            self.kg.add((proj_uri, RDF.type, self.AG.class_PLCProject))
            self.kg.add((proj_uri, self.DP.hasPLCProjectName, Literal(project_name)))
        return proj_uri

    def save(self) -> None:
        self.kg.serialize(self.config.kg_to_fill_path, format='turtle')
        print(f"Ingestion abgeschlossen: {self.config.kg_to_fill_path} geschrieben.")
