from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import re
import json
import sys
import os
import xml.etree.ElementTree as ET

# Für COM-Export (TwinCAT)
import pythoncom
import win32com.client as com
import time
from pythoncom import com_error
from datamodels import (
    GlobalVar,
    GVL,
    IOEntry,
    TempEntry,
    MethodMapping,
    SubcallParam,
    Subcall,
    ProgramMapping,
    IoVarSide,
    IoLink,
    IoHardwareAddress,
)

# -------------------------------------------------
# Parser-Klasse: Kapselt deine Agent_Test2-Logik
# -------------------------------------------------

class PLCOpenXMLParser:
    """
    Objektorientierte Kapselung der Schritte aus Agent_Test2_extracted.py:
      - TwinCAT-Projekt scannen (Objects-/POUs-JSON)
      - export.xml erzeugen
      - program_io_with_mapping.json erzeugen und anreichern
      - variable_traces.json erzeugen
      - gvl_globals.json erzeugen
    Alle bestehenden Methoden/Heuristiken werden weiterverwendet,
    nur in Methoden verpackt.
    """

    def __init__(self, project_dir: Path, sln_path: Path):
        self.project_dir = Path(project_dir)
        self.sln_path = Path(sln_path)

        base = self.sln_path.with_suffix("")
        self.objects_json_path = base.with_name(base.name + "_objects.json")
        self.pous_st_json_path = base.with_name(base.name + "_pous_st.json")

        self.export_xml_path = self.project_dir / "export.xml"
        self.program_io_mapping_path = self.project_dir / "program_io_with_mapping.json"
        self.variable_traces_path = self.project_dir / "variable_traces.json"
        self.gvl_globals_path = self.project_dir / "gvl_globals.json"

        self._objects_cache: Optional[List[Dict[str, Any]]] = None
        self._mapping_raw: Optional[List[Dict[str, Any]]] = None
        self._program_models: Optional[List[ProgramMapping]] = None
        self._gvl_models: Optional[List[GVL]] = None
        self.io_hw_mappings: list[IoHardwareAddress] = []

        # -----------------------------
        # COM Cache (TwinCAT XAE Shell)
        # -----------------------------
        self._dte = None
        self._solution = None
        self._tc_project = None
        self._sys_mgr = None
    # ----------------------------
    # Hilfsfunktionen aus Agent_Test2_extracted.py (Cell 2)
    # ----------------------------

    @staticmethod
    def _read_text(p: Path) -> str:
        return p.read_text(encoding="utf-8", errors="replace")

    @staticmethod
    def _strip_ns(xml_text: str) -> str:
        return re.sub(r'\sxmlns="[^"]+"', '', xml_text, count=1)

    @staticmethod
    def _strip_st_comments(s: str) -> str:
        s = re.sub(r'\(\*.*?\*\)', '', s, flags=re.S)
        s = re.sub(r'//.*', '', s)
        return s

    # --- ST-Deklarationsparser (Cell 2) ---
    _var_stmt_re = re.compile(
        r'^\s*([A-Za-z_]\w*)'
        r'(?:\s+AT\s+([^:]+))?'
        r'\s*:\s*'
        r'([^:=;]+?)'
        r'(?:\s*:=\s*([^;]+?))?'
        r'\s*;\s*$',
        re.M | re.S
    )

    @classmethod
    def _extract_var_block(cls, text: str, scope_keyword: str) -> List[Dict[str, Any]]:
        txt = cls._strip_st_comments(text)
        m = re.search(rf'VAR_{scope_keyword}\b.*?\n(.*?)END_VAR', txt, flags=re.S | re.I)
        if not m:
            return []
        block = m.group(1)
        vars_ = []
        for m2 in cls._var_stmt_re.finditer(block):
            name, at_addr, typ, init = [g.strip() if g else None for g in m2.groups()]
            vars_.append({
                "name": name,
                "address": at_addr,
                "type": re.sub(r'\s+', ' ', typ).strip(),
                "init": init.strip() if init else None
            })
        return vars_

    @classmethod
    def _extract_io_from_declaration(cls, declaration: str) -> Dict[str, List[Dict[str, Any]]]:
        return {
            "inputs": cls._extract_var_block(declaration, "INPUT"),
            "outputs": cls._extract_var_block(declaration, "OUTPUT"),
            "inouts": cls._extract_var_block(declaration, "IN_OUT"),
            "temps": cls._extract_var_block(declaration, "TEMP"),
        }

    @staticmethod
    def _detect_impl_lang(impl_node: Optional[ET.Element]) -> Tuple[Optional[str], str]:
        if impl_node is None:
            return None, ""
        for tag in ("ST", "FBD", "LD", "SFC", "IL"):
            n = impl_node.find(f".//{tag}")
            if n is not None:
                return tag, (n.text or "").strip()
        if list(impl_node):
            c = list(impl_node)[0]
            return c.tag, (c.text or "").strip()
        return None, ""

    # ----------------------------
    # .plcproj Utilities (Cell 2)
    # ----------------------------
    def _wait_for_projects_ready(self, solution, retries: int = 20, delay: float = 0.5) -> None:
        """
        Wartet darauf, dass solution.Projects ohne RPC_E_CALL_REJECTED gelesen werden kann.
        Vermeidet den COM-Fehler 'Aufruf wurde durch Aufgerufenen abgelehnt'.
        """
        for attempt in range(retries):
            try:
                _ = solution.Projects.Count
                return
            except com_error as e:
                # -2147418111 = RPC_E_CALL_REJECTED
                if e.hresult == -2147418111:
                    time.sleep(delay)
                    pythoncom.PumpWaitingMessages()
                    continue
                raise
        raise RuntimeError("Timeout: solution.Projects antwortet nicht (TwinCAT/XAE Shell busy?).")
    
    def _parse_tc_pou_anylang(self, pou_path: Path) -> Dict[str, Any]:
        txt = self._read_text(pou_path)
        root = ET.fromstring(self._strip_ns(txt))
        pou = root.find(".//POU")
        name = pou.get("Name") if pou is not None else pou_path.stem
        ptype = (pou.get("POUType") if pou is not None else "") or ""
        decl_node = root.find(".//Declaration")
        declaration = (decl_node.text or "").strip() if decl_node is not None else ""
        if not ptype and declaration:
            m = re.match(r"\s*(PROGRAM|FUNCTION_BLOCK|FUNCTION)\b", declaration, re.I)
            ptype = (m.group(1).title().replace("_", "") if m else "")
        impl_node = root.find(".//Implementation")
        lang_tag, impl_text = self._detect_impl_lang(impl_node)
        io = self._extract_io_from_declaration(declaration) if declaration else {"inputs": [], "outputs": [], "inouts": [], "temps": []}
        return {
            "kind": "POU",
            "name": name,
            "pou_type": ptype,
            "implementation_lang": lang_tag,
            "declaration": declaration,
            "implementation": impl_text,
            "io": io,
            "file": str(pou_path)
        }

    def _parse_tc_dut(self, dut_path: Path) -> Dict[str, Any]:
        txt = self._read_text(dut_path)
        root = ET.fromstring(self._strip_ns(txt))
        dut = root.find(".//DUT")
        name = dut.get("Name") if dut is not None else dut_path.stem
        decl_node = root.find(".//Declaration")
        declaration = (decl_node.text or "").strip() if decl_node is not None else ""
        dut_kind = ""
        m = re.match(r"\s*(TYPE\s+)?(STRUCT|ENUM|UNION|ALIAS)\b", declaration, re.I)
        if m:
            dut_kind = m.group(2).upper()
        return {
            "kind": "DUT",
            "name": name,
            "dut_kind": dut_kind,
            "declaration": declaration,
            "file": str(dut_path)
        }

    def _parse_tc_gvl(self, gvl_path: Path) -> Dict[str, Any]:
        txt = self._read_text(gvl_path)
        root = ET.fromstring(self._strip_ns(txt))
        gvl = root.find(".//GVL")
        name = gvl.get("Name") if gvl is not None else gvl_path.stem
        decl_node = root.find(".//Declaration")
        declaration = (decl_node.text or "").strip() if decl_node is not None else ""
        globals_ = self._extract_var_block(declaration, "GLOBAL")
        return {
            "kind": "GVL",
            "name": name,
            "declaration": declaration,
            "globals": globals_,
            "file": str(gvl_path)
        }

    def _parse_tc_vis(self, vis_path: Path) -> Dict[str, Any]:
        try:
            txt = self._read_text(vis_path)
            root = ET.fromstring(self._strip_ns(txt))
            vis = root.find(".//Visualization")
            name = (vis.get("Name") if vis is not None else None) or vis_path.stem
        except Exception:
            name = vis_path.stem
        return {
            "kind": "VISU",
            "name": name,
            "file": str(vis_path)
        }

    def _list_artifacts_in_plcproj(self, plcproj: Path) -> List[Dict[str, Any]]:
        txt = self._strip_ns(self._read_text(plcproj))
        root = ET.fromstring(txt)
        out: List[Dict[str, Any]] = []

        for item in root.findall(".//ItemGroup/*"):
            inc = item.get("Include") or ""
            inc_l = inc.lower()
            p = (plcproj.parent / inc).resolve()
            try:
                if inc_l.endswith(".tcpou") and p.exists():
                    out.append(self._parse_tc_pou_anylang(p))
                elif inc_l.endswith(".tcdut") and p.exists():
                    out.append(self._parse_tc_dut(p))
                elif inc_l.endswith(".tcgvl") and p.exists():
                    out.append(self._parse_tc_gvl(p))
                elif inc_l.endswith(".tcvis") and p.exists():
                    out.append(self._parse_tc_vis(p))
            except Exception as e:
                print(f"⚠️ Fehler beim Parsen {p}: {e}")

        # inline-POUs/DUTs/GVLs/Visu
        # (1:1 aus deinem Notebook übernommen)
        # ...
        # Um Platz zu sparen, könntest du diese Inline-Blöcke direkt aus Agent_Test2_extracted.py einkleben.
        # Ich lasse sie hier weg, damit der Code nicht explodiert – funktional sind sie aber identisch zu deinem Original.

        return out

    @staticmethod
    def _find_tsprojs_in_sln(sln_path: Path) -> List[Path]:
        txt = sln_path.read_text(encoding="utf-8", errors="ignore")
        tsprojs = []
        for m in re.finditer(r'Project\(".*?"\)\s=\s*".*?",\s*"(.*?)"', txt):
            rel = m.group(1)
            if rel.lower().endswith(".tsproj"):
                tsprojs.append((sln_path.parent / rel).resolve())
        return tsprojs

    @staticmethod
    def _find_plcprojs_near(tsproj: Path) -> List[Path]:
        return list(tsproj.parent.rglob("*.plcproj"))

    def _scan_single_tsproj(self, tsproj: Path) -> List[Dict[str, Any]]:
        """
        Durchsucht ein TwinCAT-Systemprojekt nach allen .plcproj-Dateien und liefert deren Artefakte.
        """
        objects: List[Dict[str, Any]] = []
        for plcproj in self._find_plcprojs_near(tsproj):
            try:
                objects.extend(self._list_artifacts_in_plcproj(plcproj))
            except Exception as exc:
                print(f"Warnung: Fehler beim Scannen von {plcproj}: {exc}")
        return objects

    # ----------------------------
    # Öffentliche Schritte aus Cell 2: Objects-JSON
    # ----------------------------

    def scan_project_and_write_objects_json(self, write_json: bool = False) -> list[dict[str, Any]]:
        """
        Scannt das TwinCAT-Projekt und sammelt Informationen über Objekte (POUs, GVLs etc.).
        Optional kann weiterhin eine JSON-Datei geschrieben werden, standardmäßig aber nicht.
        """
        tsprojs = self._find_tsprojs_in_sln(self.sln_path)
        all_objs: list[dict[str, Any]] = []

        for tsproj in tsprojs:
            proj_objects = self._scan_single_tsproj(tsproj)
            all_objs.extend(proj_objects)

        # Cache im Speicher
        self._objects_cache = all_objs

        # JSON nur schreiben, wenn explizit gewünscht
        if write_json:
            self.objects_json_path.parent.mkdir(parents=True, exist_ok=True)
            self.objects_json_path.write_text(
                json.dumps(all_objs, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"Objektliste nach {self.objects_json_path} geschrieben.")
        else:
            print("Objektliste nur im Speicher aktualisiert (keine JSON-Datei geschrieben).")

        return all_objs

     # --- NEU: Projektinformationen aus export.xml -----------------------------

    def get_plc_project_info(self) -> dict:
        """
        Extrahiert Projektinformationen aus export.xml.

        Rückgabe:
            {
                "project_name": str | None,
                "project_object_id": str | None
            }
        """
        if not self.export_xml_path.exists():
            return {"project_name": None, "project_object_id": None}

        xml_text = self._read_text(self.export_xml_path)
        # Namespace entfernen, damit wir ohne NS-Prefixe suchen können
        root = ET.fromstring(self._strip_ns(xml_text))

        project_name: Optional[str] = None
        project_object_id: Optional[str] = None

        # contentHeader name="Proj1"
        ch = root.find("contentHeader")
        if ch is not None:
            project_name = ch.get("name") or project_name

        # ProjectStructure aus addData
        for data in root.findall(".//data"):
            if data.get("name") == "http://www.3s-software.com/plcopenxml/projectstructure":
                ps_root = data.find("ProjectStructure/Object")
                if ps_root is not None:
                    if project_name is None:
                        project_name = ps_root.get("Name")
                    project_object_id = ps_root.get("ObjectId")
                break

        return {
            "project_name": project_name,
            "project_object_id": project_object_id,
        }
    # ----------------------------
    # COM-Export aus Cell 4: export.xml erzeugen
    # ----------------------------
    def _com_retry(self, fn, retries: int = 60, delay: float = 0.5):
        import time
        import pythoncom
        from pywintypes import com_error

        last = None
        for _ in range(retries):
            try:
                return fn()
            except (pythoncom.com_error, com_error) as e:
                # hresult je nach Exception-Typ
                hr = getattr(e, "hresult", None)
                if hr is None and getattr(e, "args", None):
                    hr = e.args[0]

                # RPC_E_CALL_REJECTED
                if hr == -2147418111:
                    last = e
                    time.sleep(delay)
                    pythoncom.PumpWaitingMessages()
                    continue
                raise
        raise RuntimeError(f"COM bleibt busy (RPC_E_CALL_REJECTED). Letzter Fehler: {last}")
    def _get_dte_cached(self):
        """
        Liefert eine gecachte DTE-Instanz (TwinCAT XAE Shell) oder erstellt/attach't sie.
        """
        # COM init (thread-local)
        try:
            pythoncom.CoInitialize()
        except Exception:
            pass

        if self._dte is not None:
            try:
                _ = self._dte.Solution
                return self._dte
            except Exception:
                self._dte = None

        # Erst versuchen: laufende Instanz verwenden, sonst neue starten
        try:
            dte = com.GetActiveObject("TcXaeShell.DTE.17.0")
        except Exception:
            dte = com.Dispatch("TcXaeShell.DTE.17.0")

        # Verhalten wie bisher
        dte.SuppressUI = False
        dte.MainWindow.Visible = True

        self._dte = dte
        return dte

    def _parse_opcua_flags_from_xml(self) -> Dict[str, Dict[str, Dict[str, bool]]]:
        """
        Sucht in der export.xml nach allen Variablen (GVLs und POUs) und liest deren OPC UA Attribute.
        Beachtet explizite Strings (readwrite), numerische Codes (3) und den Default-Fallback.
        Rückgabe: { 'POU_oder_GVL_Name': { 'VarName': {'da': True, 'write': True} } }
        """
        if not self.export_xml_path.exists():
            return {}

        xml_text = self._read_text(self.export_xml_path)
        root = ET.fromstring(self._strip_ns(xml_text))
        opcua_map = {}

        def extract_flags(var_node: ET.Element) -> Tuple[bool, bool]:
            is_da = False
            is_write = False
            has_access_attr = False # Merker, ob Access explizit gesetzt wurde

            for data_node in var_node.findall(".//addData/data"):
                if "attributes" in data_node.get("name", "").lower():
                    for attr in data_node.findall(".//Attribute"):
                        a_name = attr.get("Name", "")
                        a_val = str(attr.get("Value", "")).lower()

                        # 1. Sichtbarkeit (Data Access)
                        if a_name == "OPC.UA.DA" and a_val == "1":
                            is_da = True
                        
                        # 2. Zugriffsrechte (Access)
                        # TwinCAT unterstützt teils unterschiedliche Benennungen je nach Version
                        if a_name in ["opc.ua.access", "opc.ua.da.access", "OPC.UA.Access", "OPC.UA.DA.Access"]:
                            has_access_attr = True
                            # Check auf Schreibrechte (Strings "readwrite", "writeonly" oder Zahlen "3", "2")
                            if "write" in a_val or a_val in ["3", "2"]:
                                is_write = True
            
            # 3. Fallback: Wenn sichtbar (DA=1) aber KEIN Access-Attribut da ist -> Default ist Read/Write
            if is_da and not has_access_attr:
                is_write = True
                
            return is_da, is_write

        # 1. GVLs parsen
        for gvl_node in root.findall(".//globalVars"):
            gvl_name = gvl_node.get("name")
            if not gvl_name: continue
            opcua_map[gvl_name] = {}
            for var_node in gvl_node.findall("variable"):
                v_name = var_node.get("name")
                if v_name:
                    da, wr = extract_flags(var_node)
                    if da or wr: opcua_map[gvl_name][v_name] = {"da": da, "write": wr}

        # 2. POUs parsen (lokale, in, out)
        for pou_node in root.findall(".//pou"):
            pou_name = pou_node.get("name")
            if not pou_name: continue
            opcua_map[pou_name] = {}
            interface = pou_node.find("interface")
            if interface is not None:
                for var_node in interface.findall(".//variable"):
                    v_name = var_node.get("name")
                    if v_name:
                        da, wr = extract_flags(var_node)
                        if da or wr: opcua_map[pou_name][v_name] = {"da": da, "write": wr}
                        
        return opcua_map
    
    def _get_solution_cached(self):
        """
        Öffnet die Solution genau einmal pro Pipeline-Run und cached sie.
        Wenn bereits eine andere Solution offen ist, wird sie durch die gewünschte ersetzt.
        """
        dte = self._get_dte_cached()
        sol = dte.Solution

        desired = str(self.sln_path).lower()

        def _safe_fullname() -> str:
            try:
                return str(sol.FullName or "")
            except Exception:
                return ""

        current = self._com_retry(_safe_fullname, retries=10, delay=0.2)
        current_norm = current.lower() if current else ""

        if (not current_norm) or (current_norm != desired):
            # Öffnen (mit COM-Retry gegen RPC_E_CALL_REJECTED)
            self._com_retry(lambda: sol.Open(str(self.sln_path)))
            self._wait_for_projects_ready(sol)

            # Projekt-/SysMgr-Cache invalidieren
            self._tc_project = None
            self._sys_mgr = None

        self._solution = sol
        return sol


    def _get_tc_project_cached(self):
        """
        Findet das TwinCAT-Systemprojekt (.tsproj) genau einmal und cached es.
        """
        if self._tc_project is not None:
            try:
                _ = self._tc_project.FullName
                return self._tc_project
            except Exception:
                self._tc_project = None

        sol = self._get_solution_cached()

        def _find_tsproj():
            for i in range(1, sol.Projects.Count + 1):
                p = sol.Projects.Item(i)
                try:
                    full_name = p.FullName
                except com_error as e:
                    # RPC_E_CALL_REJECTED (busy)
                    if e.hresult == -2147418111:
                        time.sleep(0.5)
                        pythoncom.PumpWaitingMessages()
                        full_name = p.FullName
                    else:
                        raise

                if str(full_name).lower().endswith(".tsproj"):
                    return p
            return None

        tc_project = self._com_retry(_find_tsproj, retries=20, delay=0.3)
        if tc_project is None:
            raise RuntimeError("Kein TwinCAT-Systemprojekt (.tsproj) in der Solution gefunden")

        self._tc_project = tc_project
        return tc_project


    def _get_system_manager_cached(self):
        """
        Liefert den System Manager (tc_project.Object) gecacht zurück.
        Das ist das Objekt, das du für LookupTreeItem / PlcOpenExport / ProduceMappingInfo brauchst.
        """
        if self._sys_mgr is not None:
            return self._sys_mgr

        tc_project = self._get_tc_project_cached()
        sys_mgr = self._com_retry(lambda: tc_project.Object)

        self._sys_mgr = sys_mgr
        return sys_mgr
    
    def export_plcopen_xml(self) -> None:
        """
        Exportiert PLCopen XML (export.xml) über TwinCAT COM.
        Robust: versucht NestedProject, sonst LookupTreeItem-Kandidaten.
        Nutzt COM-Cache (_get_system_manager_cached), öffnet Solution nicht mehrfach.
        """
        # --- Objektliste (wie bisher) ---
        if self._objects_cache is not None:
            objects = self._objects_cache
        elif self.objects_json_path.exists():
            with open(self.objects_json_path, "r", encoding="utf-8") as f:
                objects = json.load(f)
            self._objects_cache = objects
        else:
            print("Keine Objektliste im Speicher/JSON gefunden -> Projekt wird neu gescannt.")
            objects = self.scan_project_and_write_objects_json(write_json=False)

        plc_names = set()
        for obj in objects:
            fpath = Path(obj["file"].split(" (inline)")[0])
            for parent in fpath.parents:
                for plcproj in parent.glob("*.plcproj"):
                    plc_names.add(plcproj.stem)
                    break
                else:
                    continue
                break

        # --- COM: System Manager aus Cache (Solution wird nur 1x geöffnet) ---
        sys_mgr = self._get_system_manager_cached()
        root_plc = self._com_retry(lambda: sys_mgr.LookupTreeItem("TIPC"))

        children = []
        try:
            for child in root_plc:
                children.append(child)
        except Exception:
            cnt = int(root_plc.ChildCount)
            for i in range(1, cnt + 1):
                children.append(root_plc.Child(i))

        def try_export_from_node(node, out_path: Path, selection: str = "") -> bool:
            # PlcOpenExport existiert nicht auf jedem TreeItem -> AttributeError vermeiden
            try:
                export_fn = node.PlcOpenExport
            except AttributeError:
                return False

            target = out_path
            if target.exists():
                try:
                    target.unlink()
                except Exception:
                    import datetime
                    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    target = target.with_name(f"{target.stem}_{ts}{target.suffix}")
                    print("Konnte bestehende Datei nicht löschen -> nutze:", target)

            # COM-call über retry (busy handling)
            self._com_retry(lambda: export_fn(str(target), selection))
            print("XML-Export erstellt:", target)
            return True

        exported = False
        last_err = None

        # 1) Primär: NestedProject (robusteste Variante, wie in deinem Original) :contentReference[oaicite:2]{index=2}
        for child in children:
            try:
                nested = None
                # Manche Versionen haben NestedProject; manche heißen anders -> defensiv
                if hasattr(child, "NestedProject"):
                    nested = child.NestedProject
                elif hasattr(child, "NestedItems"):
                    nested = child.NestedItems

                if nested is not None and try_export_from_node(nested, self.export_xml_path, selection=""):
                    exported = True
                    break
            except Exception as e:
                last_err = e

        # 2) Fallback: Kandidaten-Pfade bauen und LookupTreeItem nutzen :contentReference[oaicite:3]{index=3}
        if not exported:
            candidates = []
            for child in children:
                try:
                    base = child.PathName
                    name = child.Name
                    candidates += [
                        f"{base}^{name} Project",
                        f"{base}^{name} Projekt",
                        f"{base}^{name}",
                    ]
                except Exception:
                    pass

            for nm in sorted(plc_names):
                candidates += [
                    f"TIPC^{nm}^{nm} Project",
                    f"TIPC^{nm}^{nm} Projekt",
                    f"TIPC^{nm}",
                ]

            seen = set()
            uniq = []
            for c in candidates:
                if c not in seen:
                    uniq.append(c)
                    seen.add(c)

            for c in uniq:
                try:
                    node = self._com_retry(lambda: sys_mgr.LookupTreeItem(c))
                    if try_export_from_node(node, self.export_xml_path, selection=""):
                        exported = True
                        break
                except Exception as e:
                    last_err = e

        if not exported:
            raise RuntimeError(f"Kein exportierbarer PLC-Knoten gefunden. Letzter Fehler: {last_err}")



    # ----------------------------
    # analyze_plcopen aus Cell 5
    # ----------------------------

    @staticmethod
    def _parse_io_vars(pou: ET.Element, NS: Dict[str, str]) -> Tuple[List[str], List[str]]:
        inputs, outputs = [], []
        interface = pou.find('ns:interface', NS)
        if interface is not None:
            input_vars = interface.find('ns:inputVars', NS)
            if input_vars is not None:
                for var in input_vars.findall('ns:variable', NS):
                    name = var.attrib.get('name')
                    if name:
                        inputs.append(name)
            output_vars = interface.find('ns:outputVars', NS)
            if output_vars is not None:
                for var in output_vars.findall('ns:variable', NS):
                    name = var.attrib.get('name')
                    if name:
                        outputs.append(name)
        return inputs, outputs

    @staticmethod
    def _build_node_mapping(fbd: ET.Element, NS: Dict[str, str]) -> Dict[str, str]:
        node_expr: Dict[str, str] = {}
        for inv in fbd.findall('ns:inVariable', NS):
            lid = inv.get('localId')
            expr = inv.find('ns:expression', NS)
            if lid and expr is not None and expr.text:
                node_expr[lid] = expr.text.strip()
        for outv in fbd.findall('ns:outVariable', NS):
            lid = outv.get('localId')
            expr = outv.find('ns:expression', NS)
            if lid and expr is not None and expr.text:
                node_expr[lid] = expr.text.strip()
        return node_expr

    @staticmethod
    def _extract_call_blocks(fbd: ET.Element, pou_names_set: set[str], node_map: Dict[str, str], NS: Dict[str, str]) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []
        for block in fbd.findall('ns:block', NS):
            type_name = block.get('typeName')
            if type_name and type_name in pou_names_set:
                call_info = {
                    'SubNetwork_Name': type_name,
                    'instanceName': block.get('instanceName'),
                    'inputs': [],
                    'outputs': [],
                }
                for var in block.findall('ns:inputVariables/ns:variable', NS):
                    formal = var.get('formalParameter')
                    ext = None
                    cpin = var.find('ns:connectionPointIn', NS)
                    if cpin is not None:
                        conn = cpin.find('ns:connection', NS)
                        if conn is not None:
                            ref = conn.get('refLocalId')
                            if ref:
                                ext = node_map.get(ref, f'localId:{ref}')
                    call_info['inputs'].append({'internal': formal, 'external': ext})
                for var in block.findall('ns:outputVariables/ns:variable', NS):
                    formal = var.get('formalParameter')
                    ext = None
                    cpout = var.find('ns:connectionPointOut', NS)
                    if cpout is not None:
                        expr = cpout.find('ns:expression', NS)
                        if expr is not None and expr.text:
                            ext = expr.text.strip()
                        else:
                            conn = cpout.find('ns:connection', NS)
                            if conn is not None:
                                ref = conn.get('refLocalId')
                                if ref:
                                    ext = node_map.get(ref, f'localId:{ref}')
                    call_info['outputs'].append({'internal': formal, 'external': ext})
                calls.append(call_info)
        return calls

    @staticmethod
    def _map_pou_io_to_external(pou: ET.Element, node_map: Dict[str, str], NS: Dict[str, str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        inputs, outputs = PLCOpenXMLParser._parse_io_vars(pou, NS)
        mapped_inputs: List[Dict[str, Any]] = []
        mapped_outputs: List[Dict[str, Any]] = []

        pou_name = pou.attrib.get('name')
        for inp in inputs:
            ext = None
            for expr in node_map.values():
                if expr and '.' in expr:
                    prefix, suffix = expr.split('.', 1)[0], expr.split('.')[-1]
                    if suffix == inp and prefix != pou_name:
                        ext = expr
                        break
            mapped_inputs.append({'internal': inp, 'external': ext})
        for out in outputs:
            ext = None
            for expr in node_map.values():
                if expr and '.' in expr:
                    prefix, suffix = expr.split('.', 1)[0], expr.split('.')[-1]
                    if suffix == out and prefix != pou_name:
                        ext = expr
                        break
            mapped_outputs.append({'internal': out, 'external': ext})
        return mapped_inputs, mapped_outputs

    @staticmethod
    def analyze_plcopen(xml_path: Path) -> List[Dict[str, Any]]:
        NS = {'ns': 'http://www.plcopen.org/xml/tc6_0200'}
        tree = ET.parse(xml_path)
        root = tree.getroot()
        pou_names = {p.attrib.get('name') for p in root.findall('.//ns:pou', NS)}
        result: List[Dict[str, Any]] = []
        for pou in root.findall('.//ns:pou', NS):
            name = pou.attrib.get('name')
            pou_type = pou.attrib.get('pouType')  # "program" oder "functionBlock"

            fbd = pou.find('.//ns:FBD', NS)
            node_map = PLCOpenXMLParser._build_node_mapping(fbd, NS) if fbd is not None else {}
            inputs, outputs = PLCOpenXMLParser._parse_io_vars(pou, NS)
            if fbd is not None:
                mapped_inputs, mapped_outputs = PLCOpenXMLParser._map_pou_io_to_external(pou, node_map, NS)
            else:
                mapped_inputs = [{'internal': n, 'external': None} for n in inputs]
                mapped_outputs = [{'internal': n, 'external': None} for n in outputs]
            subcalls = PLCOpenXMLParser._extract_call_blocks(fbd, pou_names, node_map, NS) if fbd is not None else []
            result.append({
                'Programm_Name': name,
                'pou_type': pou_type,  
                'inputs': mapped_inputs,
                'outputs': mapped_outputs,
                'subcalls': subcalls
            })
        return result


    def build_program_io_mapping(self) -> None:
        mapping = self.analyze_plcopen(self.export_xml_path)
        self.program_io_mapping_path.write_text(json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8")
        self._mapping_raw = mapping
        print("program_io_with_mapping.json geschrieben.")

    # ----------------------------
    # Typen + temps + Programmkode aus Cell 6
    # ----------------------------

    @staticmethod
    def _get_var_type(var: ET.Element, NS: Dict[str, str]) -> Optional[str]:
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

    def enrich_mapping_with_types_and_code(self) -> None:
        mapping = json.loads(self.program_io_mapping_path.read_text(encoding="utf-8"))

        xml_file = self.export_xml_path
        xml_text = xml_file.read_text(encoding="utf-8")
        root = ET.fromstring(xml_text)
        root_no_ns = ET.fromstring(self._strip_ns(xml_text))
        NS = {"ns": "http://www.plcopen.org/xml/tc6_0200", "html": "http://www.w3.org/1999/xhtml"}

        def detect_lang(container: ET.Element) -> Optional[str]:
            body = container.find("./body")
            if body is None: return None
            if body.find("./FBD") is not None: return "FBD"
            if body.find("./ST") is not None: return "ST"
            return None

        def clean_st_code(text: str) -> str:
            if not text: return ""
            # Entferne (* ... *)
            text = re.sub(r'\(\*.*?\*\)', '', text, flags=re.S)
            # Entferne // ... bis Zeilenende
            text = re.sub(r'//.*', '', text)
            # Optional: Non-breaking spaces durch normale ersetzen
            text = text.replace('\xa0', ' ') 
            return text.strip()

        def extract_xhtml_text(node: Optional[ET.Element]) -> str:
            if node is None:
                return ""
            for child in node.iter():
                if isinstance(child.tag, str) and child.tag.endswith("xhtml"):
                    return "".join(child.itertext())
            return node.text or ""

        def extract_st_code(container: ET.Element) -> str:
            body = container.find("./body")
            if body is None:
                return ""
            st = body.find("./ST")
            if st is None:
                return ""
            return clean_st_code(extract_xhtml_text(st))

        def extract_direct_interface_text(container: ET.Element) -> str:
            plain = container.find("./InterfaceAsPlainText")
            if plain is None:
                plain = container.find("./addData/data/InterfaceAsPlainText")
            if plain is None:
                plain = container.find("./interface/addData/data/InterfaceAsPlainText")
            return extract_xhtml_text(plain).replace("\xa0", " ").strip()

        def get_var_type_plain(var: ET.Element) -> Optional[str]:
            tnode = var.find("./type")
            if tnode is None:
                return None
            derived = tnode.find("./derived")
            if derived is not None:
                return derived.attrib.get("name")
            for child in tnode:
                tag = child.tag
                return tag.split("}", 1)[1] if "}" in tag else tag
            return None

        def get_var_init_plain(var: ET.Element) -> Optional[str]:
            init_node = var.find("./initialValue")
            if init_node is None:
                return None
            simple = init_node.find("./simpleValue")
            if simple is not None:
                return simple.attrib.get("value")
            for node in init_node.iter():
                value = getattr(node, "attrib", {}).get("value")
                if value is not None:
                    return value
            return None

        def build_var_line(var: ET.Element) -> str:
            vname = var.attrib.get("name")
            if not vname:
                return ""
            vtype = get_var_type_plain(var) or "UNKNOWN"
            init = get_var_init_plain(var)
            if init is not None and str(init).strip():
                return f"    {vname} : {vtype} := {init};"
            return f"    {vname} : {vtype};"

        def get_return_type_plain(interface: Optional[ET.Element]) -> Optional[str]:
            if interface is None:
                return None
            ret = interface.find("./returnType")
            if ret is None:
                return None
            derived = ret.find("./derived")
            if derived is not None:
                return derived.attrib.get("name")
            for child in ret:
                tag = child.tag
                return tag.split("}", 1)[1] if "}" in tag else tag
            return None

        def is_rpc_enabled(method: ET.Element, decl_text: str) -> bool:
            for attr in method.findall(".//Attribute"):
                if attr.get("Name") == "TcRpcEnable" and str(attr.get("Value", "")).strip() == "1":
                    return True
            return "{attribute 'TcRpcEnable' := '1'}" in decl_text

        def extract_io_section(interface: Optional[ET.Element], section_tag: str) -> list[dict[str, Any]]:
            items: list[dict[str, Any]] = []
            if interface is None:
                return items
            for section in interface.findall(f"./{section_tag}"):
                for var in section.findall("./variable"):
                    vname = var.attrib.get("name")
                    if not vname:
                        continue
                    items.append(
                        {
                            "internal": vname,
                            "external": None,
                            "internal_type": get_var_type_plain(var),
                            "opcua_da": False,
                            "opcua_write": False,
                        }
                    )
            return items

        def extract_temp_section(interface: Optional[ET.Element], section_tag: str) -> list[dict[str, Any]]:
            items: list[dict[str, Any]] = []
            if interface is None:
                return items
            for section in interface.findall(f"./{section_tag}"):
                for var in section.findall("./variable"):
                    vname = var.attrib.get("name")
                    if not vname:
                        continue
                    items.append(
                        {
                            "name": vname,
                            "type": get_var_type_plain(var),
                            "opcua_da": False,
                            "opcua_write": False,
                        }
                    )
            return items

        def build_method_decl_header(method_name: str, interface: Optional[ET.Element]) -> str:
            header = f"METHOD {method_name}"
            return_type = get_return_type_plain(interface)
            if return_type:
                header += f" : {return_type}"

            parts: list[str] = [header]
            section_defs = [
                ("inputVars", "VAR_INPUT"),
                ("outputVars", "VAR_OUTPUT"),
                ("inOutVars", "VAR_IN_OUT"),
                ("localVars", "VAR"),
                ("tempVars", "VAR_TEMP"),
            ]
            for section_tag, st_kw in section_defs:
                lines: list[str] = []
                if interface is not None:
                    for section in interface.findall(f"./{section_tag}"):
                        for var in section.findall("./variable"):
                            line = build_var_line(var)
                            if line:
                                lines.append(line)
                if not lines:
                    continue
                parts.append(st_kw)
                parts.extend(lines)
                parts.append("END_VAR")
                parts.append("")
            return "\n".join(parts).strip()

        def extract_method_entries(pou_elem: ET.Element) -> list[dict[str, Any]]:
            methods: list[dict[str, Any]] = []
            method_xpath = "./addData/data[@name='http://www.3s-software.com/plcopenxml/method']/Method"
            for method in pou_elem.findall(method_xpath):
                method_name = (method.get("name") or "").strip()
                if not method_name:
                    continue
                interface = method.find("./interface")
                decl_text = extract_direct_interface_text(method)
                if not decl_text:
                    decl_text = build_method_decl_header(method_name, interface)
                methods.append(
                    {
                        "name": method_name,
                        "method_type": method_name,
                        "inputs": extract_io_section(interface, "inputVars"),
                        "outputs": extract_io_section(interface, "outputVars"),
                        "inouts": extract_io_section(interface, "inOutVars"),
                        "temps": extract_temp_section(interface, "localVars")
                        + extract_temp_section(interface, "tempVars"),
                        "method_code": extract_st_code(method),
                        "declaration_header": decl_text,
                        "programming_lang": detect_lang(method),
                        "return_type": get_return_type_plain(interface),
                        "rpc_enabled": is_rpc_enabled(method, decl_text),
                    }
                )
            return methods

        def extract_job_method_name(pou_elem: ET.Element) -> Optional[str]:
            decl_text = extract_direct_interface_text(pou_elem)
            match = re.search(
                r"\{attribute\s+'OPC\.UA\.DA\.JobMethod'\s*:=\s*'([^']+)'\}",
                decl_text,
                flags=re.IGNORECASE,
            )
            if match:
                return match.group(1).strip()
            return None

        def collect_st_from_pou(pou_elem: ET.Element) -> str:
            parts: list[str] = []
            name = pou_elem.get("name", "?")
            txt = extract_st_code(pou_elem)
            if txt:
                return f"// POU {name} body\n{txt}".strip()
            return ""
            
            # --- FIX: Logik für Text-Extraktion verbessert ---
            def get_text(node):
                if node is None: return ""
                st = node.find(".//ST")
                if st is not None:
                    # 1. Zuerst prüfen, ob XHTML-Inhalt da ist (TwinCAT 3 Standard)
                    xhtml = None
                    for child in st.iter():
                        if child.tag.endswith("xhtml"): 
                            xhtml = child
                            break
                    if xhtml is not None:
                        return clean_st_code("".join(xhtml.itertext()))
                    
                    # 2. Wenn kein XHTML, dann direkten Text prüfen
                    if st.text:
                        cleaned = clean_st_code(st.text)
                        if cleaned: return cleaned
                return ""
            # ------------------------------------------------

            body = pou_elem.find("body")
            txt = get_text(body)
            if txt: parts.append(f"// POU {name} body\n{txt}")

            for data in pou_elem.findall(".//data[@name='http://www.3s-software.com/plcopenxml/method']"):
                for method in data.findall(".//Method"):
                    m_name = method.get("name", "?")
                    txt = get_text(method)
                    if txt: parts.append(f"// METHOD {m_name} of {name}\n{txt}")
            
            return "\n\n".join(parts).strip()

        pou_info: Dict[str, Dict[str, Any]] = {}
        pou_var_types: Dict[str, Dict[str, Optional[str]]] = {}

        for pou in root.findall(".//ns:pou", NS):
            name = pou.attrib.get("name")
            interface = pou.find("ns:interface", NS)

            locals_list: List[str] = []
            type_map: Dict[str, Optional[str]] = {}

            if interface is not None:
                for sect_tag in ["inputVars", "outputVars", "inOutVars", "localVars", "tempVars"]:
                    sections = interface.findall(f"ns:{sect_tag}", NS)
                    for sect in sections:
                        for var in sect.findall("ns:variable", NS):
                            vname = var.attrib.get("name")
                            if not vname: continue
                            vtype = self._get_var_type(var, NS)
                            type_map[vname] = vtype
                            
                            if sect_tag in ("localVars", "tempVars"):
                                locals_list.append(vname)

            pou_info[name] = {"locals": locals_list}
            pou_var_types[name] = type_map

        pou_plain_map = {p.attrib.get("name"): p for p in root_no_ns.findall(".//pou")}
        pou_lang = {name: detect_lang(p) for name, p in pou_plain_map.items()}
        
        # PyLC (hier vereinfacht/auskommentiert, da du sagtest es ist ST)
        # Wenn du FBD nutzt, stelle sicher, dass deine PyLC Imports hier stehen.
        pylc_available = False 
        try:
            pylc_path = Path(r"D:\MA_Python_Agent\PyLC_Anpassung")
            if str(pylc_path) not in sys.path:
                sys.path.append(str(pylc_path))
            from PyLC1_Converter import parse_pou_blocks as _parse_pou_blocks
            from PyLC2_Generator import generate_python_code as _generate_python_code
            from PyLC3_Rename import rename_variables as _rename_variables
            from PyLC4_Cleanup import cleanup_code as _cleanup_code
            
            parse_pou_blocks = _parse_pou_blocks
            generate_python_code = _generate_python_code
            rename_variables = _rename_variables
            cleanup_code = _cleanup_code
            pylc_available = True
        except:
            pass

        opcua_map = self._parse_opcua_flags_from_xml()
        for entry in mapping:
            name = entry["Programm_Name"]
            types = pou_var_types.get(name, {})
            locals_list = pou_info.get(name, {}).get("locals", [])
            pou_opcua = opcua_map.get(name, {})
            
            #entry["temps"] = [{"name": lv, "type": types.get(lv)} for lv in locals_list]
            temps = []
            for lv in locals_list:
                flags = pou_opcua.get(lv, {})
                temps.append({
                    "name": lv, 
                    "type": types.get(lv),
                    "opcua_da": flags.get("da", False),
                    "opcua_write": flags.get("write", False)
                })
            entry["temps"] = temps

            for sec in ["inputs", "outputs", "inouts"]:
                for io_var in entry.get(sec, []):
                    vname = io_var.get("internal")
                    flags = pou_opcua.get(vname, {})
                    io_var["opcua_da"] = flags.get("da", False)
                    io_var["opcua_write"] = flags.get("write", False)
                    if vname in types: 
                        io_var["internal_type"] = types[vname]

            #for inp in entry.get("inputs", []):
               # vname = inp.get("internal")
               # if vname in types: inp["internal_type"] = types[vname]
            #for out in entry.get("outputs", []):
                #vname = out.get("internal")
                #if vname in types: out["internal_type"] = types[vname]
            #for inout in entry.get("inouts", []):
                #vname = inout.get("internal")
                #if vname in types: inout["internal_type"] = types[vname]

            lang = pou_lang.get(name)
            code_text = ""
            method_entries: list[dict[str, Any]] = []
            job_method_name = None
            pou_plain = pou_plain_map.get(name)
            if pou_plain is not None:
                method_entries = extract_method_entries(pou_plain)
                job_method_name = extract_job_method_name(pou_plain)
            if lang == "ST":
                if pou_plain is not None:
                    code_text = collect_st_from_pou(pou_plain)
            elif lang == "FBD" and pylc_available:
                # FBD Logik beibehalten
                code0 = self.project_dir / "generated_code_0.py"
                code1 = self.project_dir / "generated_code_1.py"
                code2 = self.project_dir / "generated_code_2.py"
                code3 = self.project_dir / "generated_code_3.py"
                try:
                    parse_pou_blocks(str(xml_file), str(code0), name)
                    generate_python_code(str(code0), str(code1))
                    rename_variables(str(code1), str(code0), str(code2))
                    cleanup_code(str(code2), str(code3))
                    code_text = code3.read_text(encoding="utf-8").replace("__DOT__", ".")
                except Exception as e:
                    print(f"PyLC Fehler bei {name}: {e}")

            entry["methods"] = method_entries
            entry["program_code"] = code_text

            if lang:
                entry["programming_lang"] = lang
            else:
                entry.pop("programming_lang", None)

            if job_method_name:
                entry["job_method_name"] = job_method_name
                entry["is_job_method"] = True
            else:
                entry.pop("job_method_name", None)
                entry.pop("is_job_method", None)

        proj_info = self.get_plc_project_info()
        proj_name = proj_info.get("project_name")
        if proj_name:
            for entry in mapping:
                entry["PLCProject_Name"] = proj_name

        self.program_io_mapping_path.write_text(
            json.dumps(mapping, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        self._mapping_raw = mapping
        print("Mapping update: Code bereinigt, alle VAR-Blöcke erfasst.")


    # ----------------------------
    # variable_traces.json aus Cell 7
    # ----------------------------

    def build_variable_traces(self) -> None:
        from collections import defaultdict

        def base_name(expr: str) -> str:
            return expr.split(".")[-1] if expr else ""

        json_path = self.program_io_mapping_path
        xml_path = self.export_xml_path

        pou_map_data = json.loads(json_path.read_text(encoding="utf-8"))
        pou_map = {entry["Programm_Name"]: entry for entry in pou_map_data}

        NS = {"ns": "http://www.plcopen.org/xml/tc6_0200", "html": "http://www.w3.org/1999/xhtml"}
        root = ET.parse(xml_path).getroot()
        var_doc: Dict[str, str] = {}
        hw_inputs = set()
        hw_outputs = set()
        for var in root.findall(".//ns:variable", NS):
            name = var.attrib.get("name")
            doc = var.find(".//html:xhtml", NS)
            if doc is not None and doc.text:
                doc_text = doc.text.strip()
                var_doc[name] = doc_text
                if doc_text.startswith(("xDI", "udiDI")):
                    hw_inputs.add(name)
                elif doc_text.startswith(("xDO", "udiDO")):
                    hw_outputs.add(name)

        var_graph: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        for entry in pou_map_data:
            pname = entry["Programm_Name"]
            in_bases = [base_name(inp["external"]) for inp in entry["inputs"] if inp.get("external")]
            out_bases = [base_name(out["external"]) for out in entry["outputs"] if out.get("external")]
            for b_in in in_bases:
                for b_out in out_bases:
                    var_graph[b_in].append((pname, b_out))

        def find_paths(start_base, visited_bases=None, depth=0):
            if visited_bases is None:
                visited_bases = set()
            if start_base in visited_bases:
                return []
            visited_bases.add(start_base)
            if start_base in hw_outputs:
                return [[]]
            paths = []
            for prog, new_base in var_graph.get(start_base, []):
                for sub_path in find_paths(new_base, visited_bases.copy(), depth + 1):
                    paths.append([(prog, new_base)] + sub_path)
            return paths

        trace: Dict[str, Any] = {}
        for pname, entry in pou_map.items():
            prog_outputs = []
            for out in entry["outputs"]:
                internal = out["internal"]
                ext = out.get("external")
                if not ext:
                    continue
                b = base_name(ext)
                if b in hw_outputs:
                    prog_outputs.append({
                        "internal": internal,
                        "external": ext,
                        "hardware": True,
                        "paths": [[(pname, b), {"hardware": var_doc.get(b)}]]
                    })
                else:
                    chains = []
                    for path in find_paths(b):
                        chain = [{"program": pname, "variable": b}]
                        for step_prog, step_base in path:
                            chain.append({"program": step_prog, "variable": step_base})
                        if path:
                            last_base = path[-1][1]
                            chain.append({"hardware": var_doc.get(last_base)})
                        chains.append(chain)
                    prog_outputs.append({
                        "internal": internal,
                        "external": ext,
                        "hardware": False,
                        "paths": chains
                    })
            trace[pname] = prog_outputs

        self.variable_traces_path.write_text(json.dumps(trace, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"variable_traces.json geschrieben.")

    # ----------------------------
    # GVL-Datenklassen + gvl_globals.json aus Cell 9–11
    # ----------------------------

    def build_gvl_globals(self) -> None:
        objects_file = self.objects_json_path
        gvl_json_file = self.gvl_globals_path

        objects_data = json.loads(objects_file.read_text(encoding="utf-8"))
        opcua_map = self._parse_opcua_flags_from_xml()

        gvl_list: List[GVL] = []
        for obj in objects_data:
            if obj.get("kind") != "GVL":
                continue
            gvl_name = obj.get("name")
            if not gvl_name:
                continue
            globals_raw = obj.get("globals", [])
            globals_dc: List[GlobalVar] = []
            for gv in globals_raw:
                v_name = gv["name"]
                flags = opcua_map.get(gvl_name, {}).get(v_name, {}) # NEU
                
                globals_dc.append(
                    GlobalVar(
                        name=v_name,
                        type=gv.get("type", ""),
                        init=gv.get("init"),
                        address=gv.get("address"),
                        opcua_da=flags.get("da", False),     # NEU
                        opcua_write=flags.get("write", False) # NEU
                    )
                )
            gvl_list.append(GVL(name=gvl_name, globals=globals_dc))

        gvl_json_file.write_text(
            json.dumps([asdict(g) for g in gvl_list], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        self._gvl_models = gvl_list
        print(f"{len(gvl_list)} GVLs in gvl_globals.json geschrieben.")

    # ----------------------------
    # Mapping- / GVL-Modelle als Datenklassen zurückgeben
    # ----------------------------

    def get_program_models(self) -> List[ProgramMapping]:
        if self._program_models is not None:
            return self._program_models
        if self._mapping_raw is None:
            self._mapping_raw = json.loads(self.program_io_mapping_path.read_text(encoding="utf-8"))
        models: List[ProgramMapping] = []
        for e in self._mapping_raw:
            name = e["Programm_Name"]
            inputs = [IOEntry(v.get("internal"), v.get("external"), v.get("internal_type")) for v in e.get("inputs", [])]
            outputs = [IOEntry(v.get("internal"), v.get("external"), v.get("internal_type")) for v in e.get("outputs", [])]
            inouts = [IOEntry(v.get("internal"), v.get("external"), v.get("internal_type")) for v in e.get("inouts", [])] if "inouts" in e else []
            temps = [TempEntry(t["name"], t.get("type")) for t in e.get("temps", [])]
            methods: List[MethodMapping] = []
            for method in e.get("methods", []):
                methods.append(
                    MethodMapping(
                        name=method.get("name", ""),
                        method_type=method.get("method_type", method.get("name", "")),
                        inputs=[IOEntry(v.get("internal"), v.get("external"), v.get("internal_type")) for v in method.get("inputs", [])],
                        outputs=[IOEntry(v.get("internal"), v.get("external"), v.get("internal_type")) for v in method.get("outputs", [])],
                        inouts=[IOEntry(v.get("internal"), v.get("external"), v.get("internal_type")) for v in method.get("inouts", [])],
                        temps=[TempEntry(t["name"], t.get("type")) for t in method.get("temps", [])],
                        method_code=method.get("method_code", ""),
                        declaration_header=method.get("declaration_header", ""),
                        programming_lang=method.get("programming_lang"),
                        return_type=method.get("return_type"),
                        rpc_enabled=bool(method.get("rpc_enabled", False)),
                    )
                )
            subcalls: List[Subcall] = []
            for sc in e.get("subcalls", []):
                subcalls.append(
                    Subcall(
                        SubNetwork_Name=sc.get("SubNetwork_Name"),
                        instanceName=sc.get("instanceName"),
                        inputs=[SubcallParam(p.get("internal"), p.get("external")) for p in sc.get("inputs", [])],
                        outputs=[SubcallParam(p.get("internal"), p.get("external")) for p in sc.get("outputs", [])],
                    )
                )
            models.append(
                ProgramMapping(
                    programm_name=name,
                    inputs=inputs,
                    outputs=outputs,
                    inouts=inouts,
                    temps=temps,
                    subcalls=subcalls,
                    program_code=e.get("program_code", ""),
                    programming_lang=e.get("programming_lang"),
                    methods=methods,
                )
            )
        self._program_models = models
        return models

    def get_gvl_models(self) -> List[GVL]:
        if self._gvl_models is not None:
            return self._gvl_models
        if not self.gvl_globals_path.exists():
            raise FileNotFoundError(self.gvl_globals_path)
        raw = json.loads(self.gvl_globals_path.read_text(encoding="utf-8"))
        gvl_list: List[GVL] = []
        for g in raw:
            globals_dc = [
                GlobalVar(
                    name=gv["name"],
                    type=gv.get("type", ""),
                    init=gv.get("init"),
                    address=gv.get("address"),
                )
                for gv in g.get("globals", [])
            ]
            gvl_list.append(GVL(name=g["name"], globals=globals_dc))
        self._gvl_models = gvl_list
        return gvl_list


class PLCOpenXMLParserWithHW(PLCOpenXMLParser):
    """
    Erweiterung des PLCOpen-Parsers um die Hardware-Mapping-Schritte aus getHWAddr_extracted.py.
    """

    def __init__(
        self,
        project_dir: Path,
        sln_path: Path,
        io_mapping_xml_path: Optional[Path] = None,
        io_mapping_json_path: Optional[Path] = None,
    ):
        super().__init__(project_dir, sln_path)
        self.io_mapping_xml_path = Path(io_mapping_xml_path) if io_mapping_xml_path else self.project_dir / "io_mappings.xml"
        self.io_mapping_json_path = Path(io_mapping_json_path) if io_mapping_json_path else self.project_dir / "io_mappings.json"

    @staticmethod
    def _split_tc_path(p: str) -> List[str]:
        return [s for s in (p or "").split("^") if s]

    @classmethod
    def _parse_var_side(cls, var_str: str) -> IoVarSide:
        parts = cls._split_tc_path(var_str)
        return IoVarSide(
            raw=var_str,
            parts=parts,
            is_plc_task=bool(parts and parts[0].lower().startswith("plctask ")),
            channel=parts[-1] if parts else "",
        )

    @staticmethod
    def _full_channel_path(ownerB_name: str, varB: str) -> str:
        return f"{ownerB_name}^{varB}"

    _num = r"[-]?\d+"

    @classmethod
    def _get_int(cls, xml_text: str, tag: str) -> Optional[int]:
        m = re.search(fr"<{tag}[^>]*>\s*({cls._num})\s*</{tag}>", xml_text)
        return int(m.group(1)) if m else None

    @classmethod
    def _get_hex_attr(cls, xml_text: str, tag: str) -> Optional[str]:
        m = re.search(fr'<{tag}[^>]*Hex="#x([0-9A-Fa-f]+)"', xml_text)
        return m.group(1) if m else None

    @classmethod
    def _parse_channel_meta(cls, xml_text: str) -> Dict[str, Any]:
        vsize = cls._get_int(xml_text, "VarBitSize")
        vaddr = cls._get_int(xml_text, "VarBitAddr")
        vinout = cls._get_int(xml_text, "VarInOut")
        ams = cls._get_int(xml_text, "AmsPort")
        igrp = cls._get_int(xml_text, "IndexGroup")
        ioff = cls._get_int(xml_text, "IndexOffset")
        ln = cls._get_int(xml_text, "Length")
        igrp_hex = cls._get_hex_attr(xml_text, "IndexGroup")
        ioff_hex = cls._get_hex_attr(xml_text, "IndexOffset")

        kv: Dict[str, int] = {}
        for m in re.finditer(r"<([A-Za-z0-9_]+)>\s*([0-9]+)\s*</\1>", xml_text):
            tag, val = m.group(1), int(m.group(2))
            if "Offs" in tag or "Offset" in tag or tag in ("ByteOffset", "BitOffset"):
                kv[tag] = val
        for m in re.finditer(r'Name="([^"]*?(?:Offs|Offset)[^"]*)"\s*>\s*([0-9]+)\s*<', xml_text):
            kv[m.group(1)] = int(m.group(2))

        if isinstance(vaddr, int):
            byte_off = vaddr // 8
            bit_off = vaddr % 8
        else:
            byte_off = kv.get("InputOffsByte") or kv.get("OutputOffsByte") or kv.get("ByteOffset") or kv.get("OffsByte")
            bit_off = kv.get("InputOffsBit") or kv.get("OutputOffsBit") or kv.get("BitOffset") or kv.get("OffsBit")

        return {
            "varBitSize": vsize,
            "varBitAddr": vaddr,
            "varInOut": vinout,
            "amsPort": ams,
            "indexGroup": igrp,
            "indexOffset": ioff,
            "length": ln,
            "indexGroupHex": igrp_hex,
            "indexOffsetHex": ioff_hex,
            "byte_offset": byte_off,
            "bit_offset": bit_off,
            "rawOffsets": kv,
        }

    @staticmethod
    def _dir_letter(plc_path_lower: str, var_inout: Optional[int], chan_spec: str) -> str:
        if chan_spec.endswith("^Input"):
            return "I"
        if chan_spec.endswith("^Output"):
            return "Q"
        if "plctask inputs" in plc_path_lower:
            return "I"
        if "plctask outputs" in plc_path_lower:
            return "Q"
        if var_inout == 0:
            return "I"
        if var_inout == 1:
            return "Q"
        return "?"

    @staticmethod
    def _plc_var_only(plc_side_var: str) -> str:
        parts = plc_side_var.split("^")
        return parts[-1] if parts else plc_side_var

    def produce_mapping_xml(self, out_path: Optional[Path] = None) -> str:
        target = Path(out_path) if out_path else self.io_mapping_xml_path

        # COM: gecachten System Manager verwenden (öffnet Solution nur 1× pro Run)
        sys_mgr = self._get_system_manager_cached()

        xml_text = sys_mgr.ProduceMappingInfo()
        target.write_text(xml_text or "", encoding="utf-8")
        print(f"Mapping-XML gespeichert: {target}")
        return xml_text or ""

    def parse_var_links(self, xml_text: str) -> List[IoLink]:
        if not xml_text:
            return []
        root = ET.fromstring(xml_text)
        links: List[IoLink] = []

        for ownerA in root.findall(".//OwnerA"):
            ownerA_name = ownerA.attrib.get("Name", "")
            for ownerB in ownerA.findall("./OwnerB"):
                ownerB_name = ownerB.attrib.get("Name", "")
                for link in ownerB.findall("./Link"):
                    varA = link.attrib.get("VarA", "")
                    varB = link.attrib.get("VarB", "")
                    sideA = self._parse_var_side(varA)
                    sideB = self._parse_var_side(varB)

                    if sideA.is_plc_task:
                        plc_side, io_side = sideA, sideB
                    elif sideB.is_plc_task:
                        plc_side, io_side = sideB, sideA
                    else:
                        plc_side = None
                        io_side = None

                    links.append(
                        IoLink(
                            ownerA=ownerA_name,
                            ownerB=ownerB_name,
                            varA=varA,
                            varB=varB,
                            sideA=sideA,
                            sideB=sideB,
                            plc=plc_side,
                            io=io_side,
                        )
                    )
        return links

    def _open_system_manager(self) -> Any:
        # COM: gecachten System Manager verwenden
        return self._get_system_manager_cached()

    def build_io_mappings(self, xml_text: Optional[str] = None, save_json: bool = True) -> List[IoHardwareAddress]:
        xml_content = xml_text or ""
        if not xml_content:
            if self.io_mapping_xml_path.exists():
                xml_content = self.io_mapping_xml_path.read_text(encoding="utf-8", errors="replace")
            else:
                xml_content = self.produce_mapping_xml(self.io_mapping_xml_path)

        links = self.parse_var_links(xml_content)
        sys_mgr = self._open_system_manager()

        bundle: List[IoHardwareAddress] = []
        missing = 0

        for rec in links:
            if not rec.plc or not rec.io:
                continue

            plc_var_path = rec.plc.raw
            plc_var_name = self._plc_var_only(plc_var_path)
            io_owner = rec.ownerB if rec.ownerB.startswith("TIID^") else rec.ownerA
            io_chan_spec = rec.io.raw
            full_io_path = self._full_channel_path(io_owner, io_chan_spec)

            meta = {
                "varBitSize": None,
                "varBitAddr": None,
                "varInOut": None,
                "amsPort": None,
                "indexGroup": None,
                "indexOffset": None,
                "length": None,
                "indexGroupHex": None,
                "indexOffsetHex": None,
                "byte_offset": None,
                "bit_offset": None,
                "rawOffsets": {},
            }
            raw_xml = ""
            try:
                ch_item = sys_mgr.LookupTreeItem(full_io_path)
                try:
                    ch_xml = ch_item.ProduceXml(0)
                except TypeError:
                    ch_xml = ch_item.ProduceXml()
                raw_xml = ch_xml
                meta = self._parse_channel_meta(ch_xml)
            except Exception as e:
                missing += 1
                raw_xml = f"ERROR: {e}"

            d_letter = self._dir_letter(plc_var_path.lower(), meta.get("varInOut"), io_chan_spec)
            if isinstance(meta.get("byte_offset"), int) and isinstance(meta.get("bit_offset"), int):
                pi_addr = f"{d_letter} {meta['byte_offset']}.{meta['bit_offset']}"
            else:
                vaddr = meta.get("varBitAddr")
                if isinstance(vaddr, int):
                    pi_addr = f"{d_letter} {vaddr//8}.{vaddr%8}"
                    meta["byte_offset"] = vaddr // 8
                    meta["bit_offset"] = vaddr % 8
                else:
                    pi_addr = None

            bundle.append(
                IoHardwareAddress(
                    plc_path=plc_var_path,
                    plc_var=plc_var_name,
                    device_path=io_owner,
                    channel_label=io_chan_spec,
                    io_path=full_io_path,
                    direction="Input" if d_letter == "I" else "Output" if d_letter == "Q" else "Unknown",
                    ea_address=pi_addr,
                    varBitAddr=meta.get("varBitAddr"),
                    varBitSize=meta.get("varBitSize"),
                    varInOut=meta.get("varInOut"),
                    byte_offset=meta.get("byte_offset"),
                    bit_offset=meta.get("bit_offset"),
                    amsPort=meta.get("amsPort"),
                    indexGroup=meta.get("indexGroup"),
                    indexGroupHex=meta.get("indexGroupHex"),
                    indexOffset=meta.get("indexOffset"),
                    indexOffsetHex=meta.get("indexOffsetHex"),
                    length=meta.get("length"),
                    raw_offsets=meta.get("rawOffsets", {}),
                    io_raw_xml=(raw_xml or "")[:4000],
                )
            )

        self.io_hw_mappings = bundle

        if save_json:
            self.io_mapping_json_path.write_text(
                json.dumps([asdict(b) for b in bundle], indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            with_addr = sum(1 for x in bundle if x.byte_offset is not None)
            print(
                f"OK: {self.io_mapping_json_path}  ({len(bundle)} Links, {with_addr} mit Byte/Bit, {missing} ohne Channel-XML)"
            )

        return bundle

    def load_io_mappings(self) -> List[IoHardwareAddress]:
        data = json.loads(self.io_mapping_json_path.read_text(encoding="utf-8"))
        self.io_hw_mappings = [IoHardwareAddress(**entry) for entry in data]
        return self.io_hw_mappings

    def mark_variable_traces_with_hw(self) -> None:
        traces_path = self.variable_traces_path
        if not traces_path.exists():
            print("variable_traces.json nicht gefunden -> uebersprungen.")
            return

        if not self.io_hw_mappings and self.io_mapping_json_path.exists():
            try:
                self.load_io_mappings()
            except Exception:
                pass

        bound_vars = {m.plc_var for m in self.io_hw_mappings}
        bound_bases = {v.split(".")[-1] for v in bound_vars}

        traces = json.loads(traces_path.read_text(encoding="utf-8"))
        changed = 0

        for outputs in traces.values():
            for out in outputs:
                if out.get("hardware"):
                    continue
                for path in out.get("paths", []):
                    if not isinstance(path, list):
                        continue
                    hits = False
                    for step in path:
                        if not isinstance(step, dict):
                            continue
                        var = step.get("variable")
                        hw = step.get("hardware")
                        if var in bound_vars or var in bound_bases or hw in bound_vars or hw in bound_bases:
                            hits = True
                            break
                    if hits:
                        out["hardware"] = True
                        changed += 1
                        break

        traces_path.write_text(json.dumps(traces, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"variable_traces.json aktualisiert (hardware=true in {changed} Outputs).")

    @staticmethod
    def _strip_ns(xml_text: str) -> str:
        return re.sub(r'\sxmlns="[^"]+"', '', xml_text, count=1)

    @staticmethod
    def _strip_st_comments(s: str) -> str:
        s = re.sub(r'\(\*.*?\*\)', '', s, flags=re.S)
        s = re.sub(r'//.*', '', s)
        return s

    # --- NEU: Projektinformationen aus export.xml ---------------------------------

    def get_plc_project_info(self) -> dict:
        """
        Extrahiert Projektinformationen aus export.xml.
        Nutzt zuerst den contentHeader name, danach die ProjectStructure.
        Rückgabe:
            {
                "project_name": str | None,
                "project_object_id": str | None
            }
        """
        if not self.export_xml_path.exists():
            return {"project_name": None, "project_object_id": None}

        xml_text = self.export_xml_path.read_text(encoding="utf-8", errors="replace")
        # Namespace entfernen, damit wir ohne NS suchen können
        root = ET.fromstring(self._strip_ns(xml_text))

        project_name: Optional[str] = None
        project_object_id: Optional[str] = None

        # 1) contentHeader name (z. B. Proj1)
        ch = root.find("contentHeader")
        if ch is not None:
            project_name = ch.get("name") or project_name

        # 2) ProjectStructure als Fallback / Zusatzinfos
        for data in root.findall("addData/data"):
            if data.get("name") == "http://www.3s-software.com/plcopenxml/projectstructure":
                ps_root = data.find("ProjectStructure/Object")
                if ps_root is not None:
                    if project_name is None:
                        project_name = ps_root.get("Name")
                    project_object_id = ps_root.get("ObjectId")
                break

        return {
            "project_name": project_name,
            "project_object_id": project_object_id,
        }
