// InventorySnapshotUtils.h – Hilfsfunktionen rund um InventorySnapshot
//
// buildInventorySnapshotNow(...):
//   - browsed vom gegebenen root-Knoten aus den SPS-Adressraum,
//   - füllt InventorySnapshot.rows (Struktur),
//   - liest anschließend konkrete Werte (bool/string/int16/double/float)
//     und trägt sie in die typisierten Maps ein.
//   - wird u. a. in main.cpp bei TriggerD1/D2/D3 verwendet.
//
// dumpInventorySnapshot(...):
//   - einfache Textausgabe des Snapshots (Debugging, Logging), inkl. aller
//     rows und Werte-Maps, wie im MPA-Draft zur Nachvollziehbarkeit gefordert.

#pragma once
#include "InventorySnapshot.h"
#include "PLCMonitor.h"
#include <string>
#include <iostream>
#include <cstdint>
#include <sstream>

// identisch zur RM-Logik, nur als freie Funktion
bool buildInventorySnapshotNow(PLCMonitor& mon,
                               const std::string& root,
                               InventorySnapshot& out); 
                               // Formatiert einen NodeKey in eine gut lesbare Form (z. B. für Logs).
inline std::string nodeKeyToStr(const NodeKey& k) {
    std::ostringstream os;
    os << "ns=" << k.ns << ";type=" << k.type << ";id=\"" << k.id << "\"";
    return os.str();
}
// Gibt einen vollständigen Snapshot auf dem übergebenen ostream aus.
// Praktisches Hilfsmittel, um z. B. D2/D3-Snapshots im Log nachzuvollziehen.
inline void dumpInventorySnapshot(const InventorySnapshot& inv, std::ostream& os = std::cout) {
    os << "\n=== InventorySnapshot ===\n"
       << "rows="    << inv.rows.size()
       << " bools="  << inv.bools.size()
       << " strings="<< inv.strings.size()
       << " int16s=" << inv.int16s.size()
       << " uint16s=" << inv.uint16s.size()
       << " int32s=" << inv.int32s.size()
       << " uint32s=" << inv.uint32s.size()
       << " int64s=" << inv.int64s.size()
       << " uint64s=" << inv.uint64s.size()
       << " floats=" << inv.floats.size() << "\n";

    os << "-- rows (NodeClass | NodeId | DType/Signature)\n";
    // Schleife: alle Strukturzeilen (Browse-Ergebnisse) protokollieren.
    for (const auto& r : inv.rows)
        os << "  " << r.nodeClass << " | " << r.nodeId << " | " << r.dtypeOrSig << "\n";

    os << "-- bools\n";
    // Schleife: alle bool-Werte im Snapshot ausgeben.
    for (const auto& [k,v] : inv.bools)   os << "  " << nodeKeyToStr(k) << " = " << (v ? "true":"false") << "\n";
    os << "-- strings\n";
    // Schleife: alle string-Werte im Snapshot ausgeben.
    for (const auto& [k,v] : inv.strings) os << "  " << nodeKeyToStr(k) << " = \"" << v << "\"\n";
    os << "-- int16s\n";
    // Schleife: alle int16-Werte im Snapshot ausgeben.
    for (const auto& [k,v] : inv.int16s)  os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "-- uint16s\n";
    for (const auto& [k,v] : inv.uint16s) os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "-- int32s\n";
    for (const auto& [k,v] : inv.int32s)  os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "-- uint32s\n";
    for (const auto& [k,v] : inv.uint32s) os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "-- int64s\n";
    for (const auto& [k,v] : inv.int64s)  os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "-- uint64s\n";
    for (const auto& [k,v] : inv.uint64s) os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "-- floats\n";
    // Schleife: alle float/double-Werte im Snapshot ausgeben.
    for (const auto& [k,v] : inv.floats)  os << "  " << nodeKeyToStr(k) << " = " << v << "\n";
    os << "=== /InventorySnapshot ===\n";
}
