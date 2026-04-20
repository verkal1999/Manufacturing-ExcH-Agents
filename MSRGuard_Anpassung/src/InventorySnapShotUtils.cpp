// InventorySnapShotUtils.cpp
// Hilfsfunktionen, um aus dem PLCMonitor einen InventorySnapshot zu erzeugen,
// der sowohl Struktur (rows) als auch aktuelle Werte für relevante Variablen enthält.

#include "InventorySnapShotUtils.h"
#include <iostream>

bool parseNsAndId(const std::string &nodeId, uint16_t &ns, std::string &id, char &typeChar);

// Diese Funktion baut den Snapshot sofort, indem sie alle Variablen unterhalb von root
// browsed und dann ihre Werte mit den passenden Read-Funktionen einliest.
bool buildInventorySnapshotNow(PLCMonitor &mon, const std::string &root, InventorySnapshot &s) {
    s = InventorySnapshot{};
    mon.dumpPlcInventory(s.rows, root.c_str());

    // Schleife: iteriert über alle Elemente in s.rows.
    for (const auto &r : s.rows) {
        if (r.nodeClass != "Variable") continue;

        uint16_t ns = 0;
        std::string id;
        char type = 0;
        if (!parseNsAndId(r.nodeId, ns, id, type))
            continue;
        if (type != 's')
            continue;

        const bool isBool   = (r.dtypeOrSig.find("Boolean") != std::string::npos);
        const bool isString = (r.dtypeOrSig.find("String")  != std::string::npos) ||
                              (r.dtypeOrSig.find("STRING")  != std::string::npos);
        const bool isU16    = (r.dtypeOrSig.find("UInt16")  != std::string::npos);
        const bool isI16    = !isU16 && (r.dtypeOrSig.find("Int16") != std::string::npos);
        const bool isU32    = (r.dtypeOrSig.find("UInt32")  != std::string::npos);
        const bool isI32    = !isU32 && (r.dtypeOrSig.find("Int32") != std::string::npos);
        const bool isU64    = (r.dtypeOrSig.find("UInt64")  != std::string::npos);
        const bool isI64    = !isU64 && (r.dtypeOrSig.find("Int64") != std::string::npos);
        const bool isF64    = (r.dtypeOrSig.find("Double")  != std::string::npos);
        const bool isF32    = (r.dtypeOrSig.find("Float")   != std::string::npos);

        NodeKey k;
        k.ns   = ns;
        k.type = 's';
        k.id   = id;

        if (isBool) {
            bool v{};
            if (mon.readBoolAt(id, ns, v))
                s.bools.emplace(k, v);
        }
        if (isString) {
            std::string v;
            if (mon.readStringAt(id, ns, v))
                s.strings.emplace(k, v);
        }
        if (isI16) {
            UA_Int16 v{};
            if (mon.readInt16At(id, ns, v))
                s.int16s.emplace(k, static_cast<int16_t>(v));
        }
        if (isU16) {
            UA_UInt16 v{};
            if (mon.readUInt16At(id, ns, v))
                s.uint16s.emplace(k, static_cast<uint16_t>(v));
        }
        if (isI32) {
            UA_Int32 v{};
            if (mon.readInt32At(id, ns, v))
                s.int32s.emplace(k, static_cast<int32_t>(v));
        }
        if (isU32) {
            UA_UInt32 v{};
            if (mon.readUInt32At(id, ns, v))
                s.uint32s.emplace(k, static_cast<uint32_t>(v));
        }
        if (isI64) {
            UA_Int64 v{};
            if (mon.readInt64At(id, ns, v))
                s.int64s.emplace(k, static_cast<int64_t>(v));
        }
        if (isU64) {
            UA_UInt64 v{};
            if (mon.readUInt64At(id, ns, v))
                s.uint64s.emplace(k, static_cast<uint64_t>(v));
        }
        if (isF64) {
            UA_Double v{};
            if (mon.readDoubleAt(id, ns, v))
                s.floats.emplace(k, static_cast<double>(v));
        }
        if (isF32) {
            UA_Float v{};
            if (mon.readFloatAt(id, ns, v))
                s.floats.emplace(k, static_cast<double>(v));
        }
    }

    return true;
}
