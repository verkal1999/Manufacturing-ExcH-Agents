// Repräsentiert einen typisierten Snapshot des OPC-UA-Adressraums der SPS.
// - NodeKey:      Schlüssel (Namespace, Typ, String-ID) für Variablenknoten.
// - InventorySnapshot.rows  : reine Strukturinformationen (NodeClass, NodeId, Datentyp).
// - InventorySnapshot.bools/strings/int16s/floats:
//                   aktuell gelesene Werte der Variablen an einer Stelle in der Zeit.
// - D2Snapshot:   Event-Payload (correlationId + Snapshot), wie von D2/D3 erzeugt
//                 und im ReactionManager/FailureRecorder verwendet.
#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include "PLCMonitor.h"  // für PLCMonitor::InventoryRow
// Schlüssel zur Identifikation eines Knotens (für die Werte-Maps im Snapshot).
struct NodeKey {
    uint16_t    ns   = 4;
    char        type = 's';     // 's'|'i'|'g'|'b'
    std::string id;             // e.g. "OPCUA.bool1"
    bool operator==(const NodeKey& o) const {
        return ns == o.ns && type == o.type && id == o.id;
    }
};
// Hash-Funktor für NodeKey, damit er als Schlüssel in unordered_map verwendbar ist.
struct NodeKeyHash {
    size_t operator()(const NodeKey& k) const noexcept {
        return std::hash<uint16_t>{}(k.ns)
             ^ (std::hash<char>{}(k.type) << 1)
             ^ (std::hash<std::string>{}(k.id) << 2);
    }
};

// Ex RM: typisierter Snapshot
// rows   : reine Struktur (Namensraum, NodeClass, Typ, …)
// bools, strings, int16s, floats : aktuelle Werte zu den NodeKeys.
struct InventorySnapshot {
    std::vector<PLCMonitor::InventoryRow> rows;
    std::unordered_map<NodeKey, bool,        NodeKeyHash> bools;
    std::unordered_map<NodeKey, std::string, NodeKeyHash> strings;
    std::unordered_map<NodeKey, int16_t,     NodeKeyHash> int16s;
    std::unordered_map<NodeKey, uint16_t,    NodeKeyHash> uint16s;
    std::unordered_map<NodeKey, int32_t,     NodeKeyHash> int32s;
    std::unordered_map<NodeKey, uint32_t,    NodeKeyHash> uint32s;
    std::unordered_map<NodeKey, int64_t,     NodeKeyHash> int64s;
    std::unordered_map<NodeKey, uint64_t,    NodeKeyHash> uint64s;
    std::unordered_map<NodeKey, double,      NodeKeyHash> floats;
};

// Payload-Typ für evD2 (Correlation + Snapshot)
// Wird von main/PLCMonitor bei TriggerD2/D3 erzeugt und über den EventBus verschickt.
struct D2Snapshot {
    std::string        correlationId;
    InventorySnapshot  inv;
};
