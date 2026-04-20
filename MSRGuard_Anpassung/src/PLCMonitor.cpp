// PLCMonitor.cpp
// Abstraktionsschicht zum OPC-UA-Client (open62541) für die Kommunikation mit der SPS.
// Kümmert sich um Verbindungen, Reconnect, Lesen/Schreiben, Subscriptions und Hilfsfunktionen,
// die du im MPA-Draft als Schnittstelle zwischen Framework und PLC spezifiziert hast.
#include "PLCMonitor.h"

#include <chrono>
#include <thread>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <queue>
#include <sstream>
#include <unordered_set>
#include <future>

// ==== Helpers (datei-lokal) =================================================
namespace {

bool loadFile(const std::string& path, UA_ByteString& out) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if(!f) return false;
    const std::streamsize len = f.tellg();
    if(len <= 0) return false;
    f.seekg(0, std::ios::beg);

    out.length = static_cast<size_t>(len);
    out.data   = (UA_Byte*)UA_malloc(out.length);
    if(!out.data) { out.length = 0; return false; }

    if(!f.read(reinterpret_cast<char*>(out.data), len)) {
        UA_ByteString_clear(&out);
        return false;
    }
    return true;
}
static void clearRefList(std::vector<UA_ReferenceDescription> &v) {
    for (auto &rd : v) UA_ReferenceDescription_clear(&rd);
    v.clear();
}

// ---- helpers: UA_String/NodeId → std::string
inline std::string uaToStdString(const UA_String &s) {
    if (!s.data || s.length == 0) return {};
    return std::string(reinterpret_cast<const char*>(s.data), s.length);
}

inline std::string nodeIdToString(const UA_NodeId &id) {
    UA_String s = UA_STRING_NULL;
    UA_NodeId_print(&id, &s);
    std::string out = uaToStdString(s);
    UA_String_clear(&s);
    return out;
}

inline std::string toStdString(const UA_String &s) {
    return std::string((const char*)s.data, s.length);
}

// Variant → string (einige gängige Typen)
std::string variantToString(const UA_Variant *v, std::string &outTypeName) {
    if (!v || !v->type) { outTypeName = "null"; return "<null>"; }

    if (UA_Variant_isScalar(v)) {
        if (v->type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
            outTypeName = "Boolean";
            return (*(UA_Boolean*)v->data) ? "true" : "false";
        } else if (v->type == &UA_TYPES[UA_TYPES_UINT16]) {
            outTypeName = "UInt16";
            return std::to_string(*(UA_UInt16*)v->data);
        } else if (v->type == &UA_TYPES[UA_TYPES_INT16]) {
            outTypeName = "Int16";
            return std::to_string(*(UA_Int16*)v->data);
        } else if (v->type == &UA_TYPES[UA_TYPES_INT32]) {
            outTypeName = "Int32";
            return std::to_string(*(UA_Int32*)v->data);
        } else if (v->type == &UA_TYPES[UA_TYPES_UINT32]) {
            outTypeName = "UInt32";
            return std::to_string(*(UA_UInt32*)v->data);
        } else if (v->type == &UA_TYPES[UA_TYPES_INT64]) {
            outTypeName = "Int64";
            return std::to_string(*(UA_Int64*)v->data);
        } else if (v->type == &UA_TYPES[UA_TYPES_UINT64]) {
            outTypeName = "UInt64";
            return std::to_string(*(UA_UInt64*)v->data);
        } else if (v->type == &UA_TYPES[UA_TYPES_DOUBLE]) {
            outTypeName = "Double";
            std::ostringstream os; os << *(UA_Double*)v->data; return os.str();
        } else if (v->type == &UA_TYPES[UA_TYPES_FLOAT]) {
            outTypeName = "Float";
            std::ostringstream os; os << *(UA_Float*)v->data; return os.str();
        } else if (v->type == &UA_TYPES[UA_TYPES_STRING]) {
            outTypeName = "String";
            UA_String *s = (UA_String*)v->data;
            return toStdString(*s);
        } else {
            outTypeName = "Scalar";
            return "<scalar>";
        }
    } else {
        outTypeName = "Array";
        return "<array>";
    }
}
// Builtin-Typname aus DataType-NodeId (nur ns=0 sauber mappbar)
// Builtin-Typname aus DataType-NodeId (nur ns=0 sauber mappbar)
static std::string typeNameFromNodeId(const UA_NodeId &typeId) {
    if (typeId.namespaceIndex == 0) {
        for (size_t i = 0; i < UA_TYPES_COUNT; ++i)
            if (UA_NodeId_equal(&UA_TYPES[i].typeId, &typeId))
                return std::string(UA_TYPES[i].typeName);
    }
    return nodeIdToString(typeId); // Fallback: volle NodeId (benutzerdefiniert)
}

// browsed eine Ebene FORWARD/Hierarchical und liefert Response (direkt nutzen)
static bool browseOne(UA_Client* c, const UA_NodeId &start, UA_BrowseResponse &out) {
    UA_BrowseDescription bd; UA_BrowseDescription_init(&bd);
    bd.nodeId = start;
    bd.resultMask = UA_BROWSERESULTMASK_ALL;
    bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HIERARCHICALREFERENCES);
    bd.includeSubtypes = true;
    bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;

    UA_BrowseRequest req; UA_BrowseRequest_init(&req);
    req.nodesToBrowse = &bd;
    req.nodesToBrowseSize = 1;
    req.requestedMaxReferencesPerNode = 0;

    out = UA_Client_Service_browse(c, req);
    return UA_StatusCode_isGood(out.responseHeader.serviceResult) && out.resultsSize == 1;
}

// sucht direkten Child-Folder mit gegebenem BrowseName (exakter Name)
static bool findChildByBrowseName(UA_Client* c, const UA_NodeId& parent,
                                  const char* name, UA_NodeId &outNode) {
    UA_BrowseResponse br; UA_BrowseResponse_init(&br);
    if(!browseOne(c, parent, br)) return false;
    bool ok = false;
    const auto &res = br.results[0];
    for (size_t i = 0; i < res.referencesSize; ++i) {
        const auto &r = res.references[i];
        if (!r.browseName.name.length) continue;
        std::string bn = uaToStdString(r.browseName.name);
        if (bn == name) {
            ok = (UA_NodeId_copy(&r.nodeId.nodeId, &outNode) == UA_STATUSCODE_GOOD);
            break;
        }
    }
    UA_BrowseResponse_clear(&br);
    return ok;
}

// Input/OutputArguments-Property finden
static bool findMethodProperty(UA_Client* c, const UA_NodeId &method,
                               const char* propName, UA_NodeId &outProp) {
    UA_BrowseDescription bd; UA_BrowseDescription_init(&bd);
    bd.nodeId          = method;
    bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY);
    bd.includeSubtypes = true;
    bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;
    bd.resultMask      = UA_BROWSERESULTMASK_ALL;

    UA_BrowseRequest req; UA_BrowseRequest_init(&req);
    req.nodesToBrowse = &bd;
    req.nodesToBrowseSize = 1;

    UA_BrowseResponse resp = UA_Client_Service_browse(c, req);
    bool ok = false;
    if (resp.resultsSize) {
        for (size_t i = 0; i < resp.results[0].referencesSize; ++i) {
            const auto &r = resp.results[0].references[i];
            if (!r.browseName.name.length) continue;
            if (uaToStdString(r.browseName.name) == propName) {
                ok = (UA_NodeId_copy(&r.nodeId.nodeId, &outProp) == UA_STATUSCODE_GOOD);
                break;
            }
        }
    }
    UA_BrowseResponse_clear(&resp);
    return ok;
}

// Browsed **eine** Ebene (FORWARD, HierarchicalReferences) unterhalb von 'start'.
UA_StatusCode browseOneDeep(UA_Client *client,
                            const UA_NodeId &start,
                            std::vector<UA_ReferenceDescription> &out) {
    out.clear();

    UA_BrowseDescription bd; UA_BrowseDescription_init(&bd);
    bd.nodeId          = start;
    bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HIERARCHICALREFERENCES);
    bd.includeSubtypes = true;
    bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;

    UA_BrowseRequest brq; UA_BrowseRequest_init(&brq);
    brq.nodesToBrowse = &bd;
    brq.nodesToBrowseSize = 1;
    brq.requestedMaxReferencesPerNode = 0;

    UA_BrowseResponse brs = UA_Client_Service_browse(client, brq);
    UA_StatusCode rc = brs.responseHeader.serviceResult;

    if (rc == UA_STATUSCODE_GOOD && brs.resultsSize > 0) {
        const auto &res = brs.results[0];
        out.reserve(res.referencesSize);
        for (size_t i = 0; i < res.referencesSize; ++i) {
            UA_ReferenceDescription tmp; UA_ReferenceDescription_init(&tmp);
            if (UA_ReferenceDescription_copy(&res.references[i], &tmp) == UA_STATUSCODE_GOOD)
                out.push_back(tmp);
        }
    }

    UA_BrowseResponse_clear(&brs);
    return rc;
}

static bool resolveBrowsePathToNode(UA_Client* client,
                                    const std::vector<UA_QualifiedName>& qnames,
                                    UA_NodeId& outTarget /* deep copy */) {
    UA_NodeId_clear(&outTarget);
    if (!client || qnames.empty())
        return false;

    UA_BrowsePath bp;
    UA_BrowsePath_init(&bp);
    bp.startingNode = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER); // ns=0;i=85

    // RelativePath mit HierarchicalReferences (inkl. Subtypen)
    std::vector<UA_RelativePathElement> elems(qnames.size());
    for (size_t i = 0; i < qnames.size(); ++i) {
        UA_RelativePathElement_init(&elems[i]);
        elems[i].referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HIERARCHICALREFERENCES);
        elems[i].isInverse       = false;
        elems[i].includeSubtypes = true;
        elems[i].targetName      = qnames[i]; // <-- der richtige UA_QualifiedName
    }
    UA_RelativePath rp;
    UA_RelativePath_init(&rp);
    rp.elements     = elems.data();
    rp.elementsSize = static_cast<UA_UInt32>(elems.size());
    bp.relativePath = rp;

    UA_TranslateBrowsePathsToNodeIdsRequest req;
    UA_TranslateBrowsePathsToNodeIdsRequest_init(&req);
    req.browsePaths     = &bp;
    req.browsePathsSize = 1;

    UA_TranslateBrowsePathsToNodeIdsResponse resp =
        UA_Client_Service_translateBrowsePathsToNodeIds(client, req);

    bool ok = false;
    if (resp.responseHeader.serviceResult == UA_STATUSCODE_GOOD &&
        resp.resultsSize == 1 &&
        UA_StatusCode_isGood(resp.results[0].statusCode) &&
        resp.results[0].targetsSize > 0) {
        ok = (UA_NodeId_copy(&resp.results[0].targets[0].targetId.nodeId, &outTarget)
              == UA_STATUSCODE_GOOD);
    }

    UA_TranslateBrowsePathsToNodeIdsResponse_clear(&resp);
    return ok;
} 

    static std::string ltextToStd(const UA_LocalizedText &lt) {
        return uaToStdString(lt.text);
    } 

static void dumpChildrenOf(UA_Client* c, const UA_NodeId& node, const char* title) {
    // BrowseDescription wie in browseOneDeep – keine Heap-Arrays nötig
    UA_BrowseDescription bd;
    UA_BrowseDescription_init(&bd);
    bd.nodeId = node;
    bd.resultMask = UA_BROWSERESULTMASK_ALL;
    bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HIERARCHICALREFERENCES);
    bd.includeSubtypes = true;
    bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;

    UA_BrowseRequest req; UA_BrowseRequest_init(&req);
    req.nodesToBrowse = &bd;
    req.nodesToBrowseSize = 1;
    req.requestedMaxReferencesPerNode = 0;

    UA_BrowseResponse resp = UA_Client_Service_browse(c, req);

    std::cout << "[BrowseDump] " << title << " children:\n";
    if (resp.resultsSize) {
        const auto &res = resp.results[0];
        for (size_t j = 0; j < res.referencesSize; ++j) {
            const auto &r  = res.references[j];
            const auto &bn = r.browseName;

            // sichere Konvertierung (UA_String -> std::string)
            const std::string browseName = uaToStdString(bn.name);
            const std::string dispName   = uaToStdString(r.displayName.text);

            std::cout << "  - nodeId="     << nodeIdToString(r.nodeId.nodeId)
                      << "  browseName="   << browseName
                      << "  (ns="         << bn.namespaceIndex << ")"
                      << "  displayName="  << dispName
                      << "  class="        << (int)r.nodeClass
                      << "\n";
            if (browseName == "OPCUA") {
                std::cout << "    -> found OPCUA folder\n";
            }
        }
    }
    UA_BrowseResponse_clear(&resp);
}
static std::string friendlyTypeName(UA_Client* c, const UA_NodeId &typeId) {
    // ns=0: direkter Builtin-Name
    if (typeId.namespaceIndex == 0) {
        for (size_t i = 0; i < UA_TYPES_COUNT; ++i)
            if (UA_NodeId_equal(&UA_TYPES[i].typeId, &typeId))
                return std::string(UA_TYPES[i].typeName);
        return "ns=0:" + nodeIdToString(typeId); // Fallback
    }

    // 2) DisplayName des Alias-Typs
    std::string aliasName;
    UA_LocalizedText dlt; UA_LocalizedText_init(&dlt);
    if (UA_Client_readDisplayNameAttribute(c, typeId, &dlt) == UA_STATUSCODE_GOOD)
        aliasName = uaToStdString(dlt.text);
    UA_LocalizedText_clear(&dlt);

    // 3) Inverse HasSubtype (-> Supertyp) einmal oder mehrfach folgen
    UA_NodeId cur; UA_NodeId_init(&cur);
    UA_NodeId_copy(&typeId, &cur);

    std::string baseName;
    for (int steps = 0; steps < 8; ++steps) { // kleine Obergrenze genügt
        UA_BrowseDescription bd; UA_BrowseDescription_init(&bd);
        bd.nodeId          = cur;
        bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HASSUBTYPE);
        bd.browseDirection = UA_BROWSEDIRECTION_INVERSE;   // nach oben
        bd.includeSubtypes = UA_FALSE;
        bd.resultMask      = UA_BROWSERESULTMASK_ALL;

        UA_BrowseRequest req; UA_BrowseRequest_init(&req);
        req.nodesToBrowse = &bd;
        req.nodesToBrowseSize = 1;

        UA_BrowseResponse resp = UA_Client_Service_browse(c, req);
        if (!resp.resultsSize || resp.results[0].referencesSize == 0) {
            UA_BrowseResponse_clear(&resp);
            break;
        }

        // Nimm den ersten Supertyp
        const auto &sup = resp.results[0].references[0].nodeId.nodeId;
        if (sup.namespaceIndex == 0) {
            // Builtin-Name auflösen
            baseName = friendlyTypeName(c, sup);
            UA_BrowseResponse_clear(&resp);
            break;
        } else {
            UA_NodeId tmp; UA_NodeId_init(&tmp);
            UA_NodeId_copy(&sup, &tmp);
            UA_BrowseResponse_clear(&resp);
            UA_NodeId_clear(&cur);
            cur = tmp;
        }
    }
    UA_NodeId_clear(&cur);

    if (!aliasName.empty() && !baseName.empty() && aliasName != baseName)
        return aliasName + " (-> " + baseName + ")";
    if (!aliasName.empty())
        return aliasName;

    // Fallback: rohe NodeId
    return nodeIdToString(typeId);
}
// Signatur-String aus InputArguments/OutputArguments bauen
static std::string methodSignature(UA_Client* c, const UA_NodeId &method) {
    auto readArgList = [&](const char* prop) {
        std::vector<std::string> out;
        UA_NodeId propId; UA_NodeId_init(&propId);
        if (!findMethodProperty(c, method, prop, propId)) return out;

        UA_Variant v; UA_Variant_init(&v);
        if (UA_Client_readValueAttribute(c, propId, &v) == UA_STATUSCODE_GOOD &&
            UA_Variant_hasArrayType(&v, &UA_TYPES[UA_TYPES_ARGUMENT])) {
            auto args = static_cast<UA_Argument*>(v.data);
            for (size_t i = 0; i < v.arrayLength; ++i)
                out.push_back(friendlyTypeName(c, args[i].dataType));
        }
        UA_Variant_clear(&v);
        UA_NodeId_clear(&propId);
        return out;
    };
    auto join = [](const std::vector<std::string>& xs){
        std::string s; for(size_t i=0;i<xs.size();++i){ if(i) s += ", "; s += xs[i]; } return s;
    };
    auto ins  = readArgList("InputArguments");
    auto outs = readArgList("OutputArguments");
    return "in: [" + join(ins) + "], out: [" + join(outs) + "]";
}

static void appendVariableRow(UA_Client* c,
                              const UA_NodeId& target,
                              std::vector<PLCMonitor::InventoryRow>& out) {
    UA_NodeId dt; UA_NodeId_init(&dt);
    std::string dtype = "?";
    if (UA_Client_readDataTypeAttribute(c, target, &dt) == UA_STATUSCODE_GOOD) {
        dtype = friendlyTypeName(c, dt);
    }
    UA_NodeId_clear(&dt);

    out.push_back(PLCMonitor::InventoryRow{
        "Variable",
        nodeIdToString(target),
        dtype
    });
}

static void appendMethodRow(UA_Client* c,
                            const UA_NodeId& target,
                            std::vector<PLCMonitor::InventoryRow>& out) {
    out.push_back(PLCMonitor::InventoryRow{
        "Method",
        nodeIdToString(target),
        methodSignature(c, target)
    });
}

static void appendObjectLikeRow(const char* nodeClass,
                                const UA_NodeId& target,
                                std::vector<PLCMonitor::InventoryRow>& out) {
    out.push_back(PLCMonitor::InventoryRow{
        nodeClass,
        nodeIdToString(target),
        "-"
    });
}

static void collectInventorySubtree(UA_Client* c,
                                    const UA_NodeId& root,
                                    std::vector<PLCMonitor::InventoryRow>& out) {
    std::vector<UA_NodeId> stack;
    std::unordered_set<std::string> seen;

    UA_NodeId start; UA_NodeId_init(&start);
    if (UA_NodeId_copy(&root, &start) != UA_STATUSCODE_GOOD) {
        return;
    }

    seen.insert(nodeIdToString(root));
    stack.push_back(start);

    while (!stack.empty()) {
        UA_NodeId cur = stack.back();
        stack.pop_back();

        UA_BrowseResponse br; UA_BrowseResponse_init(&br);
        if (!browseOne(c, cur, br)) {
            UA_BrowseResponse_clear(&br);
            UA_NodeId_clear(&cur);
            continue;
        }

        for (size_t i = 0; i < br.results[0].referencesSize; ++i) {
            const auto& ref = br.results[0].references[i];
            const UA_NodeId target = ref.nodeId.nodeId;

            const std::string targetId = nodeIdToString(target);
            if (!seen.insert(targetId).second) {
                continue;
            }

            if (ref.nodeClass == UA_NODECLASS_VARIABLE) {
                appendVariableRow(c, target, out);
                continue;
            }

            if (ref.nodeClass == UA_NODECLASS_METHOD) {
                appendMethodRow(c, target, out);
                continue;
            }

            if (ref.nodeClass == UA_NODECLASS_OBJECT || ref.nodeClass == UA_NODECLASS_VIEW) {
                appendObjectLikeRow(ref.nodeClass == UA_NODECLASS_VIEW ? "View" : "Object",
                                    target,
                                    out);

                UA_NodeId next; UA_NodeId_init(&next);
                if (UA_NodeId_copy(&target, &next) == UA_STATUSCODE_GOOD) {
                    stack.push_back(next);
                }
            }
        }

        UA_BrowseResponse_clear(&br);
        UA_NodeId_clear(&cur);
    }
}
} // namespace (helpers)
// === Ende Namespace helpers ================================================

//=== PLCMonitor Inventory Methoden =========================================
bool PLCMonitor::readBoolAt(const std::string& nodeIdStr,
                            UA_UInt16 nsIndex,
                            bool& out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex, const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_BOOLEAN] &&
                    val.data != nullptr;
    if (ok) out = (*static_cast<UA_Boolean*>(val.data)) != 0;
    UA_Variant_clear(&val);
    return ok;
}
bool PLCMonitor::readFloatAt(const std::string& nodeIdStr,
                             UA_UInt16 nsIndex,
                             UA_Float &out) const {
    if (!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_FLOAT] &&
                    val.data != nullptr;
    if (ok) out = *static_cast<UA_Float*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readDoubleAt(const std::string& nodeIdStr,
                              UA_UInt16 nsIndex,
                              UA_Double &out) const {
    if (!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_DOUBLE] &&
                    val.data != nullptr;
    if (ok) out = *static_cast<UA_Double*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readStringAt(const std::string& nodeIdStr,
                              UA_UInt16 nsIndex,
                              std::string& out) const {
    if (!client_)
        return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                         const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_STRING] &&
                    val.data != nullptr;

    if (ok) {
        const UA_String* s = static_cast<const UA_String*>(val.data);
        // Hilfsfunktion oben im File vorhanden:
        out = toStdString(*s);
    }

    UA_Variant_clear(&val);
    return ok;
}

// Optional: generisch als String + Typname (nutzt deine variantToString)
bool PLCMonitor::readAsString(const std::string& nodeIdStr,
                              UA_UInt16 nsIndex,
                              std::string& outValue,
                              std::string& outTypeName) const {
    if (!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);
    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    if (st != UA_STATUSCODE_GOOD) { UA_Variant_clear(&val); return false; }
    outValue = variantToString(&val, outTypeName); // deine Helper-Funktion
    UA_Variant_clear(&val);
    return true;
}

bool PLCMonitor::dumpPlcInventory(std::vector<InventoryRow>& out, const char* plcNameContains) {
    out.clear();
    if (!client_) return false;

    // 1) /Objects durchsehen und PLC-Zweig finden (Namespace aus NodeId verwenden!)
    const UA_NodeId objects = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
    UA_BrowseResponse br; UA_BrowseResponse_init(&br);
    if(!browseOne(client_, objects, br)) { UA_BrowseResponse_clear(&br); return false; }

    UA_NodeId plcNode; UA_NodeId_init(&plcNode);
    UA_UInt16 nsPLC = 0;
    const auto &res = br.results[0];
    for (size_t i = 0; i < res.referencesSize; ++i) {
        const auto &r = res.references[i];
        if (!r.browseName.name.length) continue;
        const std::string bn = uaToStdString(r.browseName.name);
        const std::string dn = uaToStdString(r.displayName.text);

        // ExpandedNodeId zerlegen (kann nsUri/serverIndex tragen)
        const UA_ExpandedNodeId &xn = r.nodeId;
        const UA_UInt16 idNs = xn.nodeId.namespaceIndex;
        const std::string idStr = nodeIdToString(xn.nodeId);
        std::string nsUri;
        if (xn.namespaceUri.length)
            nsUri = uaToStdString(xn.namespaceUri);

        std::cout << "[Inventory][candidate]"
                << " browseName='" << bn << "'"
                << " (bn.ns=" << r.browseName.namespaceIndex << ")"
                << " displayName='" << dn << "'"
                << " nodeClass=" << (int)r.nodeClass
                << " targetId=" << idStr
                << " (id.ns=" << idNs << ")"
                << " serverIndex=" << xn.serverIndex;
        if (!nsUri.empty())
            std::cout << " nsUri=" << nsUri;
        std::cout << "\n";

        // Hinweis, wenn Name-NS und NodeId-NS nicht übereinstimmen (ist häufig ok)
        if (r.browseName.namespaceIndex != idNs) {
            std::cout << "  [note] browseName.ns (" << r.browseName.namespaceIndex
                    << ") != targetId.ns (" << idNs
                    << ") -> für weitere Schritte immer die NodeId nutzen.\n";
        }
        if (bn.find(plcNameContains ? plcNameContains : "PLC") != std::string::npos) {
            if (UA_NodeId_copy(&r.nodeId.nodeId, &plcNode) == UA_STATUSCODE_GOOD) {
                nsPLC = r.nodeId.nodeId.namespaceIndex;
            }
            break;
        }
    }
    UA_BrowseResponse_clear(&br);
    if (nsPLC == 0) {
        std::cout << "[Inventory] Kein PLC-Zweig gefunden.\n";
        return false;
    }

    collectInventorySubtree(client_, plcNode, out);
    UA_NodeId_clear(&plcNode);
    return true;

    // 2) OPCUA- und MAIN-Folder im PLC-Zweig holen
    UA_NodeId opcuaFolder; UA_NodeId_init(&opcuaFolder);
    UA_NodeId mainFolder;  UA_NodeId_init(&mainFolder);
    (void)findChildByBrowseName(client_, plcNode, "OPCUA", opcuaFolder);
    (void)findChildByBrowseName(client_, plcNode, "MAIN",  mainFolder);

    // 3a) Variablen unter OPCUA einsammeln (rekursiv: eine Ebene runter + Unterobjekte)
    if (opcuaFolder.namespaceIndex == nsPLC) {
        std::vector<UA_NodeId> stack;

        // WICHTIG: den Start-Knoten *deep* kopieren, damit opcuaFolder separat freigegeben werden kann
        {
            UA_NodeId root; UA_NodeId_init(&root);
            if (UA_NodeId_copy(&opcuaFolder, &root) == UA_STATUSCODE_GOOD)
                stack.push_back(root);
        }

        while (!stack.empty()) {
            // flache Kopie vom Top-Element ist ok; der *Owner* bleibt das Element im Stack
            UA_NodeId cur = stack.back();
            stack.pop_back();

            UA_BrowseResponse br2; UA_BrowseResponse_init(&br2);
            if (!browseOne(client_, cur, br2)) {
                UA_BrowseResponse_clear(&br2);
                UA_NodeId_clear(&cur);   // cur *besitzt* jetzt die Heap-Daten des ehem. Stack-Elements
                continue;
            }

            for (size_t j = 0; j < br2.results[0].referencesSize; ++j) {
                const auto &r1 = br2.results[0].references[j];

                // Vorsicht: target ist nur innerhalb der Lebenszeit von br2 gültig (by-value/shallow)
                UA_NodeId target = r1.nodeId.nodeId;

                // Unterordner (OBJECT/VIEW) in den Stack legen — IMMER deep kopieren!
                if (r1.nodeClass == UA_NODECLASS_OBJECT || r1.nodeClass == UA_NODECLASS_VIEW) {
                    UA_NodeId next; UA_NodeId_init(&next);
                    if (UA_NodeId_copy(&target, &next) == UA_STATUSCODE_GOOD)
                        stack.push_back(next);
                }

                // Variablen erfassen
                if (r1.nodeClass == UA_NODECLASS_VARIABLE && target.namespaceIndex == nsPLC) {
                    UA_NodeId dt; UA_NodeId_init(&dt);
                    std::string dtype = "?";
                    if (UA_Client_readDataTypeAttribute(client_, target, &dt) == UA_STATUSCODE_GOOD) {
                        dtype = friendlyTypeName(client_, dt);
                    }
                    UA_NodeId_clear(&dt); // IMMER freigeben, unabhängig vom Status

                    out.push_back(InventoryRow{
                        "Variable",
                        nodeIdToString(target),
                        dtype
                    });
                }
            }

            UA_BrowseResponse_clear(&br2);
            UA_NodeId_clear(&cur); // genau 1x freigeben
        }
    }

    // 3b) Methoden unter MAIN: erst Objekte (z. B. MAIN.fbJob), darunter Methoden
    if (mainFolder.namespaceIndex == nsPLC) {
        UA_BrowseResponse br3; UA_BrowseResponse_init(&br3);
        if (browseOne(client_, mainFolder, br3)) {
            for (size_t i = 0; i < br3.results[0].referencesSize; ++i) {
                const auto &rObj = br3.results[0].references[i];
                if (rObj.nodeClass != UA_NODECLASS_OBJECT) continue;

                // Das Objekt selbst optional auch listen
                out.push_back(InventoryRow{
                    "Object",
                    nodeIdToString(rObj.nodeId.nodeId),
                    "-"
                });

                // Methoden darunter
                UA_BrowseResponse br4; UA_BrowseResponse_init(&br4);
                if (browseOne(client_, rObj.nodeId.nodeId, br4)) {
                    for (size_t k = 0; k < br4.results[0].referencesSize; ++k) {
                        const auto &rM = br4.results[0].references[k];
                        if (rM.nodeClass != UA_NODECLASS_METHOD) continue;
                        const UA_NodeId methId = rM.nodeId.nodeId; // by value

                        out.push_back(InventoryRow{
                            "Method",
                            nodeIdToString(methId),
                            methodSignature(client_, methId)
                        });
                    }
                }
                UA_BrowseResponse_clear(&br4);
            }
        }
        UA_BrowseResponse_clear(&br3);
    }

    UA_NodeId_clear(&opcuaFolder);
    UA_NodeId_clear(&mainFolder);
    UA_NodeId_clear(&plcNode);
    return true;
}

void PLCMonitor::printInventoryTable(const std::vector<InventoryRow>& rows) const {
    std::cout << "\nNodeClass | NodeId | Datentyp/Signatur\n";
    std::cout << "--------- | ------ | ------------------\n";
    for (const auto &r : rows)
        std::cout << r.nodeClass << " | " << r.nodeId << " | " << r.dtypeOrSig << "\n";
}


// ==== PLCMonitor – Basics ====================================================
PLCMonitor::PLCMonitor(Options o) : opt_(std::move(o)) {}
PLCMonitor::~PLCMonitor() { disconnect(); }

bool PLCMonitor::loadFileToByteString(const std::string& path, UA_ByteString &out) {
    return loadFile(path, out);
}

bool PLCMonitor::connect() {
    disconnect();
    running_.store(true, std::memory_order_release);

    client_ = UA_Client_new();
    if(!client_) return false;

    UA_ClientConfig* cfg = UA_Client_getConfig(client_);
    UA_ClientConfig_setDefault(cfg);

    cfg->outStandingPublishRequests = 5;
    cfg->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
    cfg->securityPolicyUri = UA_STRING_ALLOC(
        const_cast<char*>("http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256"));

    if(!opt_.applicationUri.empty())
        cfg->clientDescription.applicationUri = UA_STRING_ALLOC(
            const_cast<char*>(opt_.applicationUri.c_str()));

    UA_ByteString cert = UA_BYTESTRING_NULL;
    UA_ByteString key  = UA_BYTESTRING_NULL;
    if(!loadFile(opt_.certDerPath, cert) || !loadFile(opt_.keyDerPath, key)) {
        std::fprintf(stderr, "Failed to load cert/key\n");
        return false;
    }

    UA_StatusCode st = UA_ClientConfig_setDefaultEncryption(
        cfg, cert, key, /*trustList*/nullptr, 0, /*revocation*/nullptr, 0);
    UA_ByteString_clear(&cert);
    UA_ByteString_clear(&key);
    if(st != UA_STATUSCODE_GOOD) {
        std::fprintf(stderr, "Encryption setup failed: 0x%08x\n", st);
        UA_Client_delete(client_); client_ = nullptr;
        return false;
    }

    st = UA_Client_connectUsername(client_,
                                   opt_.endpoint.c_str(),
                                   opt_.username.c_str(),
                                   opt_.password.c_str());
    if(st != UA_STATUSCODE_GOOD) {
        std::fprintf(stderr, "Connect failed: 0x%08x\n", st);
        UA_Client_delete(client_); client_ = nullptr;
        return false;
    }

    if(!waitUntilActivated(3000)) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_CLIENT,
                       "Session not ACTIVATED within timeout");
        disconnect();
        return false;
    }
    return true;
}

void PLCMonitor::disconnect() {
    if(client_) {
        if(subId_) {
            UA_Client_Subscriptions_deleteSingle(client_, subId_);
            subId_ = 0; monIdInt16_ = 0; monIdBool_ = 0;
        }
        UA_Client_disconnect(client_);
        UA_Client_delete(client_);
        client_ = nullptr;
    }
    running_.store(false, std::memory_order_release);
    { std::lock_guard<std::mutex> lk(qmx_); while(!q_.empty()) q_.pop(); }
    { std::lock_guard<std::mutex> lk(tmx_); timers_.clear(); }
}

UA_StatusCode PLCMonitor::runIterate(int timeoutMs) {
    if(!client_) return UA_STATUSCODE_BADSERVERNOTCONNECTED;
    return UA_Client_run_iterate(client_, timeoutMs);
}

bool PLCMonitor::waitUntilActivated(int timeoutMs) {
    if(!client_) return false;

    auto t0 = std::chrono::steady_clock::now();
    for(;;) {
        UA_SecureChannelState scState;
        UA_SessionState      ssState;
        UA_StatusCode status;
        (void)UA_Client_run_iterate(client_, 50);

        UA_Client_getState(client_, &scState, &ssState, &status);
        if(scState == UA_SECURECHANNELSTATE_OPEN &&
           ssState == UA_SESSIONSTATE_ACTIVATED)
            return true;

        if(std::chrono::steady_clock::now() - t0 >
           std::chrono::milliseconds(timeoutMs))
            return false;
    }
}

// ==== Reads & Writes =========================================================
bool PLCMonitor::readInt16At(const std::string& nodeIdStr,
                             UA_UInt16 nsIndex,
                             UA_Int16 &out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_INT16] &&
                    val.data != nullptr;
    if(ok) out = *static_cast<UA_Int16*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readUInt16At(const std::string& nodeIdStr,
                              UA_UInt16 nsIndex,
                              UA_UInt16 &out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_UINT16] &&
                    val.data != nullptr;
    if(ok) out = *static_cast<UA_UInt16*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readInt32At(const std::string& nodeIdStr,
                             UA_UInt16 nsIndex,
                             UA_Int32 &out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_INT32] &&
                    val.data != nullptr;
    if(ok) out = *static_cast<UA_Int32*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readUInt32At(const std::string& nodeIdStr,
                              UA_UInt16 nsIndex,
                              UA_UInt32 &out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_UINT32] &&
                    val.data != nullptr;
    if(ok) out = *static_cast<UA_UInt32*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readInt64At(const std::string& nodeIdStr,
                             UA_UInt16 nsIndex,
                             UA_Int64 &out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_INT64] &&
                    val.data != nullptr;
    if(ok) out = *static_cast<UA_Int64*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::readUInt64At(const std::string& nodeIdStr,
                              UA_UInt16 nsIndex,
                              UA_UInt64 &out) const {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(nsIndex,
                          const_cast<char*>(nodeIdStr.c_str()));
    UA_Variant val; UA_Variant_init(&val);

    UA_StatusCode st = UA_Client_readValueAttribute(client_, nid, &val);
    UA_NodeId_clear(&nid);

    const bool ok = (st == UA_STATUSCODE_GOOD) &&
                    UA_Variant_isScalar(&val) &&
                    val.type == &UA_TYPES[UA_TYPES_UINT64] &&
                    val.data != nullptr;
    if(ok) out = *static_cast<UA_UInt64*>(val.data);
    UA_Variant_clear(&val);
    return ok;
}

bool PLCMonitor::writeBool(const std::string& nodeIdStr, UA_UInt16 ns, bool value) {
    if(!client_) return false;

    UA_NodeId nid = UA_NODEID_STRING_ALLOC(ns, const_cast<char*>(nodeIdStr.c_str()));

    UA_Variant v; UA_Variant_init(&v);
    UA_Boolean b = value;
    UA_Variant_setScalarCopy(&v, &b, &UA_TYPES[UA_TYPES_BOOLEAN]);

    UA_StatusCode rc = UA_Client_writeValueAttribute(client_, nid, &v);
    std::cout << "WriteBool " << nodeIdStr << " = " << (value ? "true" : "false")
              << " -> " << UA_StatusCode_name(rc) << "\n";
    UA_Variant_clear(&v);
    UA_NodeId_clear(&nid);
    return rc == UA_STATUSCODE_GOOD;
}

// ==== Subscriptions ==========================================================
bool PLCMonitor::subscribeInt16(const std::string& nodeIdStr, UA_UInt16 nsIndex,
                                double samplingMs, UA_UInt32 queueSize, Int16ChangeCallback cb) {
    if(!client_) return false;
    onInt16Change_ = std::move(cb);

    if(subId_ == 0) {
        UA_CreateSubscriptionRequest sReq = UA_CreateSubscriptionRequest_default();
        sReq.requestedPublishingInterval = 20.0;
        sReq.requestedMaxKeepAliveCount  = 20;
        sReq.requestedLifetimeCount      = 60;

        UA_CreateSubscriptionResponse sResp =
            UA_Client_Subscriptions_create(client_, sReq, /*subCtx*/this, nullptr, nullptr);
        if(sResp.responseHeader.serviceResult != UA_STATUSCODE_GOOD) return false;
        subId_ = sResp.subscriptionId;
    }

    UA_MonitoredItemCreateRequest monReq =
        UA_MonitoredItemCreateRequest_default(
            UA_NODEID_STRING_ALLOC(nsIndex, const_cast<char*>(nodeIdStr.c_str())));
    monReq.requestedParameters.samplingInterval = samplingMs;
    monReq.requestedParameters.queueSize        = queueSize;
    monReq.requestedParameters.discardOldest    = UA_TRUE;

    UA_MonitoredItemCreateResult monRes =
        UA_Client_MonitoredItems_createDataChange(
            client_, subId_, UA_TIMESTAMPSTORETURN_SOURCE, monReq,
            this, &PLCMonitor::dataChangeHandler, nullptr);

    UA_NodeId_clear(&monReq.itemToMonitor.nodeId);

    if(monRes.statusCode != UA_STATUSCODE_GOOD) return false;
    monIdInt16_ = monRes.monitoredItemId;
    return true;
}

bool PLCMonitor::subscribeBool(const std::string& nodeIdStr, UA_UInt16 nsIndex,
                               double samplingMs, UA_UInt32 queueSize, BoolChangeCallback cb) {
    if(!client_) return false;

    if(subId_ == 0) {
        UA_CreateSubscriptionRequest sReq = UA_CreateSubscriptionRequest_default();
        sReq.requestedPublishingInterval = 20.0;
        sReq.requestedMaxKeepAliveCount  = 20;
        sReq.requestedLifetimeCount      = 60;

        UA_CreateSubscriptionResponse sResp =
            UA_Client_Subscriptions_create(client_, sReq, /*subCtx*/this, nullptr, nullptr);
        if(sResp.responseHeader.serviceResult != UA_STATUSCODE_GOOD)
            return false;
        subId_ = sResp.subscriptionId;
    }

    UA_MonitoredItemCreateRequest monReq =
        UA_MonitoredItemCreateRequest_default(
            UA_NODEID_STRING_ALLOC(nsIndex, const_cast<char*>(nodeIdStr.c_str())));
    monReq.requestedParameters.samplingInterval = samplingMs;
    monReq.requestedParameters.queueSize        = queueSize;
    monReq.requestedParameters.discardOldest    = UA_TRUE;

    UA_MonitoredItemCreateResult monRes =
        UA_Client_MonitoredItems_createDataChange(
            client_, subId_, UA_TIMESTAMPSTORETURN_SOURCE, monReq,
            this, &PLCMonitor::dataChangeHandler, nullptr);

    UA_NodeId_clear(&monReq.itemToMonitor.nodeId);
    if(monRes.statusCode != UA_STATUSCODE_GOOD) return false;

    {
        std::lock_guard<std::mutex> lk(cbmx_);
        boolCbs_[monRes.monitoredItemId] = std::move(cb);
    }
    // monIdBool_ behalten wir nur aus Kompatibilitätsgründen, ist aber nicht mehr nötig:
    monIdBool_ = monRes.monitoredItemId;
    return true;
}

void PLCMonitor::unsubscribe() {
    if(client_ && subId_) {
        UA_Client_Subscriptions_deleteSingle(client_, subId_);
    }
    subId_ = 0;
    monIdInt16_ = 0;
    monIdBool_  = 0;
    onInt16Change_ = nullptr;
    onBoolChange_  = nullptr;
    {
        std::lock_guard<std::mutex> lk(cbmx_);
        boolCbs_.clear();
    }
}

void PLCMonitor::dataChangeHandler(UA_Client*,
                                   UA_UInt32 /*subId*/, void* subCtx,
                                   UA_UInt32 monId, void* monCtx,
                                   UA_DataValue* value) {
    PLCMonitor* self = static_cast<PLCMonitor*>(monCtx ? monCtx : subCtx);
    if(!self || !value || !value->hasValue) return;

    // INT16 bleibt wie gehabt
    if(self->onInt16Change_ &&
       UA_Variant_isScalar(&value->value) &&
       value->value.type == &UA_TYPES[UA_TYPES_INT16] &&
       value->value.data) {
        UA_Int16 v = *static_cast<UA_Int16*>(value->value.data);
        self->onInt16Change_(v, *value);
        return;
    }

    // BOOL: passendes Callback per monId suchen
    if(UA_Variant_isScalar(&value->value) &&
       value->value.type == &UA_TYPES[UA_TYPES_BOOLEAN] &&
       value->value.data) {
        UA_Boolean b = *static_cast<UA_Boolean*>(value->value.data);

        BoolChangeCallback cb;
        {
            std::lock_guard<std::mutex> lk(self->cbmx_);
            auto it = self->boolCbs_.find(monId);
            if (it != self->boolCbs_.end()) cb = it->second;
        }
        if (cb) { cb(b, *value); return; }

        // Fallback: altes Single-Callback (falls woanders noch genutzt)
        if (self->onBoolChange_) { self->onBoolChange_(b, *value); return; }
    }
}


// ==== Task-Queue =============================================================
void PLCMonitor::post(UaFn fn) {
    std::lock_guard<std::mutex> lk(qmx_);
    q_.push(std::move(fn));
}
void PLCMonitor::processPosted(size_t max) {
    processTimers();
    for(size_t i=0; i<max; ++i) {
        UaFn fn;
        { std::lock_guard<std::mutex> lk(qmx_);
          if(q_.empty()) break;
          fn = std::move(q_.front()); q_.pop(); }
        fn(); // läuft im gleichen Thread, in dem du runIterate() aufrufst
    }
    // ggf. neu fällig gewordene Timer nachziehen
    processTimers();
}
void PLCMonitor::postDelayed(int delayMs, UaFn fn) {
    auto due = std::chrono::steady_clock::now() + std::chrono::milliseconds(delayMs);
    std::lock_guard<std::mutex> lk(tmx_);
    timers_.push_back(TimedFn{due, std::move(fn)});
}

void PLCMonitor::processTimers() {
    std::vector<UaFn> dueFns;
    {
        std::lock_guard<std::mutex> lk(tmx_);
        const auto now = std::chrono::steady_clock::now();
        auto it = timers_.begin();
        while (it != timers_.end()) {
            if (it->due <= now) {
                dueFns.push_back(std::move(it->fn));
                it = timers_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto &f : dueFns) f();
}

// ==== Testserver-Komfort =====================================================
PLCMonitor::Options
PLCMonitor::TestServerDefaults(const std::string& clientCertDer,
                               const std::string& clientKeyDer,
                               const std::string& endpoint) {
    Options o;
    o.endpoint       = endpoint;
    o.username       = "user";
    o.password       = "pass";
    o.certDerPath    = clientCertDer;
    o.keyDerPath     = clientKeyDer;
    o.applicationUri = "urn:example:open62541:TestClient";
    o.nsIndex        = 2;
    return o;
}

bool PLCMonitor::connectToSecureTestServer(const std::string& clientCertDer,
                                           const std::string& clientKeyDer,
                                           const std::string& endpoint) {
    opt_ = TestServerDefaults(clientCertDer, clientKeyDer, endpoint);
    return connect();
}

bool PLCMonitor::watchTriggerD2(double samplingMs, UA_UInt32 queueSize) {
    return subscribeBool("TriggerD2", /*ns*/1, samplingMs, queueSize,
        [](bool b, const UA_DataValue& dv){
            std::cout << "[Client] TriggerD2: "
                      << (b ? "TRUE" : "FALSE")
                      << "  (status=0x" << std::hex << dv.status << std::dec << ")\n";
        });
}

//== Job Method Call ========================================================

bool PLCMonitor::callMethodTyped(const std::string& objNodeId,
                                 const std::string& methNodeId,
                                 const UAValueMap& inputs,
                                 UAValueMap& outputs,
                                 unsigned timeoutMs)
{
    std::mutex m; std::condition_variable cv;
    bool done=false, ok=false;
    UAValueMap tmpOut;

    post([&, objNodeId, methNodeId, timeoutMs]{
        UA_NodeId obj  = UA_NODEID_STRING_ALLOC(opt_.nsIndex, const_cast<char*>(objNodeId.c_str()));
        UA_NodeId meth = UA_NODEID_STRING_ALLOC(opt_.nsIndex, const_cast<char*>(methNodeId.c_str()));

        // Inputs: in[] Größe = maxIndex+1
        size_t inSz = inputs.empty() ? 0u : static_cast<size_t>(inputs.rbegin()->first + 1);
        std::vector<UA_Variant> in(inSz);
        for (auto& v : in) UA_Variant_init(&v);

        for (auto& [i, val] : inputs) {
            switch (val.index()) {
              case 1: { // bool
                UA_Boolean b = std::get<bool>(val) ? UA_TRUE : UA_FALSE;
                UA_Variant_setScalarCopy(&in[i], &b, &UA_TYPES[UA_TYPES_BOOLEAN]); break;
              }
              case 2: { // int16
                UA_Int16 x = std::get<int16_t>(val);
                UA_Variant_setScalarCopy(&in[i], &x, &UA_TYPES[UA_TYPES_INT16]); break;
              }
              case 3: { // int32
                UA_Int32 x = std::get<int32_t>(val);
                UA_Variant_setScalarCopy(&in[i], &x, &UA_TYPES[UA_TYPES_INT32]); break;
              }
              case 4: { // float
                UA_Float x = std::get<float>(val);
                UA_Variant_setScalarCopy(&in[i], &x, &UA_TYPES[UA_TYPES_FLOAT]); break;
              }
              case 5: { // double
                UA_Double x = std::get<double>(val);
                UA_Variant_setScalarCopy(&in[i], &x, &UA_TYPES[UA_TYPES_DOUBLE]); break;
              }
              case 6: { // string
                std::string s = std::get<std::string>(val);
                UA_String ua = UA_String_fromChars(s.c_str());
                UA_Variant_setScalarCopy(&in[i], &ua, &UA_TYPES[UA_TYPES_STRING]);
                UA_String_clear(&ua);
                break;
              }
              default: break; // monostate -> lässt Slot leer
            }
        }

        // Timeout temporär setzen und Call ausführen
        UA_ClientConfig *cfg = UA_Client_getConfig(client_);
        UA_UInt32 oldTo = cfg->timeout;
        cfg->timeout = timeoutMs;

        size_t outSz = 0; UA_Variant* out = nullptr;
        UA_StatusCode st = UA_Client_call(client_, obj, meth, inSz, in.data(), &outSz, &out); // offizielle API. :contentReference[oaicite:2]{index=2}

        cfg->timeout = oldTo;
        for (auto& v : in) UA_Variant_clear(&v);

        if (st == UA_STATUSCODE_GOOD) {
            ok = true;
            for (size_t i = 0; i < outSz; ++i) {
                const UA_Variant &vi = out[i];
                if (!UA_Variant_isScalar(&vi) || !vi.type || !vi.data) continue; // Absicherung. :contentReference[oaicite:3]{index=3}

                if (vi.type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
                    tmpOut[(int)i] = (*static_cast<UA_Boolean*>(vi.data) == UA_TRUE);
                } else if (vi.type == &UA_TYPES[UA_TYPES_INT16]) {
                    tmpOut[(int)i] = *static_cast<UA_Int16*>(vi.data);
                } else if (vi.type == &UA_TYPES[UA_TYPES_INT32]) {
                    tmpOut[(int)i] = *static_cast<UA_Int32*>(vi.data);
                } else if (vi.type == &UA_TYPES[UA_TYPES_FLOAT]) {
                    tmpOut[(int)i] = *static_cast<UA_Float*>(vi.data);
                } else if (vi.type == &UA_TYPES[UA_TYPES_DOUBLE]) {
                    tmpOut[(int)i] = *static_cast<UA_Double*>(vi.data);
                } else if (vi.type == &UA_TYPES[UA_TYPES_STRING]) {
                    const UA_String* s = static_cast<UA_String*>(vi.data);
                    std::string cpp((char*)s->data, s->length); // UA_String ist NICHT nullterminiert. :contentReference[oaicite:4]{index=4}
                    tmpOut[(int)i] = std::move(cpp);
                } else {
                    // TODO: weitere Typen bei Bedarf
                }
            }
        }

        if (out) UA_Array_delete(out, outSz, &UA_TYPES[UA_TYPES_VARIANT]);
        UA_NodeId_clear(&obj);
        UA_NodeId_clear(&meth);

        { std::lock_guard<std::mutex> lk(m); done = true; }
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lk(m);
    if (cv.wait_for(lk, std::chrono::milliseconds(timeoutMs + 500)) == std::cv_status::timeout)
        return false;

    if (ok) outputs = std::move(tmpOut);
    return ok;
}

bool PLCMonitor::callJob(const std::string& objNodeId,
                         const std::string& methNodeId,
                         UA_Int32 x, UA_Int32& yOut,
                         unsigned timeoutMs)
{
    std::mutex m; std::condition_variable cv;
    bool done=false, ok=false; UA_Int32 yTmp=0;

    std::cout << "[PLCMonitor] callJob ENTER obj=\"" << objNodeId
              << "\" meth=\"" << methNodeId
              << "\" x=" << x << " timeout=" << timeoutMs << "ms\n";

    // UA-Operation *im Monitor-Thread* ausführen
    post([&, objNodeId, methNodeId, x, timeoutMs]{
        std::cout << "[PLCMonitor] [ua] build NodeIds nsIndex=" << opt_.nsIndex
                  << " obj=\"" << objNodeId << "\" meth=\"" << methNodeId << "\"\n";

        UA_NodeId obj  = UA_NODEID_STRING_ALLOC(opt_.nsIndex, const_cast<char*>(objNodeId.c_str()));
        UA_NodeId meth = UA_NODEID_STRING_ALLOC(opt_.nsIndex, const_cast<char*>(methNodeId.c_str()));

        UA_Variant in[1]; UA_Variant_init(&in[0]);
        (void)UA_Variant_setScalarCopy(&in[0], &x, &UA_TYPES[UA_TYPES_INT32]);
        std::cout << "[PLCMonitor] [ua] input[0]=int32:" << x << "\n";

        UA_ClientConfig *cfg = UA_Client_getConfig(client_);
        UA_UInt32 oldTo = cfg->timeout;
        cfg->timeout = timeoutMs;

        size_t outSz = 0; UA_Variant* out = nullptr;
        UA_StatusCode st = UA_Client_call(client_, obj, meth, 1, in, &outSz, &out);

        cfg->timeout = oldTo; // zurücksetzen

        std::cout << "[PLCMonitor] [ua] UA_Client_call status=0x"
                  << std::hex << st << std::dec << " outSz=" << outSz << "\n";

        // Aufräumen Inputs
        UA_Variant_clear(&in[0]);

        if (st == UA_STATUSCODE_GOOD && outSz >= 1 &&
            UA_Variant_isScalar(&out[0]) &&
            out[0].type == &UA_TYPES[UA_TYPES_INT32] && out[0].data)
        {
            yTmp = *static_cast<UA_Int32*>(out[0].data);
            ok = true;
            std::cout << "[PLCMonitor] [ua] yOut=" << yTmp << "\n";
        } else {
            std::cout << "[PLCMonitor] [ua] no/invalid output variant\n";
        }

        if (out)
            UA_Array_delete(out, outSz, &UA_TYPES[UA_TYPES_VARIANT]);

        UA_NodeId_clear(&obj);
        UA_NodeId_clear(&meth);

        { std::lock_guard<std::mutex> lk(m); done = true; }
        cv.notify_one();
    });

    // Hier (Aufrufer-Thread) warten wir auf das Ergebnis, während der Main-Loop weiterpumpt.
    std::unique_lock<std::mutex> lk(m);
    if (cv.wait_for(lk, std::chrono::milliseconds(timeoutMs + 500)) == std::cv_status::timeout) {
        std::cout << "[PLCMonitor] callJob TIMEOUT (>" << (timeoutMs+500) << "ms)\n";
        return false;
    }

    if (ok) {
        yOut = yTmp;
        std::cout << "[PLCMonitor] callJob EXIT -> OK yOut=" << yOut << "\n";
    } else {
        std::cout << "[PLCMonitor] callJob EXIT -> FAIL\n";
    }
    return ok;
}
