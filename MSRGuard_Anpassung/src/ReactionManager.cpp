// ReactionManager.cpp
// Zentrale Komponente für das Runtime Exception Handling in deiner MPA.
// Sie verarbeitet D2/D3-Snapshots, holt Failure-Mode-Kandidaten aus dem KG,
// wählt passende Failure Modes aus, baut Pläne für Monitoring Actions und System Reactions
// und führt diese Pläne über CommandForceFactory / PLCMonitor aus.
#include "ReactionManager.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include "InventorySnapshotUtils.h"
#include <nlohmann/json.hpp>
#include <pybind11/embed.h>
#include <type_traits>
#include "Acks.h"
#include "PLCCommandForce.h"
#include "CommandForceFactory.h"
#include "EventBus.h"
#include "PythonWorker.h"
#include <thread>
#include <chrono>
#include <unordered_map>
#include <sstream>
#include <optional>
#include "Event.h"
#include "PLCMonitor.h"
#include "Plan.h"
#include <cmath>
#include "MonActionForce.h"
#include "PlanJsonUtils.h"
#include "NodeIdUtils.h"

using json  = nlohmann::json;
using Clock = std::chrono::steady_clock;
namespace py = pybind11;

// ---------- Logging -----------------------------------------------------------
namespace {
struct NullBuf : public std::streambuf { int overflow(int c) override { return c; } };
NullBuf nb; std::ostream nullout_stream(&nb);
}

const char* ReactionManager::toCStr(LogLevel lvl) const {
    switch (lvl) {
        case LogLevel::Error:   return "ERR";
        case LogLevel::Warn:    return "WRN";
        case LogLevel::Info:    return "INF";
        case LogLevel::Debug:   return "DBG";
        case LogLevel::Trace:   return "TRC";
        case LogLevel::Verbose: return "VRB";
    }
    return "?";
}
std::ostream& ReactionManager::log(LogLevel lvl) const {
    if (isEnabled(lvl)) { std::cout << "[RM][" << toCStr(lvl) << "] "; return std::cout; }
    return nullout_stream;
}

std::string ReactionManager::makeCorrelationId(const char* evName) {
    return std::string(evName) + "-" + std::to_string(Clock::now().time_since_epoch().count());
}
static std::mutex pending_mx_;
std::unordered_map<std::string, Plan> pendingFallbackPlans_;
std::unordered_map<std::string, std::string> pendingProcessNames_;

namespace {
json snapshotToJson_flat(const InventorySnapshot& inv)
{
    auto add = [](json& arr, const std::string& id, const char* t, const json& v) {
        arr.push_back(json{{"id", id}, {"t", t}, {"v", v}});
    };

    json out;
    out["rows"] = json::array();
    for (const auto& r : inv.rows) {
        out["rows"].push_back({{"id", r.nodeId}, {"t", r.dtypeOrSig}, {"nodeClass", r.nodeClass}});
    }

    out["vars"] = json::array();
    for (const auto& [k, v] : inv.bools)   add(out["vars"], k.id, "bool", v);
    for (const auto& [k, v] : inv.strings) add(out["vars"], k.id, "string", v);
    for (const auto& [k, v] : inv.int16s)  add(out["vars"], k.id, "int16", v);
    for (const auto& [k, v] : inv.uint16s) add(out["vars"], k.id, "uint16", v);
    for (const auto& [k, v] : inv.int32s)  add(out["vars"], k.id, "int32", v);
    for (const auto& [k, v] : inv.uint32s) add(out["vars"], k.id, "uint32", v);
    for (const auto& [k, v] : inv.int64s)  add(out["vars"], k.id, "int64", v);
    for (const auto& [k, v] : inv.uint64s) add(out["vars"], k.id, "uint64", v);
    for (const auto& [k, v] : inv.floats)  add(out["vars"], k.id, "float", v);
    return out;
}

std::string wrapSnapshot(const std::string& js)
{
    return "==InventorySnapshot==" + js + "==InventorySnapshot==";
}
}

// ---------- Konstruktor: Worker-Thread ---------------------------------------
ReactionManager::ReactionManager(PLCMonitor& mon, EventBus& bus)
    : mon_(mon), bus_(bus)
{
    worker_ = std::jthread([this](std::stop_token st){
        for (;;) {
            std::function<void(std::stop_token)> job;
            {
                std::unique_lock<std::mutex> lk(job_mx_);
                job_cv_.wait(lk, [&]{ return st.stop_requested() || !jobs_.empty(); });
                if (st.stop_requested() && jobs_.empty()) break;
                job = std::move(jobs_.front());
                jobs_.pop();
            }
            try { job(st); }
            catch (const std::exception& e) { log(LogLevel::Warn) << "[worker] job failed: " << e.what() << "\n"; }
        }
    });
}

ReactionManager::~ReactionManager() {
    if (worker_.joinable()) {
        worker_.request_stop();      // Stop-Flag setzen
        { std::lock_guard<std::mutex> lk(job_mx_); }
        job_cv_.notify_all();        // WARTER wecken, damit Prädikat neu geprüft wird
        // jthread-Destruktor joint automatisch
    }
}

// ---------- Event-Entry -------------------------------------------------------
void ReactionManager::onEvent(const Event& ev) {
        // --- NEU: UI/Agent fertig -> pending Fallback ausführen ---
    if (ev.type == EventType::evAgentAbort) {
        auto d = std::any_cast<AgentAbortAck>(&ev.payload);
        if (!d) return;

        {
            std::lock_guard<std::mutex> lk(pending_mx_);
            pendingFallbackPlans_.erase(d->correlationId);
            pendingProcessNames_.erase(d->correlationId);
        }

        log(LogLevel::Warn) << "[RM] AgentAbort corr=" << d->correlationId
                            << " -> no pulse executed\n";
        return;
    }

    if (ev.type == EventType::evAgentFail) {
        auto d = std::any_cast<AgentFailAck>(&ev.payload);
        if (!d) return;

        {
            std::lock_guard<std::mutex> lk(pending_mx_);
            pendingFallbackPlans_.erase(d->correlationId);
            pendingProcessNames_.erase(d->correlationId);
        }

        log(LogLevel::Warn) << "[RM] AgentFail corr=" << d->correlationId
                            << " exitCode=" << d->exitCode
                            << " -> no pulse executed\n";
        return;
    }

    if (ev.type == EventType::evAgentDone) {
        auto d = std::any_cast<AgentDoneAck>(&ev.payload);
        if (!d) return;

        // Plan (DiagnoseFinished-Puls) optional aus Pending-Map holen und immer bereinigen
        Plan plan;
        {
            std::lock_guard<std::mutex> lk(pending_mx_);
            auto it = pendingFallbackPlans_.find(d->correlationId);
            if (it != pendingFallbackPlans_.end()) {
                plan = it->second;
                pendingFallbackPlans_.erase(it);
                pendingProcessNames_.erase(d->correlationId);
            }
        }

        // Optional: “continue” aus resultJson auswerten (wenn du das so baust)
        bool proceed = (d->rc != 0);
        try {
            if (!d->resultJson.empty()) {
                auto jr = nlohmann::json::parse(d->resultJson);
                proceed = jr.value("continue", proceed);
            }
        } catch (...) {
            // wenn parsing kaputt ist: nicht blockieren
        }

        if (!proceed) {
            log(LogLevel::Warn) << "[RM] AgentDone corr=" << d->correlationId
                                << " proceed=false -> no pulse executed (check your PLC behavior!)\n";
            // Ich empfehle hier trotzdem “auto proceed” zu machen, sonst hängt die SPS ggf.
            // proceed = true;
            return;
        }

        if (plan.ops.empty()) {
            plan = buildPlanFromComparison(d->correlationId, ComparisonReport{false, {}});
        }

        // Wichtig: nicht im Event-Thread blockieren -> in Worker-Queue schieben
        {
            std::lock_guard<std::mutex> lk(job_mx_);
            jobs_.push([this, plan](std::stop_token st) mutable {
                if (st.stop_requested()) return;

                // evAgentDone soll (bei "Weiter") das DiagnoseFinished-Pulsing auslösen.
                // Hier bewusst ohne SRDone/ProcessFail-Acks, um keine erneute KG-Ingestion zu triggern.
                PLCCommandForce cf(mon_, /*oq*/nullptr);
                (void)cf.execute(plan);
            });
        }
        job_cv_.notify_one();

        return;
    }

    const char* evName = nullptr;
    switch (ev.type) {
        case EventType::evD1: evName = "evD1"; break;
        case EventType::evD2: evName = "evD2"; break;
        case EventType::evD3: evName = "evD3"; break;
        default: return;
    }

    // Nur evD2 hat hier „Arbeit“ – und zwar *ausschließlich* mit dem Snapshot aus der Payload.
    std::string       corr = makeCorrelationId(evName);
    InventorySnapshot inv;

    if (ev.type == EventType::evD2) {
        if (auto p = std::any_cast<D2Snapshot>(&ev.payload)) {
            if (!p->correlationId.empty()) corr = p->correlationId;
            inv = p->inv;
        } else {
            log(LogLevel::Warn) << "evD2 ohne D2Snapshot-Payload -> ignoriere\n";
            return;
        }
    } else {
        log(LogLevel::Info) << "received " << evName << " corr=" << corr << " (no work)\n";
        return;
    }

    log(LogLevel::Info) << "onEvent ENTER " << evName << " corr=" << corr << "\n";
    logInventoryVariables(inv);
    const std::string processName = getStringFromCache(inv, /*ns*/4, "OPCUA.lastExecutedProcess");
    const std::string triggerEvent = evName ? evName : "";

    // --- Worker-Job -----------------------------------------------------------
    {
        std::lock_guard<std::mutex> lk(job_mx_);
        jobs_.push([this, corr, inv, processName, triggerEvent](std::stop_token st) mutable {
            log(LogLevel::Info) << "[worker] corr=" << corr << " START\n";
            const auto lap = [this, t0=Clock::now()](const char* tag) {
                auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now()-t0).count();
                log(LogLevel::Info) << "[timer] " << tag << " +" << dt << " ms\n";
            };

            // 1) KG-Parameter anhand unterbrochenem Skill (nur Cache!)
            const std::string interruptedSkill = getLastExecutedSkill(inv);
            std::string srows;
            try {
                srows = PythonWorker::instance().call([&](){
                    py::module_ sys = py::module_::import("sys");
                    py::list path = sys.attr("path").cast<py::list>();
                    py::module_ kg = py::module_::import("msrguard.KG_Interface");
                    py::object kgi = kg.attr("KGInterface")();
                    if (interruptedSkill.empty()) {
                        // Fallback: ggf. neutraler Skillname
                        return std::string(R"({"rows":[]})");
                    }
                    py::object res  = kgi.attr("getFailureModeParameters")(interruptedSkill.c_str());
                    return std::string(py::str(res));
                });
                log(LogLevel::Info) << "[worker] KG.getFailureModeParameters OK json_len=" << srows.size()
                                    << " preview=\"" << srows/*.substr(0, std::min<size_t>(srows.size(), 120))*/ << "\"\n";
            } catch (const std::exception& e) {
                log(LogLevel::Warn) << "[worker] KG error: " << e.what() << "\n";
                srows = R"({"rows":[]})";
            }
            lap("kg-params-ready");
            if (st.stop_requested()) { log(LogLevel::Warn) << "[worker] stop requested -> abort corr=" << corr << "\n"; return; }

            // 2) Kandidaten parsen & Checks gegen *Cache*
            auto potCands = normalizeKgPotFM(srows);
            std::vector<std::string> winners;
            winners.reserve(potCands.size());
            for (const auto& c : potCands) {
                auto rep = compareAgainstCache(inv, c.expects);
                if (rep.allOk) winners.push_back(c.potFM);
            }
            lap("potFM-selected");

            // 3) MonitoringActions je Winner via Winner-Filter
            if (!winners.empty()) {
                auto wf = CommandForceFactory::createWinnerFilter(
                    mon_, bus_,
                    [this](const std::string& fm){ return this->fetchMonitoringActionForFM(fm); },
                    /*defaultTimeoutMs=*/30000
                );
                winners = wf->filter(winners, corr, processName);   // <— Wichtig: filter(...) statt tryExecute(...)
                lap("monact-evaluated");
            }

            // 4) Genau ein Winner? -> SystemReaction via Winner-Filter
            if (winners.size() == 1) {
                const std::string& winner = winners.front();
                bus_.post(Event{
                        EventType::evGotFM, Clock::now(),
                        std::any{ GotFMAck {
                            corr,
                            winner
                        } }
                    });
                auto wfSys = CommandForceFactory::createSystemReactionFilter(
                    mon_, bus_,
                    [this](const std::string& fmIri){ return fetchSystemReactionForFM(fmIri); },
                    /*defaultTimeoutMs=*/30000
                );
                winners = wfSys->filter(winners, corr, processName); // <— ebenfalls filter(...)
                log(LogLevel::Info) << "[worker] corr=" << corr << " END (winner ok)\n";
                return;
            // 5) Fallback bei 0 oder >1 Gewinnern -> DiagnoseFinished-Puls
            } else {
                std::string summary;
                if (potCands.empty()) {
                    log(LogLevel::Info) << "[potFM] KG lieferte 0 Kandidaten -> UnknownFM + Fallback Agent";
                    summary = std::string("KG: no failure modes for skill '") + interruptedSkill + "'";
                } else if (winners.empty()) {
                    log(LogLevel::Info) << "[potFM] keine Kandidaten übrig nach MonAct -> Fallback Agent";
                    summary = "No candidates after MonitoringAction filter";
                } else {
                    log(LogLevel::Warn) << "[potFM] mehrdeutige Kandidaten (" << winners.size() << ") -> Fallback Agent";
                    summary = "Ambiguous candidates after KG/filters";
                }

                const std::string snapshotWrapped = wrapSnapshot(snapshotToJson_flat(inv).dump());
                const std::string uiProcessName = processName.empty() ? "UnknownFM" : processName;

                // -> UI/Agent triggern + Ingestion triggern
                bus_.post(Event{
                    EventType::evUnknownFM, Clock::now(),
                    std::any{ UnknownFMAck{
                        corr,
                        uiProcessName,
                        summary,
                        triggerEvent,
                        snapshotWrapped
                    } }
                });

                // Fallback-Plan (DiagnoseFinished-Puls) nur speichern, NICHT ausführen
                auto plan = buildPlanFromComparison(corr, ComparisonReport{false, {}});

                {
                    std::lock_guard<std::mutex> lk(pending_mx_);
                    pendingFallbackPlans_[corr] = plan;
                    pendingProcessNames_[corr]  = uiProcessName;
                }

                log(LogLevel::Info) << "[worker] corr=" << corr
                                    << " fallback stored -> waiting for evAgentDone\n";
                return;
            }
        }
            
        );
        job_cv_.notify_one();
    }

    log(LogLevel::Info) << "onEvent EXIT " << evName << " corr=" << corr << " (worker enqueued)\n";
}

// ---------- Inventar / Cache-Helper ------------------------------------------
void ReactionManager::logInventoryVariables(const InventorySnapshot& inv) const {
    log(LogLevel::Info) << "buildInventorySnapshot BOOL vars=" << inv.bools.size()
                        << " | STR vars=" << inv.strings.size()
                        << " | I16 vars=" << inv.int16s.size()
                        << " | U16 vars=" << inv.uint16s.size()
                        << " | I32 vars=" << inv.int32s.size()
                        << " | U32 vars=" << inv.uint32s.size()
                        << " | I64 vars=" << inv.int64s.size()
                        << " | U64 vars=" << inv.uint64s.size()
                        << " | FP vars="  << inv.floats.size() << "\n";

    if (!isEnabled(LogLevel::Debug)) return;

    std::cout << "[Inventory] Variablen + Typen (mit Cache-Werten, falls vorhanden):\n";
    for (const auto& r : inv.rows) {
        std::cout << "  - " << r.nodeId << "  (" << r.dtypeOrSig << ")\n";
    }
}

std::string ReactionManager::getStringFromCache(const InventorySnapshot& inv, uint16_t ns, const std::string& id) {
    auto it = inv.strings.find(NodeKey{ns,'s',id});
    return it == inv.strings.end() ? std::string{} : it->second;
}

std::string ReactionManager::getLastExecutedSkill(const InventorySnapshot& inv) {
    // Nur Cache verwenden (kein UA-Read)!
    return getStringFromCache(inv, /*ns*/4, "OPCUA.lastExecutedSkill");
}

// ---------- KG-Brücke (Python) -----------------------------------------------
std::string ReactionManager::fetchFailureModeParameters(const std::string& skillName) {
    // (nicht direkt genutzt – wir rufen oben PythonWorker inline)
    return {};
}
std::string ReactionManager::fetchMonitoringActionForFM(const std::string& fmIri) {
    try {
        return PythonWorker::instance().call([&]() -> std::string {
            py::module_ sys = py::module_::import("sys");
            py::list path   = sys.attr("path").cast<py::list>();
            py::module_ kg = py::module_::import("msrguard.KG_Interface");
            py::object kgi = kg.attr("KGInterface")();
            py::object res  = kgi.attr("getMonitoringActionForFailureMode")(fmIri.c_str());
            return std::string(py::str(res));
        });
    } catch (...) { return R"({"rows":[]})"; }
}
std::string ReactionManager::fetchSystemReactionForFM(const std::string& fmIri) {
    try {
        return PythonWorker::instance().call([&]() -> std::string {
            py::module_ sys = py::module_::import("sys");
            py::list path   = sys.attr("path").cast<py::list>();
            py::module_ kg = py::module_::import("msrguard.KG_Interface");
            py::object kgi = kg.attr("KGInterface")();
            py::object res  = kgi.attr("getSystemreactionForFailureMode")(fmIri.c_str());
            //std::cout << std::string(py::str(res)) << "/n";
            return std::string(py::str(res));
        });
    } catch (...) { return R"({"rows":[]})"; }
}

// ---------- Normalisieren & Vergleichen --------------------------------------
static bool json_get_first_object(const json& j, json& out) {
    if (j.is_object()) { out = j; return true; }
    if (j.is_array()) {
        if (j.empty()) return false;
        if (j[0].is_object()) { out = j[0]; return true; }
        if (j.size()>=2 && j[1].is_object()) { out = j[1]; return true; } // [header, rows]
    }
    return false;
}

std::vector<ReactionManager::KgExpect>
ReactionManager::normalizeKgResponse(const std::string& rowsJson) {
    std::vector<KgExpect> out;
    if (rowsJson.empty()) return out;

    json j;
    try { j = json::parse(rowsJson); } catch (...) { return out; }

    json rows;
    if (!json_get_first_object(j, rows)) return out;

    auto arrIt = rows.find("rows");
    if (arrIt == rows.end() || !arrIt->is_array()) return out;

    for (const auto& r : *arrIt) {
        if (!r.contains("id") || !r.contains("t") || !r.contains("v")) continue;
        const std::string idText = r["id"].get<std::string>();
        const std::string tIn    = r["t"].get<std::string>();
        std::string t = tIn;
        std::transform(t.begin(), t.end(), t.begin(),
                    [](unsigned char c){ return static_cast<char>(std::tolower(c)); });

        // >>> UA-NodeId "ns=4;s=OPCUA.x" → NodeKey{ns=4,'s',"OPCUA.x"}
        uint16_t ns = 4; std::string idStr; char idType='s';
        if (!parseNsAndId(idText, ns, idStr, idType)) { idStr = idText; idType='s'; } // erlaubt auch Kurzform "OPCUA.x"
        KgExpect e; e.key = NodeKey{ ns, idType, idStr };
        if      (t=="b" || t=="bool" || t=="boolean") {
            e.kind = KgValKind::Bool;    e.expectedBool = r["v"].get<bool>();
        }
        else if (t=="i" || t=="int" || t=="int16" || t=="i16") {
            e.kind = KgValKind::Int16;   e.expectedI16  = r["v"].get<int16_t>();
        }
        else if (t=="f" || t=="float" || t=="double") {
            e.kind = KgValKind::Float64; e.expectedF64  = r["v"].get<double>();
        }
        else {
            e.kind = KgValKind::String;  e.expectedStr  = r["v"].get<std::string>();
        }
        out.push_back(std::move(e));
    }
    return out;
}

std::vector<ReactionManager::KgCandidate>
ReactionManager::normalizeKgPotFM(const std::string& srows) {
    log(LogLevel::Info) << "normalizeKgPotFM ENTER len=" << srows.size() << "\n";

    std::vector<KgCandidate> out;

    // 1) Optional: äußere Quotes entfernen (manche Python-Returns kommen als quoted-String)
    std::string s = srows;
    if (s.size() >= 2 && s.front()=='"' && s.back()=='"') {
        s = s.substr(1, s.size()-2);
    }

    // kleine Helfer
    auto skipWS = [&](size_t& p) {
        while (p < s.size()) {
            char c = s[p];
            if (c==' ' || c=='\t' || c=='\r' || c=='\n') { ++p; continue; }
            break;
        }
    };
    auto trimInPlace = [](std::string& x) {
        size_t a = x.find_first_not_of(" \t");
        size_t b = x.find_last_not_of(" \t");
        x = (a==std::string::npos) ? std::string{} : x.substr(a, b-a+1);
    };

    // 2) Mehrere IRI+JSON-Paare hintereinander parsen
    size_t pos = 0;
    size_t idx = 0;
    while (true) {
        skipWS(pos);
        if (pos >= s.size()) break;

        // IRI bis zum Zeilenende lesen
        size_t eol = s.find_first_of("\r\n", pos);
        std::string iri = (eol==std::string::npos) ? s.substr(pos) : s.substr(pos, eol-pos);
        trimInPlace(iri);
        if (iri.empty()) {
            if (eol==std::string::npos) break;
            pos = eol + 1;
            continue;
        }

        // zur nächsten Nicht-CR/LF-Position
        pos = (eol==std::string::npos) ? s.size() : eol;
        while (pos < s.size() && (s[pos]=='\r' || s[pos]=='\n')) ++pos;
        skipWS(pos);

        // Nächster Block muss ein JSON-Objekt sein, das bei '{' startet
        if (pos >= s.size() || s[pos] != '{') {
            log(LogLevel::Warn) << "[potFM#" << (idx+1) << "] expected JSON after IRI, got pos=" << pos << "\n";
            continue;
        }

        // Ausgewogenes JSON ab pos erfassen (Klammerzähler, string-aware)
        size_t start = pos;
        int    depth = 0;
        bool   inStr = false, esc = false;

        for (; pos < s.size(); ++pos) {
            char c = s[pos];
            if (inStr) {
                if (esc) { esc=false; continue; }
                if (c=='\\') { esc=true; continue; }
                if (c=='"') { inStr=false; }
                continue;
            }
            if (c=='"') { inStr=true; continue; }
            if (c=='{') { ++depth; continue; }
            if (c=='}') { --depth; if (depth==0) { ++pos; break; } }
        }
        const std::string jsonPart = s.substr(start, pos-start);

        ++idx;
        log(LogLevel::Debug) << "[potFM#" << idx << "] iri='" << iri << "' json.len=" << jsonPart.size() << "\n";

        // 3) JSON-Block wie gehabt in Erwartungen verwandeln
        auto expects = normalizeKgResponse(jsonPart);
        if (expects.empty()) {
            log(LogLevel::Warn) << "[potFM#" << idx << "] no expects parsed\n";
        }

        out.push_back(KgCandidate{ iri, std::move(expects) });

        // danach geht die while-Schleife weiter -> evtl. nächstes IRI+JSON-Paar
    }

    log(LogLevel::Info) << "normalizeKgPotFM EXIT candidates=" << out.size() << "\n";
    return out;
}


ReactionManager::ComparisonReport
ReactionManager::compareAgainstCache(const InventorySnapshot& inv,
                                     const std::vector<KgExpect>& expects)
{
    ComparisonReport rep; rep.allOk = true;

    auto checkOne = [&](const KgExpect& e)->ComparisonItem {
        ComparisonItem it; it.key = e.key; it.ok = false;

        switch (e.kind) {
            case KgValKind::Bool: {
                auto f = inv.bools.find(e.key);
                if (f != inv.bools.end()) { it.ok = (f->second == e.expectedBool); if(!it.ok) it.detail="bool diff"; }
                else {
                    it.detail = "bool not in cache";
                    log(LogLevel::Debug) << "[cache-miss] " << nodeKeyToStr(e.key) << "\n";
                }
            } break;
            case KgValKind::Int16: {
                auto f = inv.int16s.find(e.key);
                if (f != inv.int16s.end()) { it.ok = (f->second == e.expectedI16); if(!it.ok) it.detail="i16 diff"; }
                else {
                    it.detail = "Int16 not in cache";
                    log(LogLevel::Debug) << "[cache-miss] " << nodeKeyToStr(e.key) << "\n";
                }
            } break;
            case KgValKind::Float64: {
                auto f = inv.floats.find(e.key);
                if (f != inv.floats.end()) { it.ok = (f->second == e.expectedF64); if(!it.ok) it.detail="f64 diff"; }
                else {
                    it.detail = "Float64 not in cache";
                    log(LogLevel::Debug) << "[cache-miss] " << nodeKeyToStr(e.key) << "\n";
                }
            } break;
            case KgValKind::String: {
                auto f = inv.strings.find(e.key);
                if (f != inv.strings.end()) { it.ok = (f->second == e.expectedStr); if(!it.ok) it.detail="str diff"; }
                else {
                    it.detail = "str not in cache";
                    log(LogLevel::Debug) << "[cache-miss] " << nodeKeyToStr(e.key) << "\n";
                }
            } break;
        }
        return it;
    };

    for (const auto& e : expects) {
        auto it = checkOne(e);
        rep.allOk = rep.allOk && it.ok;
        rep.items.push_back(std::move(it));
    }
    return rep;
}

std::vector<ReactionManager::KgCandidate>
ReactionManager::selectPotFMByChecks(const InventorySnapshot& inv,
                                     const std::vector<KgCandidate>& cands)
{
    std::vector<KgCandidate> winners;
    for (const auto& c : cands) {
        auto rep = this->compareAgainstCache(inv, c.expects);
        if (rep.allOk) winners.push_back(c);
    }
    return winners;
}

// ---------- Plan-Erstellung & -Ausführung ------------------------------------
Plan ReactionManager::buildPlanFromComparison(const std::string& corr,
                                              const ComparisonReport&) const
{
    // Minimal-Fallback: DiagnoseFinished pulsen
    Plan p; p.correlationId = corr; p.resourceId = "PLC";

    Operation op;
    op.type      = OpType::PulseBool;
    op.ns        = 4;
    op.nodeId    = "OPCUA.DiagnoseFinished";
    op.arg       = "preclear";
    op.timeoutMs = 100;

    p.ops.push_back(op);
    return p;
}

void ReactionManager::createCommandForceForPlanAndAck(const Plan& plan,
                                                      bool checksOk,
                                                      const std::string& processNameForFail)
{
    log(LogLevel::Info) << "createCommandForceForPlanAndAck ENTER ops=" << plan.ops.size() << "\n";

    // Ack: PLANNED
    bus_.post(Event{
        EventType::evSRPlanned, Clock::now(),
        std::any{ ReactionPlannedAck{
            plan.correlationId,
            plan.resourceId,
            checksOk
              ? std::string("SystemReaction/MonitoringAction (KG)")
              : std::string("KG checks fail or ambiguous -> pulse DiagnoseFinished")
        } }
    });

    bool allOk = true;
    for (const auto& op : plan.ops) {
        auto cf = CommandForceFactory::createForOp(op, &mon_, bus_);
        if (!cf) { allOk = false; continue; }
        int rc = cf->execute(plan);
        allOk = allOk && (rc != 0);
    }

    const bool hasCall = std::any_of(plan.ops.begin(), plan.ops.end(),
                                     [](const Operation& o){ return o.type==OpType::CallMethod; });
    if (!hasCall) {
        bus_.post(Event{
            EventType::evProcessFail, Clock::now(),
            std::any{ ProcessFailAck{
                plan.correlationId,
                processNameForFail,
                "No unique system reaction; fallback used."
            } }
        });
    }

    bus_.post(Event{
        EventType::evSRDone, Clock::now(),
        std::any{ ReactionDoneAck{
            plan.correlationId,
            allOk ? 1 : 0,
            allOk ? "OK" : "FAIL"
        } }
    });

    log(LogLevel::Info) << "createCommandForceForPlanAndAck EXIT\n";
}
