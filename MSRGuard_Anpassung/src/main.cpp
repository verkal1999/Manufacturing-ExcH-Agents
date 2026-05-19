// main.cpp
// Einstiegspunkt der Anwendung: initialisiert Python, den EventBus, den PLCMonitor
// und verbindet die Komponenten. Zusätzlich: startet ExcHUiObserver bei evUnknownFM.

#include "PLCMonitor.h"
#include "EventBus.h"
#include "ReactionManager.h"
#include "AckLogger.h"
#include "PythonRuntime.h"
#include "PythonWorker.h"
#include "ExcHUiObserver.h"
#include "AgentStartCoordinator.h"
#include "InventorySnapshot.h"
#include "InventorySnapshotUtils.h"
#include "FailureRecorder.h"
#include "TimeBlogger.h"
#include "AgentStartCoordinator.h"
#include "AgentGate.h"
#include <pybind11/embed.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

namespace py = pybind11;

static void prepend_to_path_env(const std::filesystem::path& dir) {
    if (dir.empty()) return;

#ifdef _WIN32
    const char* oldPathC = std::getenv("PATH");
    std::string newPath = dir.string();
    if (oldPathC && *oldPathC) newPath += ";" + std::string(oldPathC);
    _putenv_s("PATH", newPath.c_str());
#else
    const char* oldPathC = std::getenv("PATH");
    std::string newPath = dir.string();
    if (oldPathC && *oldPathC) newPath += ":" + std::string(oldPathC);
    setenv("PATH", newPath.c_str(), 1);
#endif
}

//static constexpr const char* kAgentUiScript = "excH_kg_agent_ui.py";
static constexpr const char* kAgentUiScript = "rag_agent_ui.py";

int main() {
    // 1) Interpreter starten (einmalig, global gehalten)
    PythonRuntime::ensure_started();

    // 2) GIL im Main-Thread freigeben, damit andere Threads (PythonWorker) Python ausführen können.
    static std::unique_ptr<py::gil_scoped_release> main_gil_release;
    main_gil_release = std::make_unique<py::gil_scoped_release>();

    // 3) Pfade zentral definieren (kommt aus CMake target_compile_definitions)
    const std::string src_dir = PY_SRC_DIR;

    // 4) Windows: venv-Python für std::system("python ...") bevorzugen
    //    Annahme gemäß deiner Beschreibung:
    //    - MSRGuardAnpassung und MA_Python_Agent liegen im selben Parent-Ordner
    //    - venv liegt unter: MA_Python_Agent/.venv311
#ifdef _WIN32
    try {
        const std::filesystem::path srcPath = std::filesystem::path(src_dir);
        const std::filesystem::path msrGuardRoot = srcPath.parent_path();            // .../MSRGuardAnpassung
        const std::filesystem::path workspaceRoot = msrGuardRoot.parent_path();      // .../(Parent von MSRGuardAnpassung)
        const std::filesystem::path venvScripts =
            workspaceRoot / ".venv311" / "Scripts";

        const std::filesystem::path venvPython = venvScripts / "python.exe";
        if (std::filesystem::exists(venvPython)) {
            prepend_to_path_env(venvScripts);
            std::cout << "[Env] PATH prepended with venv Scripts: " << venvScripts.string() << "\n";
        } else {
            std::cerr << "[Env] WARNING: venv python not found at: " << venvPython.string()
                      << "\n[Env] std::system(\"python ...\") may use system Python.\n";
        }
    } catch (const std::exception& ex) {
        std::cerr << "[Env] WARNING: could not adjust PATH for venv: " << ex.what() << "\n";
    }
#endif

    // 5) PythonWorker starten (führt Python-Jobs in eigenem Thread aus)
    PythonWorker::instance().start();

    // 6) sys.path setzen + KG_Interface warm-up import (läuft im PythonWorker-Thread)
    PythonWorker::instance().call([src_dir] {
        namespace py = pybind11;

        py::module_ sys  = py::module_::import("sys");
        py::list path = sys.attr("path").cast<py::list>();

        // Der Ordner, der KG_Interface.py enthält
        path.insert(0, py::cast(src_dir));

        py::print("[Py] exe=", sys.attr("executable"), " prefix=", sys.attr("prefix"));
        py::print("[Py] sys.path[0..2] = ",
                  sys.attr("path").attr("__getitem__")(0), ", ",
                  sys.attr("path").attr("__getitem__")(1), ", ",
                  sys.attr("path").attr("__getitem__")(2));

        py::object spec =
            py::module_::import("importlib.util").attr("find_spec")("msrguard.KG_Interface");

        if (spec.is_none()) {
            py::print("[KG] find_spec('msrguard.KG_Interface') -> None ; sys.path=", sys.attr("path"));
            throw std::runtime_error("msrguard.KG_Interface not found on sys.path");
        }

        py::module_::import("msrguard.KG_Interface");
        std::cout << "[KG] warm-up import done\n";
    });

    // 7) PLCMonitor konfigurieren + connect
    PLCMonitor::Options opt;
    opt.endpoint       = "opc.tcp://DESKTOP-LNJR8E0:4840";
    opt.username       = "AdminVD";
    opt.password       = "123456";
    opt.certDerPath    = R"(..\..\certificates\client_cert.der)";
    opt.keyDerPath     = R"(..\..\certificates\client_key.der)";
    opt.applicationUri = "urn:DESKTOP-LNJR8E0:Test:opcua-client";
    opt.nsIndex        = 4;

    PLCMonitor mon(opt);
    if (!mon.connect()) {
        std::cerr << "[Client] connect() failed\n";
        return 1;
    }
    std::cout << "[Client] connected\n";

    // 8) EventBus + ReactionManager + Logger + Abos
    EventBus bus;

    auto rm = std::make_shared<ReactionManager>(mon, bus);
    rm->setLogLevel(ReactionManager::LogLevel::Info);

    auto subD2 = bus.subscribe_scoped(EventType::evD2, rm, 4);
    // auto subD1 = bus.subscribe_scoped(EventType::evD1, rm, 4);
    // auto subD3 = bus.subscribe_scoped(EventType::evD3, rm, 4);

    auto ackLogger = std::make_shared<AckLogger>();
    auto subPlan   = bus.subscribe_scoped(EventType::evSRPlanned, ackLogger, 1);
    auto subDone   = bus.subscribe_scoped(EventType::evSRDone,    ackLogger, 1);
    auto subPlan2  = bus.subscribe_scoped(EventType::evMonActPlanned, ackLogger, 1);
    auto subDone2  = bus.subscribe_scoped(EventType::evMonActDone,    ackLogger, 1);
    auto subProcessFail = bus.subscribe_scoped(EventType::evProcessFail, ackLogger, 1);

    auto subKGRes  = bus.subscribe_scoped(EventType::evKGResult,  rm, 4);
    auto subKGTo   = bus.subscribe_scoped(EventType::evKGTimeout, rm, 4);
    auto subAgentDone = bus.subscribe_scoped(EventType::evAgentDone, rm, 4);
    auto subAgentAbort = bus.subscribe_scoped(EventType::evAgentAbort, rm, 4);
    auto subAgentFail  = bus.subscribe_scoped(EventType::evAgentFail, rm, 4);

    auto rec = std::make_shared<FailureRecorder>(bus);
    rec->subscribeAll();

    // NEU: Coordinator sitzt zwischen evUnknownFM + evIngestionDone -> evAgentStart
    auto agentCoord = AgentStartCoordinator::attach(bus, 3);


    auto subIngPlan = bus.subscribe_scoped(EventType::evIngestionPlanned, ackLogger, 1);
    auto subIngDone = bus.subscribe_scoped(EventType::evIngestionDone,    ackLogger, 1);
    auto subUnknown = bus.subscribe_scoped(EventType::evUnknownFM,        ackLogger, 1);
    auto subAgentAbort2 = bus.subscribe_scoped(EventType::evAgentAbort,   ackLogger, 1);
    auto subAgentFail2  = bus.subscribe_scoped(EventType::evAgentFail,    ackLogger, 1);

    // 9) Python-UI auswaehlen und bei evAgentStart starten.
    //    Direkt oben ueber kAgentUiScript umschalten.
    std::cout << "[AgentUI] Using UI script: " << kAgentUiScript << "\n";
    auto excHUiObserver = ExcHUiObserver::attach(bus, PY_SRC_DIR, kAgentUiScript, 3);

    // 10) Trigger-Subscriptions → Events (D1/D2/D3)
    mon.subscribeBool("OPCUA.TriggerD3", opt.nsIndex, 0.0, 10,
        [&](bool b, const UA_DataValue& dv) {
            static std::atomic<bool> initialized{false};
            static std::atomic<bool> prev{false};

            std::cout << "[TrigD3] b=" << (b ? "true" : "false")
                      << " sourceTs=" << static_cast<UA_UInt64>(dv.sourceTimestamp)
                      << " serverTs=" << static_cast<UA_UInt64>(dv.serverTimestamp)
                      << "\n";

            if (!initialized.exchange(true)) { prev = b; return; }
            if (!b) { prev = false; return; }
            if (prev.exchange(true)) return;

            mon.post([&]{
                InventorySnapshot inv;
                const bool ok = buildInventorySnapshotNow(mon, "PLC", inv);
                std::cout << "[Debug] Snapshot D3 = " << (ok ? "OK" : "FAIL") << "\n";
                dumpInventorySnapshot(inv);

                const auto now = std::chrono::steady_clock::now();
                const std::string corr = "evD3-" + std::to_string(now.time_since_epoch().count());

                bus.post({ EventType::evD3, now, std::any{ D2Snapshot{ corr, std::move(inv) } } });
            });
        });

    mon.subscribeBool("OPCUA.TriggerD1", opt.nsIndex, 0.0, 10,
        [&](bool b, const UA_DataValue& dv) {
            static std::atomic<bool> initialized{false};
            static std::atomic<bool> prev{false};

            std::cout << "[TrigD1] b=" << (b ? "true" : "false")
                      << " sourceTs=" << static_cast<UA_UInt64>(dv.sourceTimestamp)
                      << " serverTs=" << static_cast<UA_UInt64>(dv.serverTimestamp)
                      << "\n";

            if (!initialized.exchange(true)) { prev = b; return; }
            if (!b) { prev = false; return; }
            if (prev.exchange(true)) return;

            mon.post([&]{
                InventorySnapshot inv;
                const bool ok = buildInventorySnapshotNow(mon, "PLC", inv);
                std::cout << "[Debug] Snapshot D1 = " << (ok ? "OK" : "FAIL") << "\n";
                dumpInventorySnapshot(inv);

                const auto now = std::chrono::steady_clock::now();
                const std::string corr = "evD1-" + std::to_string(now.time_since_epoch().count());

                bus.post({ EventType::evD1, now, std::any{ D2Snapshot{ corr, std::move(inv) } } });
            });
        });

    mon.subscribeBool("OPCUA.TriggerD2", opt.nsIndex, 0.0, 10,
        [&](bool b, const UA_DataValue& dv) {
            static std::atomic<bool> initialized{false};
            static std::atomic<bool> prev{false};

            std::cout << "[TrigD2] b=" << (b ? "true" : "false")
                      << " sourceTs=" << static_cast<UA_UInt64>(dv.sourceTimestamp)
                      << " serverTs=" << static_cast<UA_UInt64>(dv.serverTimestamp)
                      << "\n";

            if (!initialized.exchange(true)) { prev = b; return; }
            if (!b) { prev = false; return; }
            if (prev.exchange(true)) return;

            mon.post([&]{
                InventorySnapshot inv;
                const bool ok = buildInventorySnapshotNow(mon, "PLC", inv);
                std::cout << "[Debug] Snapshot D2 = " << (ok ? "OK" : "FAIL") << "\n";
                dumpInventorySnapshot(inv);

                const auto now = std::chrono::steady_clock::now();
                const std::string corr = "evD2-" + std::to_string(now.time_since_epoch().count());

                bus.post({ EventType::evD2, now, std::any{ D2Snapshot{ corr, std::move(inv) } } });
            });
        });

    std::cout << "[Client] subscribed triggers\n";

    auto tb = std::make_shared<TimeBlogger>(bus);
    tb->subscribeAll();

    // 11) Main-Loop
    for (;;) {
        mon.runIterate(50);
        mon.processPosted(16);
        bus.process(16);
    }

    // (Nie erreicht) PythonWorker::instance().stop();
    return 0;
}
