#pragma once

#include <string>
#include <vector>
#include <functional>
#include <queue>
#include <mutex>
#include <memory>
#include <type_traits>
#include <future>
#include <map>
#include <variant>
#include <open62541/client.h>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/util.h>
#include "common_types.h"

class PLCMonitor {
public:
  
    // ---------- Task-Posting (läuft im Thread, der runIterate() aufruft) ----------
    using UaFn = std::function<void()>;
    void post(UaFn fn);
    template<class F,
             class Decayed = std::decay_t<F>,
             std::enable_if_t<!std::is_same_v<Decayed, UaFn>, int> = 0>
    void post(F&& f) {
        auto fn = std::make_shared<Decayed>(std::forward<F>(f)); // auch move-only
        UaFn job = [fn]() mutable { (*fn)(); };
        std::lock_guard<std::mutex> lk(qmx_);
        q_.push(std::move(job));
    }
    void processPosted(size_t max = 16);

    // ---------- Verbindungs-Optionen ----------
    struct Options {
        std::string endpoint;
        std::string username;
        std::string password;
        std::string certDerPath;
        std::string keyDerPath;
        std::string applicationUri;
        UA_UInt16   nsIndex = 2;
    };

    // ---------- ctor/dtor ----------
    explicit PLCMonitor(Options o);
    ~PLCMonitor();

    // ---------- Verbindung ----------
    bool connect();            // Basic256Sha256 + Sign&Encrypt + Username/PW + Zert/Key (per Options)
    void disconnect();

    // ---------- Client-Loop ----------
    UA_StatusCode runIterate(int timeoutMs = 0);   // vorantreiben (single-thread)
    bool waitUntilActivated(int timeoutMs = 3000); // bis Session aktiv

    bool callMethodTyped(const std::string& objNodeId,
                     const std::string& methNodeId,
                     const UAValueMap& inputs,   // index -> typed value
                     UAValueMap& outputs,        // index -> typed value
                     unsigned timeoutMs);

    bool readInt16At(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_Int16 &out) const;
    bool readUInt16At(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_UInt16 &out) const;
    bool readInt32At(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_Int32 &out) const;
    bool readUInt32At(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_UInt32 &out) const;
    bool readInt64At(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_Int64 &out) const;
    bool readUInt64At(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_UInt64 &out) const;
    // Liest eine boolsche Variable (identifier string, also z.B. "OPCUA.bool1", plus Namespace)
   // PLCMonitor.h (public)
    bool readBoolAt(const std::string& nodeIdStr, UA_UInt16 nsIndex, bool& out) const;

    bool readStringAt(const std::string& nodeIdStr, UA_UInt16 nsIndex, std::string& out) const;

    bool readFloatAt (const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_Float  &out) const;

    bool readDoubleAt(const std::string& nodeIdStr, UA_UInt16 nsIndex, UA_Double &out) const;


    // Optional: generische Ausgabe als String (falls du später mehr Typen vergleichen willst)
    bool readAsString(const std::string& nodeIdStr, UA_UInt16 nsIndex,
                    std::string& outValue, std::string& outTypeName) const;
    bool writeBool(const std::string& nodeIdStr, UA_UInt16 nsIndex, bool v);

    // ---------- Subscriptions ----------
    using Int16ChangeCallback = std::function<void(UA_Int16, const UA_DataValue&)>;
    using BoolChangeCallback  = std::function<void(UA_Boolean, const UA_DataValue&)>;

    bool subscribeInt16(const std::string& nodeIdStr,
                        UA_UInt16 nsIndex,
                        double samplingMs,
                        UA_UInt32 queueSize,
                        Int16ChangeCallback cb);

    bool subscribeBool(const std::string& nodeIdStr,
                       UA_UInt16 nsIndex,
                       double samplingMs,
                       UA_UInt32 queueSize,
                       BoolChangeCallback cb);

    void unsubscribe();
    // ---------- Komfort für deinen Secure-Testserver ----------
    static Options TestServerDefaults(const std::string& clientCertDer,
                                      const std::string& clientKeyDer,
                                      const std::string& endpoint = "opc.tcp://localhost:4840");

    bool connectToSecureTestServer(const std::string& clientCertDer,
                                   const std::string& clientKeyDer,
                                   const std::string& endpoint = "opc.tcp://localhost:4840");

    bool watchTriggerD2(double samplingMs = 0.0, UA_UInt32 queueSize = 10);

    // Low-level Zugriff (falls nötig)
    UA_Client* raw() const { return client_; }

    // Alles für Inventory
    struct InventoryRow {
    std::string nodeClass;   // "Variable", "Method", "Object"
    std::string nodeId;      // "ns=4;s=OPCUA.DiagnoseFinished", ...
    std::string dtypeOrSig;  // z. B. "Boolean" oder "in: [Int32], out: [Int32]"
    };

    // public:
    bool dumpPlcInventory(std::vector<InventoryRow>& out, const char* plcNameContains = "PLC");
    void printInventoryTable(const std::vector<InventoryRow>& rows) const;

    void postDelayed(int delayMs, UaFn fn);
    void processTimers();
    bool callJob(const std::string& objNodeId,
             const std::string& methNodeId,
             UA_Int32 x, UA_Int32& yOut,
             unsigned timeoutMs);
private:
    std::mutex qmx_;
    std::queue<UaFn> q_;

    using UaFn = std::function<void()>;
    struct TimedFn { std::chrono::steady_clock::time_point due; UaFn fn; };

    std::mutex tmx_;
    std::vector<TimedFn> timers_;
    std::atomic<bool> running_{false};

    static bool loadFileToByteString(const std::string& path, UA_ByteString &out);

    Options    opt_;
    UA_Client* client_=nullptr;

    PLCMonitor(const PLCMonitor&) = delete;
    PLCMonitor& operator=(const PLCMonitor&) = delete;

    UA_UInt32            subId_{0};
    UA_UInt32            monIdInt16_{0};
    UA_UInt32            monIdBool_{0};
    Int16ChangeCallback  onInt16Change_;
    BoolChangeCallback   onBoolChange_;
    std::mutex cbmx_;
    std::unordered_map<UA_UInt32, BoolChangeCallback> boolCbs_;

    static void dataChangeHandler(UA_Client*, UA_UInt32, void*, UA_UInt32, void*, UA_DataValue*);
};
