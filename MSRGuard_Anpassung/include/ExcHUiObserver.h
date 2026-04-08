#pragma once

#include "Event.h"
#include "EventBus.h"
#include "Acks.h"
#include "ReactiveObserver.h"
#include "AgentGate.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

class ExcHUiObserver : public ReactiveObserver,
                       public std::enable_shared_from_this<ExcHUiObserver>
{
public:
    // attach() erstellt den Observer und subscribed sicher nach der Konstruktion
    static std::shared_ptr<ExcHUiObserver> attach(EventBus& bus,
                                                  std::string pythonSrcDir,
                                                  std::string scriptFile = "excH_kg_agent_ui.py",
                                                  int priority = 3,
                                                  std::shared_ptr<AgentGate> gate = nullptr);

    void onEvent(const Event& ev) override;

    void setEnabled(bool enabled) { enabled_ = enabled; }
    bool isEnabled() const { return enabled_; }

    std::shared_ptr<AgentGate> gate() const { return gate_; }

private:
    ExcHUiObserver(EventBus& bus,
                   std::string pythonSrcDir,
                   std::string scriptFile,
                   std::shared_ptr<AgentGate> gate);

    void subscribe(int priority);

    void launchPythonUI_async(const AgentStartAck& ack);

private:
    EventBus& bus_;
    std::string pythonSrcDir_;
    std::string scriptFile_;
    std::shared_ptr<AgentGate> gate_;

    bool enabled_ = true;

    std::mutex mx_;
    std::unordered_set<std::string> startedCorr_;
};
