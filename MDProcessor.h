#pragma once

#include "../Utils/MDShmem.h"
#include "../Utils/MDupdate.h"
#include "../Utils/simdjson/simdjson.h"
#include "ShmemManager.h"
#include "../Utils/SymbolIDManager.h"
#include <atomic>

struct Slot{
    std::string data;
    std::atomic<bool> is_ready{false};
};

static constexpr size_t MAX_SIZE = 4096;

struct RawBlock {
    // 4KB is usually enough for most market data JSON messages
    char buffer[MAX_SIZE + simdjson::SIMDJSON_PADDING];
    size_t len;
    std::atomic<bool> is_ready{false};
};

struct MDSlot{
    MDupdate data;
    std::atomic<bool> is_ready{false};
};

struct RawData{
    alignas(64) RawBlock data[256];
    alignas(64) std::atomic<uint8_t> next_write_index = 0;
    alignas(64) std::atomic<uint8_t> next_read_index = 0;
};

struct ProcessedData{
    alignas(64) MDSlot data[256];
    alignas(64) std::atomic<uint8_t> next_write_index = 0;
    alignas(64) std::atomic<uint8_t> next_read_index = 0;
};

class MDProcessor {
private:

    ShmemManager* mShmemManager;
    SymbolIDManager* mSymIDManager;

    MDupdate    currentMD;

    static MDProcessor* uniqueInstance;
    MDProcessor(){;}

    RawData data_queues[3];
    ProcessedData processed_data_queues[3];

    uint8_t current_raw_queue = 0;

public:

    static MDProcessor* getInstance();
    void startUp();
    void shutDown();
    void push_raw_data(const std::string& raw_json);
    void process_raw_data(const int& index);
    RawBlock* try_pop(const int& index);
    void release_slot(const int& index);
    bool try_pop(MDupdate& output, const int& index);
    void write_to_schmem();
    void process_quote(const simdjson::dom::object& obj, const int& index);
    void process_trade(const simdjson::dom::object& obj, const int& index);
    void process_imbalance(const simdjson::dom::object& obj, const int& index);
};