#include "MDProcessor.h"
#include "../Utils/Time_functions.h"
#include "../Utils/math_functions.h"
#include "glog/logging.h"
#include <string>
#include <string_view>
#include <ctime>
#include <cstdint>
#include <iostream>

MDProcessor* MDProcessor::uniqueInstance = nullptr;

MDProcessor* MDProcessor::getInstance(){
    if(uniqueInstance == nullptr){
        uniqueInstance = new MDProcessor();
    }
    return uniqueInstance;
}

void MDProcessor::startUp(){
    mShmemManager = ShmemManager::getInstance();
    mSymIDManager = SymbolIDManager::getInstance();
}

void MDProcessor::shutDown(){
    
}

void MDProcessor::process_quote(const simdjson::dom::object& _obj){

    while(true){
        uint8_t current_idx = processed_data_queue.next_write_index.load(std::memory_order_acquire);
        // Check if data is ready
        
        if (processed_data_queue.data[current_idx].is_ready.load(std::memory_order_acquire)) continue;

        // Try to claim this slot atomically
        if (processed_data_queue.next_write_index.compare_exchange_weak(current_idx, current_idx + 1)) {
            processed_data_queue.data[current_idx].data.m_type = md_type::QUOTE;
            processed_data_queue.data[current_idx].data.m_symbolId = mSymIDManager->getID(_obj["sym"].get_string());
            processed_data_queue.data[current_idx].data.m_bid_price = roundToNearestCent(Price(_obj["bp"].get_double() * DOLLAR));
            processed_data_queue.data[current_idx].data.m_ask_price = roundToNearestCent(Price(_obj["ap"].get_double() * DOLLAR));
            processed_data_queue.data[current_idx].data.m_bid_quant = Shares(_obj["bs"].get_int64() * 100);
            processed_data_queue.data[current_idx].data.m_ask_quant = Shares(_obj["as"].get_int64() * 100);

            //Timestamp conversion
            processed_data_queue.data[current_idx].data.m_timestamp = _obj["t"].get_int64() * MILLI_SECONDS;

            processed_data_queue.data[current_idx].is_ready.store(true, std::memory_order_release);
            break;
        }
    }
}
    
void MDProcessor::process_trade(const simdjson::dom::object& _obj){
    while(true){
        uint8_t current_idx = processed_data_queue.next_write_index.load(std::memory_order_acquire);
        // Check if data is ready
        
        if (processed_data_queue.data[current_idx].is_ready.load(std::memory_order_acquire)) continue;

        // Try to claim this slot atomically
        if (processed_data_queue.next_write_index.compare_exchange_weak(current_idx, current_idx + 1)) {
            processed_data_queue.data[current_idx].data.m_type = md_type::NONE;
            simdjson::dom::array conditions;
            auto error = _obj.at_key("c").get(conditions);
            if (!error) {
                for(simdjson::dom::element val : conditions){
                    int64_t code = val.get_int64();
                    int64_t exchange = Shares(_obj["x"].get_int64());
                    if(code == 16){
                        if(exchange == 10){
                            processed_data_queue.data[current_idx].data.m_type = md_type::NYSEOPEN;
                            processed_data_queue.data[current_idx].data.m_ask_price = code;
                            processed_data_queue.data[current_idx].data.m_ask_quant = exchange;
                        }
                        else if(exchange == 12){
                            processed_data_queue.data[current_idx].data.m_type = md_type::NASDOPEN;
                            processed_data_queue.data[current_idx].data.m_ask_price = code;
                            processed_data_queue.data[current_idx].data.m_ask_quant = exchange;
                        }
                    }
                }
                if(processed_data_queue.data[current_idx].data.m_type == md_type::NONE)
                    processed_data_queue.data[current_idx].data.m_type = md_type::PRINT;
            }
            else
                processed_data_queue.data[current_idx].data.m_type = md_type::PRINT;
            processed_data_queue.data[current_idx].data.m_symbolId = mSymIDManager->getID(_obj["sym"].get_string());
            processed_data_queue.data[current_idx].data.m_bid_price = roundToNearestCent(Price(_obj["p"].get_double() * DOLLAR));
            processed_data_queue.data[current_idx].data.m_bid_quant = Shares(_obj["s"].get_int64());

            //Timestamp conversion
            processed_data_queue.data[current_idx].data.m_timestamp = _obj["t"].get_int64() * MILLI_SECONDS;
            
            processed_data_queue.data[current_idx].is_ready.store(true, std::memory_order_release);
            break;
        }
    }
}

void MDProcessor::process_NYSE_imbalance(const simdjson::dom::object& _obj){
    while(true){
        uint8_t current_idx = processed_data_queue.next_write_index.load(std::memory_order_acquire);
        // Check if data is ready
        
        if (processed_data_queue.data[current_idx].is_ready.load(std::memory_order_acquire)) continue;

        // Try to claim this slot atomically
        if (processed_data_queue.next_write_index.compare_exchange_weak(current_idx, current_idx + 1)) {
            processed_data_queue.data[current_idx].data.m_type = md_type::IMBALANCE;
            processed_data_queue.data[current_idx].data.m_symbolId = mSymIDManager->getID(_obj["T"].get_string());
            processed_data_queue.data[current_idx].data.m_bid_price = roundToNearestCent(Price(_obj["b"].get_double() * DOLLAR));
            processed_data_queue.data[current_idx].data.m_bid_quant = Shares(_obj["o"].get_int64());
            processed_data_queue.data[current_idx].data.m_ask_quant = Shares(_obj["p"].get_int64());

            //Timestamp conversion
            processed_data_queue.data[current_idx].data.m_timestamp = parse_timestring(_obj["t"].get_string());

            // std::cout << "Trade price: " << processed_data_queue.data[current_idx].data.m_bid_price << std::endl;
            // std::cout << "Trade quant: " << processed_data_queue.data[current_idx].data.m_bid_quant << std::endl;
            processed_data_queue.data[current_idx].is_ready.store(true, std::memory_order_release);
            break;
        }
    }
}

void MDProcessor::push_raw_data(std::string raw_json){
    while(data_queue.data[data_queue.next_write_index].is_ready.load(std::memory_order_acquire));
    data_queue.data[data_queue.next_write_index].data = std::move(raw_json);
    data_queue.data[data_queue.next_write_index].is_ready.store(true, std::memory_order_release);
    data_queue.next_write_index.fetch_add(1, std::memory_order_release);
}

void MDProcessor::process_raw_data(){
    std::string data;
    simdjson::dom::parser parser;

    while(true){
        if(try_pop(data)){
            simdjson::padded_string padded_json_string(data);

            for(simdjson::dom::object obj : parser.parse(padded_json_string)){
                for(const auto& key_value : obj) {
                    if(key_value.key == "ev"){
                        std::string_view value = obj["ev"].get_string();
                        if(value == "Q"){
                            // std::cout << "Quote recieved thread1: " << std::endl;
                            process_quote(obj);
                            continue;
                        }
                        else if(value == "T"){
                            // std::cout << "Trade recieved thread1: " << std::endl;
                            process_trade(obj);
                            continue;
                        }
                        else if(value == "NOI"){
                            // std::cout << "Trade recieved thread1: " << std::endl;
                            //process_NYSE_Imbalance(obj);
                            continue;
                        }
                        else{
                            continue;
                        }
                    }
                }
            }
        }
    }
}

bool MDProcessor::try_pop(std::string& output){
    uint8_t current_idx = data_queue.next_read_index.load(std::memory_order_acquire);
    // Check if data is ready
    if (!data_queue.data[current_idx].is_ready.load(std::memory_order_acquire)) return false;

    // Try to claim this slot atomically
    if (data_queue.next_read_index.compare_exchange_weak(current_idx, current_idx + 1)) {
        output = std::move(data_queue.data[current_idx].data);
        data_queue.data[current_idx].is_ready.store(false, std::memory_order_release);
        return true;
    }

    return false; // Someone else beat us to it
}

bool MDProcessor::try_pop(MDupdate& output){
    uint8_t current_idx = processed_data_queue.next_read_index.load(std::memory_order_acquire);
    // Check if data is ready
    if (!processed_data_queue.data[current_idx].is_ready.load(std::memory_order_acquire)) return false;

    if (processed_data_queue.next_read_index.compare_exchange_weak(current_idx, current_idx + 1)) {
        // std::cout << "Processed Trade price: " << processed_data_queue.data[current_idx].data.m_bid_price << std::endl;
        // std::cout << "Processed Trade quant: " << processed_data_queue.data[current_idx].data.m_bid_quant << std::endl;
        output = processed_data_queue.data[current_idx].data;
        processed_data_queue.data[current_idx].is_ready.store(false, std::memory_order_release);
        return true;
    }

    return false;
}

void MDProcessor::write_to_schmem(){
    MDupdate cur_md;
    
    while(true){
        if(try_pop(cur_md)){
            // std::cout << "Trade price: " << cur_md.m_bid_price << std::endl;
            // std::cout << "Trade quant: " << cur_md.m_bid_quant << std::endl;
            mShmemManager->write_MD(cur_md);
        }
    }
}