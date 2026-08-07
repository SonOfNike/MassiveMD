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

void MDProcessor::process_quote(const simdjson::dom::object& _obj, const int& index){

    uint8_t current_idx = processed_data_queues[index].next_write_index.load(std::memory_order_relaxed);

    while(true){
        // Check if data is ready
        
        if (processed_data_queues[index].data[current_idx].is_ready.load(std::memory_order_acquire)){
            continue;
        }

        std::string_view current_sym;
        double current_bid = 0;
        double current_ask = 0;
        Price current_bid_size = 0;
        Price current_ask_size = 0;
        Timestamp current_time = 0;

        auto error = _obj["sym"].get(current_sym)
                    | _obj["bp"].get(current_bid)
                    | _obj["bs"].get(current_bid_size)
                    | _obj["ap"].get(current_ask)
                    | _obj["as"].get(current_ask_size)
                    | _obj["t"].get(current_time);

        if(error)
            break;

        processed_data_queues[index].data[current_idx].data.m_type = md_type::QUOTE;
        processed_data_queues[index].data[current_idx].data.m_symbolId = mSymIDManager->getID(current_sym);
        processed_data_queues[index].data[current_idx].data.m_bid_price = roundToNearestCent(Price(current_bid * DOLLAR));
        processed_data_queues[index].data[current_idx].data.m_ask_price = roundToNearestCent(Price(current_ask * DOLLAR));
        processed_data_queues[index].data[current_idx].data.m_bid_quant = Shares(current_bid_size * 100);
        processed_data_queues[index].data[current_idx].data.m_ask_quant = Shares(current_ask_size * 100);

        //Timestamp conversion
        processed_data_queues[index].data[current_idx].data.m_timestamp = current_time * MILLI_SECONDS;

        // processed_data_queues[index].data[current_idx].data.m_type = md_type::QUOTE;
        // processed_data_queues[index].data[current_idx].data.m_symbolId = mSymIDManager->getID(_obj["sym"].get_string());
        // processed_data_queues[index].data[current_idx].data.m_bid_price = roundToNearestCent(Price(_obj["bp"].get_double() * DOLLAR));
        // processed_data_queues[index].data[current_idx].data.m_ask_price = roundToNearestCent(Price(_obj["ap"].get_double() * DOLLAR));
        // processed_data_queues[index].data[current_idx].data.m_bid_quant = Shares(_obj["bs"].get_int64() * 100);
        // processed_data_queues[index].data[current_idx].data.m_ask_quant = Shares(_obj["as"].get_int64() * 100);

        // //Timestamp conversion
        // processed_data_queues[index].data[current_idx].data.m_timestamp = _obj["t"].get_int64() * MILLI_SECONDS;

        processed_data_queues[index].data[current_idx].is_ready.store(true, std::memory_order_release);

        processed_data_queues[index].next_write_index.store(current_idx + 1, std::memory_order_release);
        break;
    }
}
    
void MDProcessor::process_trade(const simdjson::dom::object& _obj, const int& index){
    uint8_t current_idx = processed_data_queues[index].next_write_index.load(std::memory_order_relaxed);

    while(true){
        // Check if data is ready
        if (processed_data_queues[index].data[current_idx].is_ready.load(std::memory_order_acquire)){
            continue;
        }

        processed_data_queues[index].data[current_idx].data.m_type = md_type::NONE;
        simdjson::dom::array conditions;
        // int64_t exchange = Shares(_obj["x"].get_int64());
        int64_t exchange = 0;
        // processed_data_queues[index].data[current_idx].data.m_ask_quant = exchange;
        auto error = _obj.at_key("c").get(conditions)
                    | _obj["x"].get(exchange);
        if (!error) {
            processed_data_queues[index].data[current_idx].data.m_ask_quant = exchange;
            for(simdjson::dom::element val : conditions){
                int64_t code = val.get_int64();
                if(code == 16){
                    if(exchange == 10){
                        processed_data_queues[index].data[current_idx].data.m_type = md_type::NYSEOPEN;
                        processed_data_queues[index].data[current_idx].data.m_ask_price = code;
                    }
                    else if(exchange == 12){
                        processed_data_queues[index].data[current_idx].data.m_type = md_type::NASDOPEN;
                        processed_data_queues[index].data[current_idx].data.m_ask_price = code;
                    }
                }
            }
            if(processed_data_queues[index].data[current_idx].data.m_type == md_type::NONE)
                processed_data_queues[index].data[current_idx].data.m_type = md_type::PRINT;
        }
        else
            processed_data_queues[index].data[current_idx].data.m_type = md_type::PRINT;

        std::string_view current_sym;
        double current_price = 0;
        Price current_size = 0;
        Timestamp current_time = 0;

        auto error2 = _obj["sym"].get(current_sym)
                    | _obj["p"].get(current_price)
                    | _obj["s"].get(current_size)
                    | _obj["t"].get(current_time);

        if(error2)
            break;

        processed_data_queues[index].data[current_idx].data.m_symbolId = mSymIDManager->getID(current_sym);
        processed_data_queues[index].data[current_idx].data.m_bid_price = roundToNearestCent(Price(current_price * DOLLAR));
        processed_data_queues[index].data[current_idx].data.m_bid_quant = Shares(current_size);

        //Timestamp conversion
        processed_data_queues[index].data[current_idx].data.m_timestamp = current_time * MILLI_SECONDS;

        // processed_data_queues[index].data[current_idx].data.m_symbolId = mSymIDManager->getID(_obj["sym"].get_string());
        // processed_data_queues[index].data[current_idx].data.m_bid_price = roundToNearestCent(Price(_obj["p"].get_double() * DOLLAR));
        // processed_data_queues[index].data[current_idx].data.m_bid_quant = Shares(_obj["s"].get_int64());

        // //Timestamp conversion
        // processed_data_queues[index].data[current_idx].data.m_timestamp = _obj["t"].get_int64() * MILLI_SECONDS;
        
        processed_data_queues[index].data[current_idx].is_ready.store(true, std::memory_order_release);
        processed_data_queues[index].next_write_index.store(current_idx + 1, std::memory_order_release);
        break;
    }
}

void MDProcessor::process_imbalance(const simdjson::dom::object& _obj, const int& index){
    uint8_t current_idx = processed_data_queues[index].next_write_index.load(std::memory_order_relaxed);

    while(true){
        // Check if data is ready
        if (processed_data_queues[index].data[current_idx].is_ready.load(std::memory_order_acquire)){
            continue;
        }

        std::string_view current_sym;
        std::string_view auction_type;
        double clear_price = 0;
        Price exchange_id = 0;
        Price imbalance_size = 0;
        Price paired_size = 0;
        Timestamp current_time = 0;

        auto error = _obj["T"].get(current_sym)
                    | _obj["t"].get(current_time)
                    | _obj["a"].get(auction_type)
                    | _obj["x"].get(exchange_id)
                    | _obj["o"].get(imbalance_size)
                    | _obj["p"].get(paired_size)
                    | _obj["b"].get(clear_price);

        if(error)
            break;

        if(auction_type == "P" || auction_type == "R")
            processed_data_queues[index].data[current_idx].data.m_type = md_type::SIGIMB;
        else
            processed_data_queues[index].data[current_idx].data.m_type = md_type::IMBALANCE;
        processed_data_queues[index].data[current_idx].data.m_symbolId = mSymIDManager->getID(current_sym);
        
        processed_data_queues[index].data[current_idx].data.m_bid_price = roundToNearestCent(Price(clear_price * DOLLAR));
        
        processed_data_queues[index].data[current_idx].data.m_bid_quant = Shares(imbalance_size);
        processed_data_queues[index].data[current_idx].data.m_ask_quant = Shares(paired_size);
        processed_data_queues[index].data[current_idx].data.m_ask_price = Shares(exchange_id);

        //Timestamp conversion
        processed_data_queues[index].data[current_idx].data.m_timestamp = current_time;

        // std::string_view auction_type = _obj["a"].get_string();
        // if(auction_type == "P" || auction_type == "R")
        //     processed_data_queues[index].data[current_idx].data.m_type = md_type::SIGIMB;
        // else
        //     processed_data_queues[index].data[current_idx].data.m_type = md_type::IMBALANCE;
        // processed_data_queues[index].data[current_idx].data.m_symbolId = mSymIDManager->getID(_obj["T"].get_string());
        
        // processed_data_queues[index].data[current_idx].data.m_bid_price = roundToNearestCent(Price(_obj["b"].get_double() * DOLLAR));
        
        // processed_data_queues[index].data[current_idx].data.m_bid_quant = Shares(_obj["o"].get_int64());
        // processed_data_queues[index].data[current_idx].data.m_ask_quant = Shares(_obj["p"].get_int64());
        // processed_data_queues[index].data[current_idx].data.m_ask_price = _obj["x"].get_int64();

        // //Timestamp conversion
        // processed_data_queues[index].data[current_idx].data.m_timestamp = _obj["t"].get_int64();

        // std::cout << "Trade price: " << processed_data_queue.data[current_idx].data.m_bid_price << std::endl;
        // std::cout << "Trade quant: " << processed_data_queue.data[current_idx].data.m_bid_quant << std::endl;
        processed_data_queues[index].data[current_idx].is_ready.store(true, std::memory_order_release);
        processed_data_queues[index].next_write_index.store(current_idx + 1, std::memory_order_release);
        break;
    }
}

void MDProcessor::push_raw_data(const std::string& raw_json){
    auto& target_queue = data_queues[current_raw_queue % 3];
    // auto& target_queue = data_queues[0];
    uint8_t write_idx = target_queue.next_write_index.load(std::memory_order_relaxed);;

    while(target_queue.data[write_idx].is_ready.load(std::memory_order_acquire));

    // 3. Copy data into our pre-allocated padded buffer
    // This is a "Zero-Allocation" move after the initial socket read
    size_t copy_len = std::min(raw_json.size(), MAX_SIZE);
    std::memcpy(target_queue.data[write_idx].buffer, raw_json.data(), copy_len);
    target_queue.data[write_idx].len = copy_len;
    
    // 4. Mark as ready for the Parser thread
    target_queue.data[write_idx].is_ready.store(true, std::memory_order_release);
    
    // 5. Update index for next push
    target_queue.next_write_index.store(write_idx + 1, std::memory_order_release);

    current_raw_queue++;
}

void MDProcessor::process_raw_data(const int& index){
    // Pin this thread to a core (Core 2, 3, or 4 based on index)
    pin_current_thread(index + TRADE_WTHREADS);

    simdjson::dom::parser parser;

    while(true){
        RawBlock* block = try_pop(index);

        if(block){
            auto result = parser.parse(block->buffer, block->len);

            if(!result.error()){
                for(simdjson::dom::object obj : result){
                    simdjson::dom::element ev_field;
                    if (obj["ev"].get(ev_field) == simdjson::SUCCESS){

                        std::string_view type = ev_field.get_string();

                        switch(type[0]){
                            case 'Q': 
                                process_quote(obj, index);
                                continue;
                            case 'T': 
                                process_trade(obj, index);
                                continue;
                            case 'N': 
                                process_imbalance(obj, index);
                                continue;
                        }
                    }
                }
            }

            release_slot(index);

        }
    }
}

RawBlock* MDProcessor::try_pop(const int& index){
    auto& queue = data_queues[index];
    uint8_t read_idx = queue.next_read_index.load(std::memory_order_relaxed);

    // Check if data is ready
    if (!queue.data[read_idx].is_ready.load(std::memory_order_acquire)) {
        return nullptr;
    }
    // Return the address of the data directly
    return &queue.data[read_idx];
}

void MDProcessor::release_slot(const int& index){
    auto& queue = data_queues[index];
    uint8_t read_idx = queue.next_read_index.load(std::memory_order_relaxed);
    
    queue.data[read_idx].is_ready.store(false, std::memory_order_release);
    queue.next_read_index.store(read_idx + 1, std::memory_order_release);
}

bool MDProcessor::try_pop(MDupdate& output, const int& index){
    // Check if data is ready
    if (!processed_data_queues[index].data[processed_data_queues[index].next_read_index].is_ready.load(std::memory_order_acquire)) return false;

        // std::cout << "Processed Trade price: " << processed_data_queue.data[current_idx].data.m_bid_price << std::endl;
        // std::cout << "Processed Trade quant: " << processed_data_queue.data[current_idx].data.m_bid_quant << std::endl;
    output = processed_data_queues[index].data[processed_data_queues[index].next_read_index].data;
    processed_data_queues[index].data[processed_data_queues[index].next_read_index].is_ready.store(false, std::memory_order_release);
    processed_data_queues[index].next_read_index++;
    return true;
}

void MDProcessor::write_to_schmem(){
    pin_current_thread(TRADE_WTHREADS + 4);

    MDupdate cur_md;
    
    while(true){
        if(try_pop(cur_md, 0)){
            // std::cout << "Trade price: " << cur_md.m_bid_price << std::endl;
            // std::cout << "Trade quant: " << cur_md.m_bid_quant << std::endl;
            mShmemManager->write_MD(cur_md);
        }
        if(try_pop(cur_md, 1)){
            // std::cout << "Trade price: " << cur_md.m_bid_price << std::endl;
            // std::cout << "Trade quant: " << cur_md.m_bid_quant << std::endl;
            mShmemManager->write_MD(cur_md);
        }
        if(try_pop(cur_md, 2)){
            // std::cout << "Trade price: " << cur_md.m_bid_price << std::endl;
            // std::cout << "Trade quant: " << cur_md.m_bid_quant << std::endl;
            mShmemManager->write_MD(cur_md);
        }
    }
}