#include "MDConnector.h"
#include "../Utils/pin_thread_functions.h"

int main(int argc, char* argv[]) {
    MDConnector* con = MDConnector::getInstance();
    con->on_init();

    std::thread t1(&MDProcessor::write_to_schmem, MDProcessor::getInstance());
    std::thread t2(&MDProcessor::process_raw_data, MDProcessor::getInstance(), 0);
    std::thread t3(&MDProcessor::process_raw_data, MDProcessor::getInstance(), 1);
    std::thread t4(&MDProcessor::process_raw_data, MDProcessor::getInstance(), 2);

    pin_current_thread(TRADE_WTHREADS + 3);

    con->connect();
}