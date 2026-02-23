cmake_minimum_required(VERSION 3.10.0)
project(MassiveMD VERSION 0.1.0 LANGUAGES C CXX)

find_package(OpenSSL REQUIRED)
find_package(glog REQUIRED PATHS ../glog/build/)

# Path to the websocketpp library
set(WEBSOCKETPP_PATH /home/git_repos/websocketpp)

add_executable(MassiveMD main.cpp
                         MDConnector.cpp
                         ShmemManager.cpp
                         MDProcessor.cpp
                         ../Utils/SymbolIDManager.cpp
                         ../Utils/simdjson/simdjson.cpp)

target_link_libraries(MassiveMD PRIVATE OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(MassiveMD PRIVATE glog::glog)
target_include_directories(MassiveMD PUBLIC
        ${WEBSOCKETPP_PATH}
    )

include(CTest)
enable_testing()

set_property(TARGET MassiveMD PROPERTY CXX_STANDARD 17)