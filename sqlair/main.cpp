/**
 * A simple test class to test the operation of SQLAir class and its
 * methods.
 *
 * Copyright (C) 2021 raodm@miamioh.edu
 */

//------------------------------------------------------------------
//  DO  NOT  MODIFY  CODE  IN THIS  FILE
//------------------------------------------------------------------

#include <boost/asio.hpp>
#include <iostream>
#include <string>
#include "SQLAir.h"
#include "Helper.h"

//------------------------------------------------------------------
//  DO  NOT  MODIFY  CODE  IN THIS  FILE
//------------------------------------------------------------------

/**
 * Runs the program as a server that listens to incoming connections.
 * 
 * @param port The port number on which the server should listen.
 */
void runServer(SQLAir& air, int port, int maxThr) {
    // Convenience namespace to streamline the code below.
    using namespace boost::asio;
    using namespace boost::asio::ip;

    // Setup a server socket to accept connections on the socket
    io_service service;
    // Create end point
    tcp::endpoint myEndpoint(tcp::v4(), port);
    // Create a socket that accepts connections
    tcp::acceptor server(service, myEndpoint);
    std::cout << "SQL-Air server is listening on "
              << server.local_endpoint().port()
              << " & ready to process clients...\n";
    // Process client connections one-by-one..until user enters "exit"
    air.runServer(server, maxThr);
}

//------------------------------------------------------------------
//  DO  NOT  MODIFY  CODE  IN THIS  FILE
//------------------------------------------------------------------

// Helper method for testing.
void checkRunClient(const int port, const bool printResp = false);

/**
 * The main function that serves as a test harness based on
 * command-line arguments.
 *
 * \param[in] argc The number of command-line arguments.  This test
 * harness can work with zero or one command-line argument.
 *
 * \param[in] argv The actual command-line arguments.  If this is an
 * number it is assumed to be a port number.  Otherwise it is assumed
 * to be an file name that contains inputs for testing.
 */
int main(int argc, char *argv[]) {
    // Check and use first command-line argument, if any as port or input file.
    const std::string port = (argc > 1 ? argv[1] : "-");
    // Setup the maximum number of threads to be used.
    const int maxThr = (argc > 2 ? std::stoi(argv[2]) : 20);

    // Our SQLAir object for further use.
    SQLAir air;
    
    // Check and use a given input data file for testing.
    if (port.find_first_not_of("1234567890") == std::string::npos) {
        // All characters are digits. So we assume this is a port
        // number and run as a standard web-server
        const int portNum = std::stoi(port);
#ifdef TEST_CLIENT
        checkRunClient(portNum);
#endif
        runServer(air, portNum, maxThr);
    } else {
        // In this situation, this program processes inputs from the
        // console, repeatedly until the user types "exit;"
       std::cout << "Welcome to SQL-AIR. It doesn't get any lite'r!\n\n";
       // Repeatedly read inputs from the user for processing, until the
       // process method returns false.
       for (std::string query; std::cout << "sql-air> " && 
            std::getline(std::cin, query, ';'); ) {
            // Process the lines of SQL from the user
            try {
                if (!air.process(query, std::cout)) {
                    break;   // End query processing due to "exit;" command.
                }
            } catch (const std::exception &exp) {
                std::cout << exp.what() << std::endl;
            }
       }
       std::cout << "Floating away. Bye!\n";
    }
    // All done.
    return 0;
}
