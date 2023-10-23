// Copyright 2023
/*
 * A very lightweight (light as air) implementation of a simple CSV-based
 * database system that uses SQL-like syntax for querying and updating the
 * CSV files.
 *
 */

#include "SQLAir.h"

#include <algorithm>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>

#include "HTTPFile.h"
/**
 * A fixed HTTP response header that is used by the runServer method below.
 * Note that this a constant (and not a global variable)
 */
const std::string HTTPRespHeader =
    "HTTP/1.1 200 OK\r\n"
    "Server: localhost\r\n"
    "Connection: Close\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: ";

// API method to perform operations associated with a "select" statement
// to print columns that match an optional condition.
void SQLAir::selectQuery(CSV& csv, bool mustWait, StrVec colNames,
                         const int whereColIdx, const std::string& cond,
                         const std::string& value, std::ostream& os) {
    // Convert any "*" to suitable column names. See CSV::getColumnNames()
    if (std::find(colNames.begin(), colNames.end(), "*") != colNames.end()) {
        colNames = csv.getColumnNames();
    }
    // First print the column names.
    int counts = 0;
    bool printColNames = false;
    // Print each row that matches an optional condition.
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
         std::unique_lock<std::mutex> lock(row.rowMutex);
        if (whereColIdx == -1 || matches(row.at(whereColIdx), cond, value)) {
            if (!printColNames) {
                os << colNames << std::endl;
                printColNames = true;
            }
            std::string delim = "";
            counts++;
            for (const auto& colName : colNames) {
                os << delim << row.at(csv.getColumnIndex(colName));
                delim = "\t";
            }
            os << std::endl;
        }
    }
    os << counts << " row(s) selected." << std::endl;
}

void SQLAir::updateQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    if (std::find(colNames.begin(), colNames.end(), "*") != colNames.end()) {
        colNames = csv.getColumnNames();
    }
    int counts = 0;
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        if (whereColIdx == -1 || matches(row.at(whereColIdx), cond, value)) {
            for (size_t i = 0; i < colNames.size(); i++) {
                auto colIdx = csv.getColumnIndex(colNames[i]);
                row.at(colIdx) = values[i];
            }
            counts++;
        }
    }
    os << counts << " row(s) updated." << std::endl;
    // Update each row that matches an optional condition.
    // throw Exp("update is not yet implemented.");
}

void SQLAir::insertQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, std::ostream& os) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
       auto& row = CSVRow();
            for (size_t i = 0; i < colNames.size(); i++) {
                auto colIdx = csv.getColumnIndex(colNames[i]);
                row.at(colIdx) = values[i];
    }
    os << "1 row inserted." << std::endl;
}

void SQLAir::deleteQuery(CSV& csv, bool mustWait, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    if (std::find(colNames.begin(), colNames.end(), "*") != colNames.end()) {
        colNames = csv.getColumnNames();
    }
    int counts = 0;
    int colCount = csv.getColumnCount();
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        if (whereColIdx == -1 || matches(row.at(whereColIdx), cond, value)) {
            for(int i = 0;i < colCount;i++){
                row.at(i) = null;
            }
        }
            counts++;
        }
        os << counts << " row(s) Deleted." << std::endl;
}

void SQLAir::serveClient(std::istream& is, std::ostream& os) {
    std::string line, path, str;
    std::ostringstream strStream;
    is >> line >> path;
    while (std::getline(is, line) && line != "\r") {
    }
    if (path.find("/sql-air") != std::string::npos) {
        str = path.substr(15);
        str = Helper::url_decode(str);
        try {
            SQLAirBase::process(str, strStream);
            std::string outStr = strStream.str();
            os << HTTPRespHeader << outStr.size() << "\r\n\r\n" << outStr;
        } catch (const std::exception& exp) {
            std::string str2 = "Error: ";
            str2 += exp.what();
            os << HTTPRespHeader << str2.size() + 1 << "\r\n\r\n" << str2;
        }
    } else if (!path.empty()) {
        os << http::file(path);
    }
}
// The method to have this class run as a web-server.
void SQLAir::runServer(boost::asio::ip::tcp::acceptor& server,
                       const int maxThr) {
    while (true) {
        // Creates garbage-collected connection on heap
        TcpStreamPtr client =
            std::make_shared<boost::asio::ip::tcp::iostream>();
        server.accept(*client->rdbuf());  // wait for client to connect
        // Now we have a I/O stream to talk to the client. Have a
        // conversation using the protocol.
        std::thread bgThr([this, client] { serveClient(*client, *client); });
        bgThr.detach();  // Process transaction independently
    }
}
void setupDownload(const std::string& hostName, const std::string& path,
                   boost::asio::ip::tcp::iostream& data,
                   const std::string& port = "80") {
    // Create a boost socket and request the log file from the server.
    data.connect(hostName, port);
    data << "GET " << path << " HTTP/1.1\r\n"
         << "Host: " << hostName << "\r\n"
         << "Connection: Close\r\n\r\n";
}
void checkQuery(boost::asio::ip::tcp::iostream& data, const std::string& host,
                const std::string& path, const std::string& port) {
    if (!data.good()) {
        throw Exp("Unable to connect to " + host + " at port " + port);
    }
    std::string line;
    std::getline(data, line);
    if (line.find("200 OK") == std::string::npos) {
        throw Exp("Error (" + Helper::trim(line) + ") getting " + path +
                  " from " + host + " at port " + port);
    }
    for (std::string hdr;
         std::getline(data, hdr) && !hdr.empty() && hdr != "\r";) {
    }
}
/** Convenience method to decode HTML/URL encoded strings.
 *
 * This method must be used to decode query string parameters supplied
 * along with GET request.  This method converts URL encoded entities
 * in the from %nn (where 'n' is a hexadecimal digit) to corresponding
 * ASCII characters.
 *
 * \param[in] str The string to be decoded.  If the string does not
 * have any URL encoded characters then this original string is
 * returned.  So it is always safe to call this method!
 *
 * \return The decoded string.
 */
CSV& SQLAir::loadAndGet(std::string fileOrURL) {
    // Check if the specified fileOrURL is already loaded in a thread-safe
    // manner to avoid race conditions on the unordered_map
    {
        std::scoped_lock<std::mutex> guard(recentCSVMutex);
        // Use recent CSV if parameter was empty string.
        fileOrURL = (fileOrURL.empty() ? recentCSV : fileOrURL);
        // Update the most recently used CSV for the next round
        recentCSV = fileOrURL;
        if (inMemoryCSV.find(fileOrURL) != inMemoryCSV.end()) {
            // Requested CSV is already in memory. Just return it.
            return inMemoryCSV.at(fileOrURL);
        }
    }
    // When control drops here, we need to load the CSV into memory.
    // Loading or I/O is being done outside critical sections
    CSV csv;  // Load data into this csv
    if (fileOrURL.find("http://") == 0) {
        // This is an URL. We have to get the stream from a web-server
        // Implement this feature.
        std::string host, port, path;
        std::tie(host, port, path) = Helper::breakDownURL(fileOrURL);
        boost::asio::ip::tcp::iostream is;
        setupDownload(host, path, is);
        checkQuery(is, host, path, port);
        csv.load(is);
        // throw Exp("Loading CSV from ULR not implemented.");
    } else {
        // We assume it is a local file on the server. Load that file.
        std::ifstream data(fileOrURL);
        // This method may throw exceptions on errors.
        csv.load(data);
    }
    // We get to this line of code only if the above if-else to load the
    // CSV did not throw any exceptions. In this case we have a valid CSV
    // to add to our inMemoryCSV list. We need to do that in a thread-safe
    // manner.
    std::scoped_lock<std::mutex> guard(recentCSVMutex);
    // Move (instead of copy) the CSV data into our in-memory CSVs
    inMemoryCSV[fileOrURL].move(csv);
    // Return a reference to the in-memory CSV (not temporary one)
    return inMemoryCSV.at(fileOrURL);
}
// Save the currently loaded CSV file to a local file.
void SQLAir::saveQuery(std::ostream& os) {
    if (recentCSV.empty() || recentCSV.find("http://") == 0) {
        throw Exp("Saving CSV to an URL using POST is not implemented");
    }
    // Create a local file and have the CSV write itself.
    std::ofstream csvData(recentCSV);
    inMemoryCSV.at(recentCSV).save(csvData);
    os << recentCSV << " saved.\n";
}
