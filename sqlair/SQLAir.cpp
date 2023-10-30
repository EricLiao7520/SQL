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
using namespace boost::asio;
using namespace boost::asio::ip;
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

/**
 *
 * An helper method that print rows that has been selected.
 *
 * @param row the row that has been selected.
 *
 * @param cols The content of the row
 *
 * @param csv The CSV data to be used.
 *
 * @param[out] os The output stream to where the number of rows selected must
 * be written -- e.g." "1 row(s) selected.\n"
 */
void printRow(CSVRow& row, const StrVec& cols, const CSV& csv,
              std::ostream& os) {
    CSVRow rowCopy;
    {
        std::unique_lock lock(row.rowMutex);
        rowCopy = row;
    }
    std::string delim = "";
    for (const auto& colName : cols) {
        os << delim << rowCopy.at(csv.getColumnIndex(colName));
        delim = "\t";
    }
    os << std::endl;
}

int SQLAir::selectHelper(CSV& csv, StrVec colNames, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    int rowCount = 0;
    // Convert any "*" to suitable column names.
    if (colNames.size() == 1 && colNames.front() == "*") {
        // With a wildcard column name, we print all of the columns in CSV
        colNames = csv.getColumnNames();
    }
    // Print each row that matches an optional condition.
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        bool isMatch;
        {
            std::unique_lock<std::mutex> lock(row.rowMutex);
            isMatch = (whereColIdx == -1)
                          ? true
                          : matches(row.at(whereColIdx), cond, value);
        }
        if (isMatch) {
            if (rowCount == 0) {
                os << colNames << std::endl;
            }
            printRow(row, colNames, csv, os);
            rowCount++;
        }
    }
    return rowCount;
}

/*#include <functional>

bool fn(const StrVec& vec, std::function<bool(const StrVec&)> lmb) {
    return lmb(vec);
}

void SQLAir::testfunc() {
    int whereCol = 1;
    std::string cond = "like";
    std::string val = "The";
    auto testFunc = [whereCol, cond, val](const StrVec& row) {
        return matches(row[whereCol], cond, val);
    };
    fn(vec, testFunc);
}
*/
// API method to perform operations associated with a "select" statement
// to print columns that match an optional condition.
void SQLAir::selectQuery(CSV& csv, bool mustWait, StrVec colNames,
                         const int whereColIdx, const std::string& cond,
                         const std::string& value, std::ostream& os) {
    // Convert any "*" to suitable column names. See CSV::getColumnNames()
    int rowCount = selectHelper(csv, colNames, whereColIdx, cond, value, os);
    while (rowCount == 0 && mustWait) {
        rowCount = selectHelper(csv, colNames, whereColIdx, cond, value, os);
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        csv.csvCondVar.wait(lock);
    }
    os << rowCount << " row(s) selected." << std::endl;
}

int SQLAir::updateHelper(CSV& csv, StrVec colNames, StrVec values,
                         const int whereColIdx, const std::string& cond,
                         const std::string& value, std::ostream& os) {
    if (std::find(colNames.begin(), colNames.end(), "*") != colNames.end()) {
        colNames = csv.getColumnNames();
    }
    int rowCount = 0;
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        std::unique_lock<std::mutex> lock(row.rowMutex);
        if (whereColIdx == -1 || matches(row.at(whereColIdx), cond, value)) {
            for (size_t i = 0; i < colNames.size(); i++) {
                auto colIdx = csv.getColumnIndex(colNames[i]);
                row.at(colIdx) = values[i];
            }
            rowCount++;
        }
    }
    if (rowCount > 0) {
        csv.csvCondVar.notify_all();
    }
    return rowCount;
}

void SQLAir::updateQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    int rowCount =
        updateHelper(csv, colNames, values, whereColIdx, cond, value, os);
    // Update each row that matches an optional condition.
    // throw Exp("update is not yet implemented.");
    while (rowCount == 0 && mustWait) {
        rowCount =
            updateHelper(csv, colNames, values, whereColIdx, cond, value, os);
        std::unique_lock lock(csv.csvMutex);
        csv.csvCondVar.wait(lock);
    }
    os << rowCount << " row(s) updated." << std::endl;
}

void SQLAir::insertQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, std::ostream& os) {
    CSVRow row;
    for (int i = 0; i < csv.getColumnCount(); i++) {
        row.push_back("");  // Add an empty value for each column
    }
    for (size_t i = 0; i < colNames.size(); i++) {
        auto colIdx = csv.getColumnIndex(colNames[i]);
        row.at(colIdx) = values[i];
    }
    csv.push_back(row);
    os << "1 row inserted." << std::endl;
}
void SQLAir::deleteQuery(CSV& csv, bool mustWait, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    int counts = 0;
    CSV newCSV;
    for (auto& row : csv) {
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        if (whereColIdx == -1 || !matches(row.at(whereColIdx), cond, value)) {
            newCSV.push_back(row);
            counts++;
        }
    }
    csv.swap(newCSV);
    os << counts << " row(s) Deleted." << std::endl;
}

void SQLAir::clientThread(TcpStreamPtr client) {
    // Extract the SQL query from the first line for processing
    std::string req;
    *client >> req >> req;
    // Skip over all the HTTP request headers. Without this loop the
    // web-server will not operate correctly with all the web-browsers
    for (std::string hdr;
         (std::getline(*client, hdr) && !hdr.empty() && hdr != "\r");) {
    }
    // URL-decode the request to translate special/encoded characters
    req = Helper::url_decode(req);
    // Check and do the necessary processing based on type of request
    const std::string prefix = "/sql-air?query=";
    if (req.find(prefix) != 0) {
        // This is request for a data file. So send the data file out.
        *client << http::file("./" + req);
    } else {
        // This is a sql-air query. Let's have the helper method do the
        // processing for us
        std::ostringstream os;
        try {
            std::string sql = Helper::trim(req.substr(prefix.size()));
            if (sql.back() == ';') {
                sql.pop_back();  // Remove trailing semicolon.
            }
            process(sql, os);
        } catch (const std::exception& exp) {
            os << "Error: " << exp.what() << std::endl;
        }
        // Send HTTP response back to the client.
        const std::string resp = os.str();
        // Send response back to the client.
        *client << HTTPRespHeader << resp.size() << "\r\n\r\n" << resp;
    }
    numThreads.fetch_sub(1, std::memory_order_relaxed);
    thrCond.notify_one();
}
// The method to have this class run as a web-server.
void SQLAir::runServer(boost::asio::ip::tcp::acceptor& server,
                       const int maxThr) {
    for (bool done = false; !done;) {
        // Creates garbage-collected connection on heap
        TcpStreamPtr client = std::make_shared<tcp::iostream>();
        // Wait for a client to connect
        server.accept(*client->rdbuf());
        // Now we have a I/O stream to talk to the client.
        {
            std::unique_lock lock(thrMutex);
            thrCond.wait(lock,
                         [maxThr, this] { return this->numThreads < maxThr; });
            numThreads.fetch_add(1, std::memory_order_relaxed);
        }
        std::thread thr(&SQLAir::clientThread, this, client);
        thr.detach();  // Run independently
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
