#ifndef SQL_AIR_H
#define SQL_AIR_H

/*
 * A very lightweight (light as air) implementation of a simple CSV-based
 * database system that uses SQL-like syntax for querying and updating the
 * CSV files.
 *
 * Copyright (C) 2021 raodm@miamioh.edu
 */

#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>

#include "SQLAirBase.h"

// Shortcut to smart pointer with TcpStream
using TcpStreamPtr = std::shared_ptr<boost::asio::ip::tcp::iostream>;

/**
 * The top-level class that facilitates processing SQL-like queries on CSV
 * files. The methods in this class override the default/dummy implementations
 * in the base class to perform the actual CRUD operations.
 */
class SQLAir : public SQLAirBase {
   public:
    /**
     * Method to perform the actual operations associated with printing a
     * given set of columns in a given CSV that match an optional condition.
     * This method is called with a validated (the validation is not foolproof)
     * set of parameters. This method must print the column names (separated
     * by a tab character) and the selected data (also separated by a tab).
     * The method must print the total number of rows selected towards the end.
     *
     * @note For checking conditions, use the matches() method in the base
     * class.
     *
     * @note This method will be called from multiple threads. Hence, this
     * method must take care to ensure its operations are MT-Safe
     * (multi-threading safe).
     *
     * @param csv The CSV data to be used.
     *
     * @param mustWait If this flag is true, then this query must keep trying
     * until at least one row is selected.
     *
     * @param colNames The column names in the CSV file to be printed by this
     * method. The colNames will be just {"*"} or valid column names in the CSV.
     *
     * @param whereColIdx An optional column specified in a 'where' clause in
     * the query for checking. If a 'where' clause was not specified, then this
     * parameter will be -1. For example, if "where name like 'Test'" is
     * specified, then this parameter will have the index of the 'name' column
     * in the CSV.
     *
     * @param cond The condition to be applied. The condition will always be
     * one of "=" (equal-to), "<>" (not equal to), or "like" (substring). For
     * example, if "where name like 'Test'" is specified, then this parameter
     * will be "like" (without quotes).
     *
     * @param value The value to be used for comparison. This is parameter
     * has the value specified by the user in a 'where' clause. For
     * example, if "where name like 'Test'" is specified, then this parameter
     * will be "Test" (without quotes).
     *
     * @param os The output stream to where the results are to be written.
     */
    void selectQuery(CSV& csv, bool mustWait, StrVec colNames,
                     const int whereColIdx, const std::string& cond,
                     const std::string& value, std::ostream& os) override;

    /**
     * Method that is called to perform actual operations to update specified
     * values in the CSV. This method's documentation uses the following query
     * as an example:
     *
     *     update test.csv set rating=2.5, raters=2 where movieid = 12345;
     *
     * @note This method will be called from multiple threads. Hence, this
     * method must take care to ensure its operations are MT-Safe
     * (multi-threading safe).
     *
     * @param csv The CSV whose values are to be updated. Given the above query,
     * the CSV will correspond to the data for "test.csv" (loaded into memory
     * via call to the loadAndGet() method).
     *
     * @param mustWait If this flag is true then this method must repeatedly
     * try performing the update operation until at least 1 row is updated.
     *
     * @param colNames The names of the columns to be updated in each row.
     * This method may assume the colNames are valid (as they are validated
     * in the the update method). Given the above example query, this vector
     * will contain {"rating", "raters"}.
     *
     * @param values The values to be set for each column. This method may
     * assume the values and colNames match (as it is validated in the update
     * method).  Given the above example query, this vector will contain
     * {"2.5", "2"}.
     *
     * @param whereColIdx The integer value corresponding to the column in the
     * the where clause if any. If a where clause is not present then this
     * value is -1. In the above example query, is value will contain the index
     * of the movieid column in test.csv.
     *
     * @param cond The condition to be used. If a value is not specified it
     * is an empty string. Given the above example query, this parameter will
     * contain the value "=" (without quotes).
     *
     * @param value The value to be compared against. Given the above query,
     * this parameter will contain the value "12345" (without quotes)
     *
     * @param os The output stream to where the number of rows updated must
     * be written -- e.g." "1 row(s) updated.\n"
     */
    void updateQuery(CSV& csv, bool mustWait, StrVec colNames, StrVec values,
                     const int whereColIdx, const std::string& cond,
                     const std::string& value, std::ostream& os) override;

    /**
     * Helper method to perform the actual operations associated with inserting
     * a new row into a given CSV. This method's documentation uses the
     * following query as an example:
     *
     *    insert into test.csv (name, rating, raters) values ('title', 4.5, 10);
     *
     * @note This method will be called from multiple threads. Hence, this
     * method must take care to ensure its operations are MT-Safe
     * (multi-threading safe).
     *
     * @param csv The CSV whose values are to be updated. Given the above query,
     * the CSV will correspond to the data for "test.csv" (loaded into memory
     * via call to the loadAndGet() method).
     *
     * @param mustWait This flag is not used by this method. It is present
     * for potential future functionality.
     *
     * @param colNames The names of the columns to be set in the newly added
     * row. If column names are not specified in the query, then this list
     * will be empty. This method may assume the colNames are valid (as they
     * are validated). Given the above example query, this vector
     * will contain {"title", "rating", "raters"}.
     *
     * @param values The values to be set for each column. This method may
     * assume the values and colNames match.  Given the above example query,
     * this vector will contain {"title", "2.5", "2"}. Columns whose values
     * are not specified are assumed to be empty strings.
     *
     * @param os The output stream to where the number of rows updated must
     * be written -- e.g." "1 row inserted.\n"
     */
    void insertQuery(CSV& csv, bool mustWait, StrVec colNames, StrVec values,
                     std::ostream& os) override;

    /**
     *
     * Helper method to perform the actual operations associated with inserting
     * a new row into a given CSV. This method's documentation uses the
     * following query as an example:
     *
     *    delete from test.csv where year = 2006;
     *
     * @note This method will be called from multiple threads. Hence, this
     * method must take care to ensure its operations are MT-Safe
     * (multi-threading safe).
     *
     * @param csv The CSV whose values are to be updated. Given the above query,
     * the CSV will correspond to the data for "test.csv" (loaded into memory
     * via call to the loadAndGet() method).
     *
     * @param mustWait If this flag is true then this method must repeatedly
     * try performing the delete operation until at least 1 row is deleted.
     *
     * @param whereColIdx The integer value corresponding to the column in the
     * the where clause if any. If a where clause is not present then this
     * value is -1. In the above example query, is value will contain the index
     * of the "year" column in test.csv.
     *
     * @param cond The condition to be used. If a value is not specified it
     * is an empty string. Given the above example query, this parameter will
     * contain the value "=" (without quotes)
     *
     * @param value The value to be compared against. Given the above query,
     * this parameter will contain the value "2006" (without quotes).
     *
     * @param os The output stream to where the number of rows updated must
     * be written -- e.g." "1 row(s) deleted.\n"
     */
    void deleteQuery(CSV& csv, bool mustWait, const int whereColIdx,
                     const std::string& cond, const std::string& value,
                     std::ostream& os) override;

    /**
     * Saves the recently used CSV using the name specified in the recentCSV
     * string. If the recent CSV was downloaded from an URL, then this method
     * throws an exception (as this feature is not yet implemented). If the CSV
     * was loaded from a a file, then the data in the file is overwritten.
     *
     * @param os The output stream to where the result of saving (if any) is
     * to be written.
     *
     * @exception This method may throw exceptions upon errors.
     */
    void saveQuery(std::ostream& os) override;

    /**
     * Helper method to obtain a reference to a pre-loaded CSV file from the
     * inMemoryCSV map.  If the requested file is not present, then this
     * method loads the data into the inMemoryCSV.
     *
     * @note Currently, this method is not MT-safe. So avoid calling this
     * method from multiple threads simultaneously.
     *
     * @param fileOrURL Path to a CSV file or a URL to a CSV data to be returned
     * by this method.  If the path is empty string, then this method returns
     * the most recently accessed CSV file. This method intentionally uses
     * pass-by-value.
     *
     * @return A reference to the in-memory CSV file.  If the CSV data could
     * not be loaded, then this method throws an exception.
     *
     * @exception This method throws an exception if the file could not
     * loaded.
     */
    CSV& loadAndGet(std::string fileOrURL) override;

    /**
     * Method to have this class run as a web-server that runs forever and
     * keeps processing requests. This method does not do the core processing.
     * Instead, for each connection it starts a detached-thread to process the
     * request from the client. Hence, the task of processing HTTP-GET request
     * is delegated to the clientThread method.
     *
     * @note This method uses detached threads to process each request.
     *
     * @param server The BOOST acceptor that must be used to accept connections
     * from clients.
     *
     * @param maxThr An optional maximum number of threads to be used by this
     * method.
     */
    void runServer(boost::asio::ip::tcp::acceptor& server, const int maxThr);\
    
    /**
    * This method is to  process GET requests corresponding to the 
    * Two different types of URLs and catch the errors if happened
    *
    * \param[in] is use to read GET request.
    *
    * \param[out] os to send HTTP respnse header and message
    */
    void serveClient(std::istream& is, std::ostream& os);

   protected:
    /**
     * This method is a refactored utility method. This method is called from
     * the seqlectQuery method. This method performs the actual operations
     * associated with printing a given set of columns in a given CSV that
     * match an optional condition. This method must print the column names
     * (separated by a tab character) and the selected data (also separated
     * by a tab). The method returns the total number of rows printed.
     *
     * @note For checking conditions, use the matches() method in the base
     * class.
     *
     * @note This method will be called from multiple threads. Hence, this
     * method must take care to ensure its operations are MT-Safe
     * (multi-threading safe).
     *
     * @param csv The CSV data to be used.
     *
     * @param mustWait If this flag is true, then this query must keep trying
     * until at least one row is selected.
     *
     * @param colNames The column names in the CSV file to be printed by this
     * method. The colNames will be just {"*"} or valid column names in the CSV.
     *
     * @param whereColIdx An optional column specified in a 'where' clause in
     * the query for checking. If a 'where' clause was not specified, then this
     * parameter will be -1. For example, if "where name like 'Test'" is
     * specified, then this parameter will have the index of the 'name' column
     * in the CSV.
     *
     * @param cond The condition to be applied. The condition will always be
     * one of "=" (equal-to), "<>" (not equal to), or "like" (substring). For
     * example, if "where name like 'Test'" is specified, then this parameter
     * will be "like" (without quotes).
     *
     * @param value The value to be used for comparison. This is parameter
     * has the value specified by the user in a 'where' clause. For
     * example, if "where name like 'Test'" is specified, then this parameter
     * will be "Test" (without quotes).
     *
     * @param os The output stream to where the results are to be written.
     *
     * @return The number of rows printed by this method.
     */
    int trySelect(CSV& csv, bool mustWait, StrVec colNames,
                  const int whereColIdx, const std::string& cond,
                  const std::string& value, std::ostream& os);

    /**
     * Method that is called to perform actual operations to update specified
     * values in the CSV. This method's documentation uses the following query
     * as an example:
     *
     *     update test.csv set rating=2.5, raters=2 where movieid = 12345;
     *
     * @note This method will be called from multiple threads. Hence, this
     * method must take care to ensure its operations are MT-Safe
     * (multi-threading safe).
     *
     * @param csv The CSV whose values are to be updated. Given the above query,
     * the CSV will correspond to the data for "test.csv" (loaded into memory
     * via call to the loadAndGet() method).
     *
     * @param mustWait If this flag is true then this method must repeatedly
     * try performing the update operation until at least 1 row is updated.
     *
     * @param colNames The names of the columns to be updated in each row.
     * This method may assume the colNames are valid (as they are validated
     * in the the update method). Given the above example query, this vector
     * will contain {"rating", "raters"}.
     *
     * @param values The values to be set for each column. This method may
     * assume the values and colNames match (as it is validated in the update
     * method).  Given the above example query, this vector will contain
     * {"2.5", "2"}.
     *
     * @param whereColIdx The integer value corresponding to the column in the
     * the where clause if any. If a where clause is not present then this
     * value is -1. In the above example query, is value will contain the index
     * of the movieid column in test.csv.
     *
     * @param cond The condition to be used. If a value is not specified it
     * is an empty string. Given the above example query, this parameter will
     * contain the value "=" (without quotes).
     *
     * @param value The value to be compared against. Given the above query,
     * this parameter will contain the value "12345" (without quotes)
     *
     * @return This method returns the number of rows updated by this method.
     */
    int tryUpdate(CSV& csv, bool mustWait, StrVec colNames, StrVec values,
                  const int whereColIdx, const std::string& cond,
                  const std::string& value);

    /**
     * A thread-main method to process each request from a web-client in a
     * separate thread. This method is called from the runServer method
     * each time a client connects, when sql-air is running as a web-server.
     * This web-server will get the following 2 types of HTTP-GET requests:
     *     1. Request to run a query where the request starts with the prefix
     *        "/sql-air?query=select;"
     *     2. All other requests are assumed to be requests for files that are
     *        returned back to the client using http::file() helper method in
     *        the HTTPFile class.
     *
     * @param client The socket stream to be used for performing all of the
     * I/O operations.
     */
    void clientThread(TcpStreamPtr client);

    /**
     * Internal helper method to obtain CSV file from a given URL. The URL
     * processing is initially done in the gloadAndGet method that calls
     * this method.  The URL of the form
     * "http://os1.csi.miamioh.edu/raodm/test.csv"
     * is broken down into host, port, and path by calling the
     * Helper::breakdownURL() method.  However, the user
     * must continue to use the full URL for referencing the data. This method
     * establishes the TCP stream and simply calls the csv.load() method to
     * load the CSV data from the TCP stream.
     *
     * @param csv The CSV object into which the data is to be loaded.
     *
     * @param hostName The server host name from where the data is to be
     * retrieved. This would be "localhost" or "os1.csi.miamioh.edu"
     *
     * @param port The port number from where the data is to be retrieved. This
     * value is typically 80 but could be different.
     *
     * @param path The path to the CSV file on the server. The path to the
     * file on the server. This is of the form "/test.csv"
     *
     * @exception Exp This method throws exceptions if errors ocurr when
     * reading the data from the server.
     */
    void loadFromURL(CSV& csv, const std::string& hostName,
                     const std::string& port, const std::string& path);

   private:
    /**
     * The most recently referenced CSV in a query. This value is updated
     * int he getOrLoadCSV method.
     */
    std::string recentCSV;

    /**
     * This is a convenience mutex that is used to enable thread-safe
     * operations on the recentCSV instance variable in this class. This
     * mutex is locked and unlocked in the loadAndGet method in this class.
     */
    std::mutex recentCSVMutex;

    /**
     * An unordered map to maintain the CSV files that have been accessed
     * in recent queries.  This map is used to provide convenient/rapid
     * access to CSV files that the user has recently worked with. The most
     * recent CSV used is tracked by the recentCSV instance variable. See the
     * getOrLoadCSV() method in this class.
     */
    std::unordered_map<std::string, CSV> inMemoryCSV;

    // -------------[ Limit number of threads ]-------------------
    /** The atomic counter that tracks the number of active threads.
     * This counter is incremented in runServer each time a thread is started.
     * It is decremented at the end of clientThread each time a thread finishes.
     */
    std::atomic<int> numThreads = {0};

    /** A condition variable to wait if number of threads being used
     * exceeds a given limit. The runServer method waits on it. The
     * clientThread method call notify.
     */
    std::condition_variable thrCond;
    // -----------------------------------------------------------
};

#endif /* SQL_AIR_H */
