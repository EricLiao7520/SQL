#ifndef SQL_AIR_BASE_H
#define SQL_AIR_BASE_H

/* 
 * A base class for SQL-Air -- a very lightweight (light as air) implementation 
 * of a simple CSV-based  database system that uses SQL-like syntax for querying
 * and updating the CSV files.
 * 
 * Copyright (C) 2021 raodm@miamioh.edu
 */

#include <boost/asio.hpp>
#include <string>
#include <iostream>
#include <tuple>
#include "Helper.h"
#include "CSV.h"

/* 
 * A base class for SQL-Air. The base class provides some of the core
 * string processing and validation features of SQLAir.  The actual processing
 * of queries is delegated to a derived class which can manage data and 
 * implement the queries in slightly different ways.
 * 
 * @note Use an instance of the derived SQLAir class instead of this base
 * class.
 * 
 * @note Ideally, this class should take a CSV class as a template argument
 * and be very generic. However, for this project, we are assuming that we 
 * are working with a specific CSV object.
 * 
 * Copyright (C) 2021 raodm@miamioh.edu
 */
class SQLAirBase {
public:
    /**
     * Top-level method to process a SQL-air query.
     * 
     * @param sql The SQL-air query to be processed by this method.
     * 
     * @param os The output stream to where results from the processing are
     * to be written.
     * 
     * @return This method returns true if further queries are to be processed.
     * This method returns false if the command was "exit;" 
     */
    virtual bool process(const std::string& sql, std::ostream& os);
  
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
    virtual CSV& loadAndGet(std::string fileOrURL) {
        throw Exp("loadAndGet method not implemented");
    }

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
    virtual void selectQuery(CSV& csv, bool mustWait, StrVec colNames, 
        const int whereColIdx, const std::string& cond, 
        const std::string& value, std::ostream& os) {
        throw Exp("select not implemented.");
    }

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
    virtual void updateQuery(CSV& csv, bool mustWait, StrVec colNames,
        StrVec values, const int whereColIdx, const std::string& cond, 
        const std::string& value, std::ostream& os) {
        throw Exp("update not implemented.");
    }
    
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
    virtual void insertQuery(CSV& csv, bool mustWait, StrVec colNames,
        StrVec values, std::ostream& os) {
        throw Exp("insert not implemented.");
    }

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
    virtual void deleteQuery(CSV& csv, bool mustWait, const int whereColIdx, 
        const std::string& cond, const std::string& value, std::ostream& os) {
        throw Exp("insert not implemented.");
    }
    
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
    virtual void saveQuery(std::ostream& os) {
        throw Exp("save not implemented.");
    }

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
    virtual void runServer(boost::asio::ip::tcp::acceptor& server, 
        const int maxThr) {
        throw Exp("runServer not implemented.");
    }
    
    /**
     * A simple virtual destructor to be compliant with the virtual base class
     * requirements of C++
     */
    virtual ~SQLAirBase() {}

protected:
    /**
     * Helper method to check if value (in a column in a given row)  
     * matches/satisfies a given condition. This method is used in select 
     * and update statements with a "where" clause with conditions of the form
     * "... where col1 = 10" or "... where name like Smith" or 
     * "... where year <> 2020".
     * 
     * @param colVal The value in a column of the CSV to checked (against the 
     * value specified in the query).
     * 
     * @param cond The condition to be checked. The string can be one of
     * 3 values, namely: "=", "<>", or "like"
     * 
     * @param value The value specified by the user to be used.
     * 
     * @return This method returns \c true if the condition is met. Otherwise
     * it returns \c false.
     */
    virtual bool matches(const std::string& colVal, const std::string& cond, 
            const std::string& value) const;
    
    /**
     * Helper method to check if the list of column names (specified by the
     * user) are valid and present in a given CSV.  If a column name is
     * invalid, then this method throws an exception.
     * 
     * @param csv The CSV used to determine if the column names are valid.
     * @param colNames The list of column names to be checked.
     * @param emptyOK It is ok for the list to be empty. If not this method
     * throws an exception if the list is empty.
     * @param starOk It is ok if the list is just a "*". Otherwise this method
     * throws an exception.
     * @exception If a column name is invalid this method throws an exception.
     */
    virtual void checkColNames(const CSV& csv, const StrVec& colNames,
        const bool emptyOK = false, const bool starOk = true) const;
    
    /**
     * Checks if a SQL query is valid and calls the selectQuery() API method
     * to process a valid select statement. This method is called from
     * the process method to process a "select" statement.
     * 
     * @param sql The tokens in the select statement to be processed.
     * @param mustWait Flag to indicate if the query must keep running until
     * at least 1 matching row is found.
     * @param os The output stream to where the results are to be written.
     * 
     * @exception This method throws an exception if error occur when 
     * processing the specified SQL
     */
    virtual void validateAndProcessSelect(const StrVec& sql, bool mustWait, 
        std::ostream &os); 
    
    /**
     * Checks if a SQL query is valid and calls the updateQuery() API method
     * to process a valid update statement. This method is called from
     * the process method to process a "update" statement.
     * 
     * @param sql The tokens in the update statement to be processed.
     * @param mustWait Flag to indicate if the query must keep running until
     * at least 1 row is updated.
     * @param os The output stream to where the results are to be written.
     * 
     * @exception This method throws an exception if error occur when 
     * processing the specified SQL
     */
    virtual void validateAndProcessUpdate(const StrVec& sql, bool mustWait, 
        std::ostream &os);

    /**
     * Checks if a SQL query is valid and calls the insertQuery() API method
     * to process a valid insert statement. This method is called from
     * the process method to process an "insert" statement.
     * 
     * @param sql The tokens in the insert statement to be processed.
     * @param mustWait Flag to indicate if the query must keep trying until
     * a row is inserted.
     * @param os The output stream to where the results are to be written.
     * 
     * @exception This method throws an exception if error occur when 
     * processing the specified SQL
     */
    virtual void validateAndProcessInsert(const StrVec& sql, bool mustWait,
        std::ostream &os);

    /**
     * Checks if a SQL query is valid and calls the deleteQuery() API method
     * to delete rows from a CSV.  The specified CSV also becomes the
     * next default (until some other statement changes the default). This 
     * method is called from the process() method to process 
     * an "delete" statement.
     * 
     * @param sql The tokens in the insert statement to be processed.
     * @param mustWait Flag to indicate if the query must keep trying until
     * a row is deleted.
     * @param os The output stream to where the results are to be written.
     * 
     * @exception This method throws an exception if error occur when 
     * processing the specified query.
     */
    virtual void validateAndProcessDelete(const StrVec& sql, bool mustWait,
        std::ostream &os);

    /**
     * Checks if a SQL query is valid and calls the loadAndGet() API method
     * to load and set the specified CSV.  The specified CSV also becomes the
     * next default (until some other statement changes the default). This 
     * method is called from the process() method to process an "use" statement.
     * 
     * @param sql The tokens in the insert statement to be processed.
     * @param mustWait This flag is not applicable for this query. If specified,
     * it is ignored.
     * @param os The output stream to where the results are to be written.
     * 
     * @exception This method throws an exception if error occur when 
     * processing the specified query.
     */
    virtual void validateAndProcessUse(const StrVec& sql, bool mustWait,
        std::ostream &os);

    /**
     * Checks if a SQL query is valid and calls the saveQuery() API method
     * to save the specified CSV or the currently used CSV.  The specified CSV,
     * if any, also becomes the next default (until some other statement 
     * changes the default). This method is called from the process() method 
     * to process an "save" statement.
     * 
     * @param sql The tokens in the insert statement to be processed.
     * @param mustWait This flag is not applicable for this query. If specified,
     * it is ignored.
     * @param os The output stream to where the results are to be written.
     * 
     * @exception This method throws an exception if error occur when 
     * processing the specified query.
     */
    virtual void validateAndProcessSave(const StrVec& sql, bool mustWait,
        std::ostream &os);

    /**
     * This is a helper method that is called from process() method to
     * break a given query into tokens. This method generates tokens via
     * the following steps:
     * 
     * 1. It trims leading and trailing blank spaces and removes any trailing
     *    ';' by calling Helper::trim() method.
     * 2. It uses CSV::tokenize() method to create tokens.
     * 3. If the first word is "wait" it remove that word and sets the isWait
     *    flag. It removes the "wait" clause from the tokens.
     * 4. Returns the tokens back
     * 
     * @param sql A long string of sql-air query to be tokenized.
     * @return Returns the following 3 values from query tokenization process:
     *   1. The tokens from the sql. If the sql had no tokens then this
     *      method returns an empty vector.
     *   2. A boolean to indicate if the first token was a "wait" clause. The
     *   3. An integer corresponding to the command to be processed.
     */
    virtual std::tuple<StrVec, bool, int> 
        preprocess(const std::string& sql) const;
    
private:
   /**
     * Convenience internal helper method to extract name-value pair from
     * a SQL query into two separate vector-of-string.  This method is used
     * to extract column-names and values from update or insert queries. This
     * method is called from validateAndProcessUpdate() method as well as from
     * validateAndProcessInsert()  method.
     * 
     * @param csv The reference CSV to be used to check if column names are 
     * valid.
     * @param sql The tokens in the query from where column names and values
     * are to be extracted.
     * @param nameStartIdx The starting index in sql for column names. 
     * @param valStartIdx The starting index in sql for values.
     * @param inc The increment value between adjacent tokens. It is 3 for
     * update and 1 for insert statement respectively.
     * @param delimiter An optional delimiter value to check for. It is "="
     * in the case of update statements and empty string for insert.
     * @param endToken An optional end token to use. It is ")" for insert
     * and "where" for update.
     * @return A of vector-of-string, the first is list of column names
     * and the second is corresponding list of values. The 3rd value is the
     * index position in sql where the processing was stopped.
     * @exception This method throws exceptions when errors are encountered.
     */
    std::tuple<StrVec, StrVec, int> getNamesValues(const CSV& csv, 
            const StrVec& sql, int nameStartIdx, int valStartIdx, int inc = 1,
            const std::string& delimiter = "", 
            const std::string& endToken = "") const;
};

#endif /* SQL_AIR_BASE_H */

