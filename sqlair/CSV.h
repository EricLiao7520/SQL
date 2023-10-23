#ifndef CSV_H
#define CSV_H

/**
 * A simple C++ class to encapsulate data loaded from a Comma Separated
 * Value (CSV) file.  The first line of the CSV is assumed to be a
 * header that provides titles for each column.  Note that all of the
 * data is stored as strings.  
 *
 * Copyright (C) 2021 raodm@miamioh.edu
 */

#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <condition_variable>

/** A short cut to refer to a vector of strings */
using StrVec = std::vector<std::string>;

/**
 * A custom vector-of-string to store information about each column in a
 * a row in a CSV.  This class is used to provide two additional values for
 * each row:
 *     1. A mutex to enable Multithread-Safe (MT-Safe) operation (i.e., two
 *        threads don't try to modify the same row)
 *     2. A condition variable to efficiently wait when a row is locked by
 *        another thread.
 * 
 * Of course, creating two extra objects for each row in a CSV is not very
 * efficient for large data sets. However, optimizing this design can come
 * at a later time.
 */
class CSVRow : public StrVec {
public:
    /**
     * A simple default constructor to create an empty CSVRow.
     */
    CSVRow() {}

    /** A move constructor to efficiently move data without impacting
     * the mutexs in this object. The mutex and condition variable cannot be
     * moved or copied.
     * 
     * @param row The source object from where data is to be moved.
     */
    CSVRow(CSVRow&& row) : StrVec(std::move(row)) {}
    
    /** Convenience constructor to create a row with given data.
     * 
     * @param data The source data to be copied into this class.
     */
    CSVRow(const StrVec& data) : StrVec(data) {}

    /** Convenience constructor to copy a row. This is needed because
     * the std::mutex in this object cannot be copied.
     * 
     * @param data The source data to be copied into this class.
     */
    CSVRow(const CSVRow& data) : StrVec(data) {}

    /** 
     * A convenience operator= to ease copying values in a row without
     * copying the std::mutex (because mutex's are not copyable). 
     * 
     * @param src The source row from where the data is to be copied.
     * 
     * @return As per API convention, this method always returns a reference
     * to this object.
     */
    CSVRow& operator=(const CSVRow& src) { 
        StrVec::operator=(src);
        return *this;
    }
    
    /** A convenience mutex for each row to enable MT-safe operations. */
    std::mutex rowMutex;
};

/**
 * Convenience stream insertion operator to print a vector-of-strings.
 * This helper method can be used in the following way:
 * 
 * \code
 *     std::cout << StrVec({"this", "is", "a", "test"});
 * \endcode
 * 
 * @param os The output stream to where the vector should be written
 * @param vec The vector to be printed.
 * @return This method returns the supplied output stream to be consistent
 * with API guidelines.
 */
std::ostream& operator<<(std::ostream& os, const StrVec& vec);

/** A simple class to load and manage data from a Tab Separated Value
 * (CSV) file.  An example CSV file could be:
 *
 * stock,   company,    price,    count
 * msft,    Microsoft,  25.2,     1000
 * appl,    Apple,      125.2,    20000
 */
class CSV : public std::vector<CSVRow> {
public:
    /**
     * Loads data from a given stream.  The first line of the CSV
     * file is assumed to be a header-line that provides column
     * names. The column names are stored in an unordered_map to ease
     * quick look-up of column numbers to access data in each row.
     *
     * \param[in] is The input stream from where the CSV data is to be
     * loaded.
     */
    void load(std::istream& is);
    
    /**
     * Saves this CSV data to a given stream. Each value by default is
     * quoted, though this behavior can be changed. 
     * 
     * @param[out] os The output stream to where the CSV data is to be
     * written. If this stream is invalid, then an exception is thrown.
     * 
     * @param[in] delim The delimiter to use between each column. 
     * 
     * @param[in] quote If this flag is true then each value is quoted.
     * Otherwise values are written without quotations.
     * 
     * @param[in] nl The string to be used for new lines.
     *  
     * @param csvData The output stream to where the data is to be written.
     */
    void save(std::ostream& os, const std::string& delim = ",", 
        bool quote = true, const std::string& nl = "\n") const;
    
    /** Obtain the number of rows in the CSV.
     *
     * \return The number of rows in the CSV file.
     */
    int getRowCount() const { return this->size(); }

    /**
     * Obtain the number of columns in each row of the CSV.
     *
     * \return The number of columns in each row of the CSV.
     */
    int getColumnCount() const { return colNames.size(); }

    /**
     * Returns the names of the columns in the order in which they
     * appear in the CSV.
     * 
     * @return A vector-of-strings containing column names in the same
     * order in which they appeared in the CSV. 
     */
    StrVec getColumnNames() const;
    
    /**
     * This is a convenience method to map a given column name to an
     * index position to ease accessing/requesting contents from a
     * given row.
     *
     * \note Ensure the CSV data is successfully loaded prior to using
     * this method.
     *
     * \param[in] colName The name of the column whose logical
     * position in the CSV file is to be returned.
     *
     * \return This method returns -1 if the column name is
     * invalid. Otherwise it returns the zero-based column number for
     * the given column name.
     */
    int getColumnIndex(const std::string& colName) const {
        // Get an iterator to the entry
        const auto iter = colNames.find(colName);
        // Return index if column was found, otherwise return -1
        return (iter != colNames.end() ? iter->second : -1);
    }

    /**
     * API method to move the data from a given CSV
     * 
     * @param other The other CSV from where the data is to be moved into
     * this CSV. Existing data in this CSV is lost.
     */
    void move(CSV& other);

    /**
     * Split a given string based on spaces or other specified
     * delimiters while handling quotes.  This method does a
     * reasonably involved split operation to handle CSV files (or
     * other inputs) in different formats.  Here are some examples:
     *
     *   Input string      | Returned vector
     *   ------------------| ---------------
     *   ",,,,"            | {"", "", "", ""}
     *   " , test, ,"      | {"", "test", "", ""}
     *   ""t","e","s","t"" | {"t", "e", "s", "t"}
     *
     * \param[in] str The string to be split into individual phrases.
     * Note that a phrase can contain multiple words if it is in
     * double quotes.
     *
     * \param[in] spcDelim Flag to indicate if blank space should be
     * treated as a delimiter.  If this flag is false, then blank
     * spaces are not tread as special delimiters.
     *
     * \param[in] splChars A list of characters that must be
     * considered as delimiters and should be included in the tokens.
     * Consecutive special chars are combined into a single token.
     *
     * \param[in] stopChars Optional set of characters on which the
     * tokenizer must stop when it is encoutered
     *
     * \param[in] keepQuotes Preserve quotes around words, if the data
     * contains them.
     *
     * \param[in] lowcase If this flag is true then each token that is
     * not explictly quoted is converted to lower case.
     */
    static StrVec tokenize(const std::string& str,
                           const std::string& delims = ",",
                           bool spcDelim = true,
                           const std::string& splChars = "<>=!()",
                           const std::string& stopChars = "",
                           const bool keepQuotes = false,
                           const bool lowcase = true);

    /**
     * Split a given string based on spaces or other specified
     * delimiters while handling quotes.  This method does a
     * reasonably involved split operation to handle CSV files (or
     * other inputs) in different formats.  Here are some examples:
     *
     *   Input string      | Returned vector
     *   ------------------| ---------------
     *   ",,,,"            | {"", "", "", ""}
     *   " , test, ,"      | {"", "test", "", ""}
     *   ""t","e","s","t"" | {"t", "e", "s", "t"}
     *
     * \param[in] is An input stream from where the data is to be read
     * and tokenized.
     *
     * \param[in] spcDelim Flag to indicate if blank space should be
     * treated as a delimiter.  If this flag is false, then blank
     * spaces are not tread as special delimiters.
     *
     * \param[in] splChars A list of characters that must be
     * considered as delimiters and should be included in the tokens.
     * Consecutive special chars are combined into a single token.
     *
     * \param[in] stopChars Optional set of characters on which the
     * tokenizer must stop when it is encoutered
     *
     * \param[in] keepQuotes Preserve quotes around words, if the data
     * contains them.
     *
     * \param[in] lowcase If this flag is true then each token that is
     * not explictly quoted is converted to lower case.
     */
    static StrVec tokenize(std::istream& is,
                           const std::string& delims = ",",
                           bool spcDelim = true,
                           const std::string& splChars = "<>=!()",
                           const std::string& stopChars = "",
                           const bool keepQuotes = false,
                           const bool lowcase = true);

    /**
     * Converts string to lower case.
     *
     * \param[in] str The string whose characters are to be convereted
     * to lower case.  This method intentionally takes a copy of the
     * string.
     *
     * \return A copy of the string with all characters in lower case.
     */
    static std::string toLower(std::string str);

    /** A mutex that can be used for blocking-CSV level operations to enable
     * MT-safe operations. Of course, blocking at the CSV level this way is
     * rather inefficient. Instead a reader-write type lock is the preferred
     * approach.
     */
    std::mutex csvMutex;
    
    /** A condition variable for sleep-wake-up approach for waiting on
     * some condition to be met.
     */
    std::condition_variable csvCondVar;

    /**
     * Number of threads just doing reads (i.e., select).  This variable  
     * may be used for most effectively implementing the insert & delete 
     * methods.
     */
    int numReadThreads = 0;

    /**
     * Number of threads just doing writes (i.e., update, insert, or delete).  
     * This variable is may be used for most effectively implementing the 
     * insert & delete methods.
     */
    int numWriteThreads = 0;

protected:
    // Currently, this class does not have protected members

private:
    /**
     * An map to quickly map names of columns to corresponding index
     * positions in each row of data.  For example, if a CSV file has
     * the following header line (giving names for 4-columns)
     *       stock    company     price     count
     *
     * Then this map would contain data in the form -- {{"price", 2},
     * {"stock", 0}, {"count", 3}, {"company", 1}} -- where the number
     * indicates the zero-based column number.
     */
    std::unordered_map<std::string, int> colNames;
};

#endif
