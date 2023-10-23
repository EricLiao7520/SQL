#ifndef HELPER_H
#define HELPER_H

/*
 * A relatively straightforward class to help with validating and streamlining
 * processing of SQL. This class just has convenient utility methods that
 * are used in the SQLAir main class.
 * 
 * Copyright (C) 2021 raodm@miamioh.edu
 */

#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <tuple>

/** A synonym to throw a specific type of exception. Later on we could change
 * this one to throw a custom exception
 */
using Exp = std::runtime_error;

/** A short cut to refer to a vector of strings */
using StrVec = std::vector<std::string>;

/**
 * This class has several static helper methods to streamline validation
 * and processing of SQL. Note that all of the methods in this class are
 * static. Hence, they can be directly called without needing to create
 * objects.
 */
class Helper {
public:
    /**
     * Helper method return the index position of a string in a given
     * vector. Note that the search is case sensitive.
     * 
     * @param vec The vector to search in.
     * @param str The string to search for.
     * @param startIdx An optional starting index from where to search.
     * 
     * @return Returns the zero-based index position of str in vec.
     * If str was not found in vec, then this method returns -1.
     */
    static int find(const StrVec& vec, const std::string& str,
            const int startIdx = 0) {
        auto entry = std::find(vec.begin() + startIdx, vec.end(), str);
        return (entry != vec.end() ? (entry - vec.begin()) : -1);
    }

    /**
     * Convenience method to trim leading & trailing blank spaces in a string.
     * @param str The string to be trimmed.
     * @param remove An optional set of trailing characters to be removed
     * after the string has been trimmed.
     * @return A string without leading & trailing blank spaces and any optional
     * trailing characters removed.
     */
    static std::string trim(const std::string& str, 
        const std::string& removeTrailing = "");
    
    /**
     * Helper method to extract the optional CSV/URL from a SQL-AIR query. This 
     * method can be used to extract CSV/URL from queries of the form:
     *     - select * from "user.csv" where
     *     - update "http://localhost:3030/user.csv" set...
     * 
     * @param sql The tokens of the query produced by CSV::tokenize method.
     * @param anchor The keyword to be used as the anchor to find the file/url
     * on a given sql query. Valid options include "from", "update", and
     * "into".
     * @param ignore A set of keywords if set as file/url should cause them
     * to be ignored and return an empty string.
     * 
     * @return This method returns the csv/url specified in the query.
     * @exception This method throws exceptions if SQL syntax is not valid.
     */
    static std::string getCSVInfo(const StrVec& sql, 
        const std::string& anchor = "from", const StrVec& ignore = {"set"});

    /**
     * Helper method to extract the column names specified as part of a select
     * SQL query statement.  This method can be used to extract columns from 
     * queries of the form:
     *     - select * from "user.csv" where
     *     - select a, b, c where
     * 
     * @param sql The tokens of the query produced by CSV::tokenized method.
     * @return This method returns the set of columns specified in the query.
     * @exception This method throws Exp exception if SQL syntax is not valid.
     */
    static StrVec getSelectColNames(const StrVec& sql);

    /**
     * Helper method to extract the column name, condition, and value associated
     * with the where clause (if any) in the query.
     *     - select * where name = "test";
     *     - select a, b where age = 20;
     *     - update set age = '11' where age = 10;
     * 
     * @param sql The tokens of the query produced by CSV::tokenized method.
     * @param validColNames Valid columns that can be specified. This list is
     * typically taken from the list of columns in a CSV.
     * @param startIdx An optional starting index from where to search for the
     * 'where' clause.
     * 
     * @return This method returns the the column name, condition, and value
     * specified. If a where clause was not present, then it returns empty 
     * strings for all 3.
     * @exception This method throws Exp exception if SQL syntax is not valid.
     */
    static std::tuple<std::string, std::string, std::string> 
    getWhereClause(const StrVec& sql, const StrVec& validColNames,
        const int startIdx = 0);

    /** Convenience method to decode HTML/URL encoded strings.
     * 
     * This method must be used to decode query string parameters
     * supplied along with GET request.  This method converts URL encoded
     * entities in the from %nn (where 'n' is a hexadecimal digit) to 
     * corresponding ASCII characters.
     * 
     * \param[in] str The string to be decoded.  If the string does not
     * have any URL encoded characters then this original string is
     * returned.  So it is always safe to call this method!
     * 
     * \return The decoded string.
     */
    static std::string url_decode(std::string str);

    /**
     * Helper method to break down a URL into hostname, port and path. For
     * example, given the url: "https://localhost:8080/~raodm/one.txt"
     * this method returns <"localhost", "8080", "/~raodm/one.txt">
     *
     * Similarly, given the url: "ftp://ftp.files.miamioh.edu/index.html"
     * this method returns <"ftp.files.miamioh.edu", "80", "/index.html">
     *
     * @param url A string containing a valid URL. The port number in URL
     * is always optional.  The default port number is assumed to be 80.
     *
     * @return This method returns a std::tuple with 3 strings. The 3
     * strings are in the order: hostname, port, and path.  Here we use
     * std::tuple because a method can return only 1 value.  The
     * std::tuple is a convenient class to encapsulate multiple return
     * values into a single return value.
     */
    static std::tuple<std::string, std::string, std::string>
    breakDownURL(const std::string& url);

private:
    /**
     * This class is never meant to be instantiated. Hence the constructor
     * is intentionally private.
     */
    Helper();
};

#endif /* HELPER_H */

