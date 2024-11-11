#include <duckdb/duckdb.hpp>

#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <map>
#include <set>
#include <vector>
#include <chrono>

// TODO: GetValue is apparently slow, rewrite to use Fetch

using AttributeSet = std::set<int>;

std::string toString(const AttributeSet &attrSet) {
    std::string str = "AttrSet{";
    for (const int &val : attrSet) {
        str += std::to_string(val) + ", ";
    }
    if (!attrSet.empty()) {
        str.pop_back(); // Remove trailing space and comma
        str.pop_back();
    }
    str += "}";
    return str;
}

class SchemaMiner {
private:
    // Database
    duckdb::DuckDB db;
    duckdb::Connection conn;

    // Relation info
    std::string csvPath;
    int attributeCount;
    int tupleCount;

    // Entropy info
    std::map<AttributeSet, double> entropies;
    std::map<std::string, double> entropyMap;

    double getLogN() {
        return log2(tupleCount);
    }

public:
    SchemaMiner(std::string csvPath, int attributeCount) : db(nullptr), conn(db) {
        this->csvPath = csvPath;
        this->attributeCount = attributeCount;
    }

    std::string getTblName(const AttributeSet &attrSet) {
        // Convert set to string to create table name
        std::string name = "TBL_";
        for (const auto &val : attrSet) {
            name += std::to_string(val);
        }
        return name;
    }

    void getFirstLevelEntropies() {
        // Open the CSV file
        std::ifstream file(csvPath);
        if (!file.is_open()) {
            std::cerr << "Could not open the file " << csvPath << std::endl;
        }

        std::string line;
        std::vector<std::vector<std::string>> columns;

        // Read the first line to get the number of columns
        if (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string columnValue;
            while (std::getline(ss, columnValue, ',')) {
                columns.push_back(std::vector<std::string>());  // Initialize each column
            }
        }

        // Read through the file row by row
        do {
            std::stringstream ss(line);
            std::string value;
            size_t columnIndex = 0;

            // Extract each column value and store it in the corresponding column vector
            while (std::getline(ss, value, ',')) {
                if (columnIndex < columns.size()) {
                    columns[columnIndex].push_back(value);  // Add value to the respective column
                }
                columnIndex++;
            }
        } while (std::getline(file, line));

        file.close();

        tupleCount = columns[0].size();

        for (int i = 0; i < columns.size(); i++) {
            // Create TID table for column
            std::string tblName = getTblName({i});
            conn.Query("CREATE TABLE " + tblName + " (val VARCHAR(8), tid BIGINT);");
            std::string valIdx = "CREATE INDEX val_idx ON " + tblName + "(val);";
            conn.Query(valIdx);
            std::string tidIdx = "CREATE INDEX tid_idx ON " + tblName + "(tid);";
            conn.Query(tidIdx);

            auto column = columns[i];
            std::map<int, AttributeSet> valueMap = {};
            std::map<std::string, int> valueToKey;

            int key = 1;
            // Iterate through each value
            for (int j = 0; j < column.size(); j++) {
                std::string value = column[j];

                // If the value hasn't been encountered yet, assign a new key
                if (valueToKey.find(value) == valueToKey.end()) {
                    valueToKey[value] = key++;  // Assign a new key and increment
                }

                int assignedKey = valueToKey[value];
                valueMap[assignedKey].insert(j + 1);
            }

            // Populate TID table with non-singleton values
            for (const auto& pair : valueMap) {
                if (pair.second.size() > 1) {
                    for (const auto& idx : pair.second) {
                        auto value = std::to_string(i) + ":" + std::to_string(pair.first);
                        std::string insertQuery = "INSERT INTO " + tblName + " VALUES ('" + value + "', " + std::to_string(idx) + ");";
                        conn.Query(insertQuery);
                    }
                }
            }

            // Compute entropy for single attribute
            auto entropy = conn.Query("SELECT SUM(cnt) FROM (SELECT val, COUNT(*) * LOG2(COUNT(*)) AS cnt FROM " + tblName + " GROUP BY val) AS t;")->GetValue(0, 0).GetValue<double>();
            entropies[{i}] = getLogN() - (entropy / tupleCount);
        }
    }

    int getEntropy(AttributeSet t1, AttributeSet t2) {
        // Assume TID tables exist for t1 and t2

        // Check attributes don't overlap
        for (const auto& att : t1) {
            if (t2.find(att) != t2.end()) {
                std::cerr << "Attributes overlap in t1 and t2\n";
                return 1;
            }
        }

        // Compute joined CNT 
        auto tbl1 = getTblName(t1);
        auto tbl2 = getTblName(t2);
        t1.insert(t2.begin(), t2.end());
        auto joinedTbl = getTblName(t1);

        conn.Query(
            "CREATE TABLE CNT_" + joinedTbl + " AS (" +
            "SELECT HASH(t1.val, t2.val) AS val, COUNT(*) AS cnt " +
            "FROM " + tbl1 + " AS t1, " + tbl2 + " AS t2 " +
            "WHERE t1.tid = t2.tid GROUP BY HASH(t1.val, t2.val) HAVING COUNT(*) > 1);"
        );

        if (conn.Query("SELECT COUNT(*) FROM CNT_" + joinedTbl + ";")->GetValue(0, 0).GetValue<int>() != 0) {
            // Calculate entropy
            auto entropy = conn.Query("SELECT SUM(cnt * LOG2(cnt)) FROM CNT_" + joinedTbl + ";")->GetValue(0, 0).GetValue<double>();
            entropies[t1] = getLogN() - (entropy / tupleCount);

            // Compute TID by hashing and joining tables
            conn.Query(
                "CREATE TABLE " + joinedTbl + " AS (" +
                "SELECT HASH(t1.val, t2.val) AS val, t1.tid AS tid " +
                "FROM " + tbl1 + " AS t1, " + tbl2 + " AS t2, CNT_" + joinedTbl + " AS c " +
                "WHERE t1.tid = t2.tid AND HASH(t1.val, t2.val) = c.val);"
            );
            conn.Query("DROP TABLE CNT_" + joinedTbl + ";");

            return 0;
        }
        conn.Query("DROP TABLE CNT_" + joinedTbl + ";");
        return 1; // Table pruned
    }

    void computeEntropiesTIDCNT() {
        getFirstLevelEntropies();
        
        auto start = std::chrono::high_resolution_clock::now();
        std::queue<std::pair<AttributeSet, int>> q;
        for (int i = 0; i < attributeCount; i++) {
            q.push({{i}, i});
        }

        while (!q.empty()) {
            auto [attSet, last] = q.front();
            q.pop();

            if (last == attributeCount) {
                continue;
            }

            for (int i = last+1; i < attributeCount; i++) {
                if (getEntropy(attSet, {i}) == 0) {
                    auto newAttSet = attSet;
                    newAttSet.insert(i);
                    q.push({newAttSet, i});
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "Time taken: " << duration.count() << "ms\n";
    }

    bool isCombinationCommon(std::vector<int> columns, std::vector<std::string> values) {
        std::string query = "SELECT COUNT(*) FROM data WHERE ";
        for (int i = 0; i < columns.size(); i++) {
            query += "col" + std::to_string(columns[i]) + " = '" + values[i] + "'";
            if (i != columns.size() - 1) {
                query += " AND ";
            }
        }
        query += ";";
        return conn.Query(query)->GetValue(0, 0).GetValue<int>() > 1;
    }

    // Function to calculate the sum of count * log2(count) for a given combination
    double calculateSumCntLog2(const std::string& inputRelation, const AttributeSet& groupByCols) {
        // Create the SQL GROUP BY clause dynamically from the AttributeSet
        std::string groupByClause = "";
        for (auto it = groupByCols.begin(); it != groupByCols.end(); ++it) {
            if (it != groupByCols.begin()) groupByClause += ", ";
            groupByClause += "col" + std::to_string(*it + 1);  // col1, col2, ...
        }

        // Query to calculate SUM(COUNT(*) * LOG2(COUNT(*))) for the given group-by columns
        auto result = conn.Query("SELECT SUM(cnt) FROM ("
                                 "SELECT COUNT(*) * LOG2(COUNT(*)) AS cnt FROM " + inputRelation +
                                 " GROUP BY " + groupByClause + " HAVING COUNT(*) > 1) AS t;");
        if (result->RowCount() == 0) {
            return 0.0;  // No valid rows
        }
        return result->GetValue(0, 0).GetValue<double>();
    }

    // Recursive BUC function to partition and calculate entropies
    void runBUC(const std::string& inputRelation, int dim, const AttributeSet& combination) {
        std::string temp = "TEMP_" + std::to_string(dim);

        // Iterate through remaining attributes
        for (int d = dim; d < attributeCount; ++d) {
            // Generate the partition for the current dimension (column)
            auto result = conn.Query("SELECT col" + std::to_string(d + 1) + " FROM " + inputRelation + " GROUP BY col" + std::to_string(d + 1) + " HAVING COUNT(*) > 1;");
            if (result->RowCount() == 0) {
                return;
            }

            // Get common values for current dimension
            std::vector<std::string> commonVals;
            for (size_t i = 0; i < result->RowCount(); i++) {
                commonVals.push_back(result->GetValue(0, i).ToString());
            }

            // For each common value in the current attribute, recurse
            for (const auto& val : commonVals) {
                // Create a new AttributeSet by copying the current combination and adding the new dimension
                AttributeSet newCombination = combination;
                newCombination.insert(d);

                // Create temporary table for this partition
                conn.Query("CREATE TEMPORARY TABLE " + temp + " AS SELECT * FROM " + inputRelation +
                           " WHERE col" + std::to_string(d + 1) + " = '" + val + "';");

                // Add this combination to the entropy map
                double sumCntLog2 = calculateSumCntLog2(temp, newCombination);
                double entropy = getLogN() - (sumCntLog2 / tupleCount);
                entropies[newCombination] = entropy;

                // Recursively call the next dimension
                runBUC(temp, d + 1, newCombination);

                // Drop the temporary table after recursion
                conn.Query("DROP TABLE " + temp + ";");
            }
        }
    }

    // Convert combination string into a vector of column indices (e.g., "val1-val2" -> {0, 1})
    std::vector<int> getColumnsFromCombination(const std::string& combination) {
        std::vector<int> columns;
        size_t pos = 0;
        size_t nextPos;
        while ((nextPos = combination.find("-", pos)) != std::string::npos) {
            columns.push_back(std::stoi(combination.substr(pos, nextPos - pos)) - 1);  // assuming val is 1-based
            pos = nextPos + 1;
        }
        columns.push_back(std::stoi(combination.substr(pos)) - 1);  // add the last value
        return columns;
    }

    void computeEntropiesBUC() {
        // Load CSV data into DuckDB table
        std::string query = "CREATE TABLE data AS SELECT * FROM read_csv_auto('" + csvPath + "', names = [";
        for (int i = 0; i < attributeCount; i++) {
            query += "'col" + std::to_string(i) + "'";
            if (i != attributeCount - 1) query += ", ";
        }
        query += "]);";
        conn.Query(query);

        // Count the total tuples (rows) in the data table
        tupleCount = conn.Query("SELECT COUNT(*) FROM data;")->GetValue(0, 0).GetValue<int>();

        // Start recursive BUC algorithm
        runBUC("data", 0, AttributeSet());

        // Print computed entropies for all combinations
        for (const auto& [attrSet, entropy] : entropies) {
            std::cout << "Entropy for " << toString(attrSet) << ": " << entropy << "\n";
        }
    }

    void printEntropies() {
        for (const auto& [attSet, entropy] : entropies) {
            std::cout << "Entropy for ";
            std::cout << toString(attSet) << " ";
            std::cout << entropy << "\n";
        }
    }
};

int main() {
    SchemaMiner sm = SchemaMiner("small_flights.csv", 19);
    // SchemaMiner sm = SchemaMiner("data.csv", 12);

    auto start = std::chrono::high_resolution_clock::now();
    sm.computeEntropiesBUC();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Time taken (BUC): " << duration.count() << "ms\n";
}
