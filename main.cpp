#include <duckdb/duckdb.hpp>

#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <vector>

// Custom Hash Function for AttributeSet
using AttributeSet = std::unordered_set<int>;
namespace std {
    template <>
    struct hash<AttributeSet> {
        size_t operator()(const AttributeSet &attrSet) const {
            size_t hashVal = 0;
            for (const int &val : attrSet) {
                hashVal ^= std::hash<int>()(val) + 0x9e3779b9 + (hashVal << 6) + (hashVal >> 2);
            }
            return hashVal;
        }
    };
}

std::string toString(const AttributeSet &attrSet) {
    std::string str = "AttrSet{";
    for (const int &val : attrSet) {
        str += std::to_string(val) + ", ";
    }
    str.pop_back();
    str.pop_back();
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
    std::unordered_map<AttributeSet, double> entropies;

    double getLogN() {
        return log2(tupleCount);
    }

public:
    SchemaMiner(std::string csvPath, int attributeCount) : db(nullptr), conn(db) {
        this->csvPath = csvPath;
        this->attributeCount = attributeCount;
        readFromCSV();
    }

    void readFromCSV() {
    }

    std::string getTblName(const AttributeSet &attrSet) {
        std::hash<AttributeSet> hashFn;
        size_t hashVal = hashFn(attrSet);

        return "TBL_" + std::to_string(hashVal);
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
            std::string create = "CREATE TABLE " + tblName + " (val VARCHAR(40), tid BIGINT);";
            conn.Query(create);
            std::string valIdx = "CREATE INDEX val_idx ON " + tblName + "(val);";
            conn.Query(valIdx);
            std::string tidIdx = "CREATE INDEX tid_idx ON " + tblName + "(tid);";
            conn.Query(tidIdx);

            auto column = columns[i];
            std::map<int, std::set<int>> valueMap = {};
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

    void getEntropy(AttributeSet t1, AttributeSet t2) {
        // Assume TID tables exist for t1 and t2 

        // Check attributes dont overlap
        for (const auto& att : t1) {
            if (t2.find(att) != t2.end()) {
                std::cerr << "Attributes overlap in t1 and t2\n";
                return;
            }
        }

        // Compute joined CNT 
        auto tbl1 = getTblName(t1);
        auto tbl2 = getTblName(t2);
        t1.insert(t2.begin(), t2.end());
        auto joinedTbl = getTblName(t1);

        conn.Query("CREATE TABLE CNT_" + joinedTbl + " AS (SELECT HASH(t1.val, t2.val) AS val, COUNT(*) AS cnt FROM " + tbl1 + " AS t1, " + tbl2 + " AS t2 WHERE t1.tid = t2.tid GROUP BY HASH(t1.val, t2.val) HAVING COUNT(*) > 1);");
        std::cout << "For " << toString(t1) << " and " << toString(t2) << ":\n";


        // Compute entropy 
        auto entropy = conn.Query("SELECT SUM(cnt * LOG2(cnt)) FROM CNT_" + joinedTbl + ";")->GetValue(0, 0).GetValue<double>();
        entropies[t1] = getLogN() - (entropy / tupleCount);

        // Compute TID 
        conn.Query("CREATE TABLE " + joinedTbl + " AS (SELECT HASH(t1.val, t2.val) AS val, t1.tid AS tid FROM " + tbl1 + " AS t1, " + tbl2 + " AS t2, CNT_" + joinedTbl + " AS c WHERE t1.tid = t2.tid AND HASH(t1.val, t2.val) = c.val);");
        conn.Query("CREATE INDEX val_idx ON " + joinedTbl + "(val);");
        conn.Query("CREATE INDEX tid_idx ON " + joinedTbl + "(tid);");

        // Drop CNT
        conn.Query("DROP TABLE CNT_" + joinedTbl + ";");
    }


    void computeEntropies() {
        getFirstLevelEntropies();

        getEntropy({0}, {1});
        getEntropy({2}, {3});
        getEntropy({4}, {5});
        getEntropy({6}, {7});
        getEntropy({8}, {9});
        getEntropy({10}, {11});

        for (const auto& [attSet, entropy] : entropies) {
            std::cout << "Entropy for ";
            std::cout << toString(attSet) << " ";
            std::cout << entropy << "\n";
        }
    }
};

int main() {
    SchemaMiner sm = SchemaMiner("data.csv", 12);
    sm.computeEntropies();
    return 0;
}