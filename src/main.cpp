#include "schema_miner.hpp"

class SchemaMinerTIDCNT : public SchemaMiner {
private:
    std::queue<std::pair<AttributeSet, int>> getFirstLevelEntropies() {
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

        std::queue<std::pair<AttributeSet, int>> q;

        for (int i = 0; i < columns.size(); i++) {
            // Create TID table for column
            std::string tblName = getTblName({i});
            conn.Query("CREATE TABLE " + tblName + " (val VARCHAR(8), tid BIGINT);");
            std::string valIdx = "CREATE INDEX val_idx_" + tblName + " ON " + tblName + "(val);";
            conn.Query(valIdx);
            std::string tidIdx = "CREATE INDEX tid_idx_" + tblName + " ON " + tblName + "(tid);";
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
            auto qry = conn.Query("SELECT SUM(cnt) FROM (SELECT val, COUNT(*) * LOG2(COUNT(*)) AS cnt FROM " + tblName + " GROUP BY val) AS t;");
            try {
                auto entropy = qry->GetValue(0, 0).GetValue<double>();
                entropies[{i}] = getLogN() - (entropy / tupleCount);
                q.push({{i}, i});
            } catch (const std::exception& e) {
                // Catch NULL returns when there are no common values
                continue;
            }
        }
        return q;
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
        return 1;
    }
    

public:
    SchemaMinerTIDCNT(const std::string& csvPath, int attributeCount) : SchemaMiner(csvPath, attributeCount) {}

    void computeEntropies() override {
        std::queue<std::pair<AttributeSet, int>> q = getFirstLevelEntropies();
    

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
    }
};

class SchemaMinerBUC : public SchemaMiner {
private:
    void runBUCFilter(const std::string& tblName, AttributeSet attSet, const std::string& filter = "") {
        int prevPartitionAtt = attSet.empty() ? -1 : *attSet.rbegin();

        for (int i = prevPartitionAtt + 1; i < attributeCount; i++) {
            AttributeSet nextAttSet = attSet;
            nextAttSet.insert(i);

            // If we're at the last attribute, count rather than recurse
            if (i == attributeCount - 1) {
                std::string qryStr = "SELECT SUM(cnt) FROM ("
                    "SELECT col" + std::to_string(i) + 
                    ", COUNT(*) * LOG2(COUNT(*)) AS cnt "
                    "FROM " + tblName + 
                    (filter.empty() ? "" : " WHERE " + filter) +
                    " GROUP BY col" + std::to_string(i) +
                    " HAVING COUNT(*) > 1) AS t;";
                auto qry = conn.Query(qryStr);
                try {
                    auto cnt = qry->GetValue(0, 0).GetValue<double>();
                    entropies[nextAttSet] += cnt;
                } catch (const std::exception& e) {
                    // Catch NULL returns when there are no common values
                    continue;
                }
                return;
            }

            // Get common values of the partition attribute 
            std::string qryStr = "SELECT col" + std::to_string(i) + " "
                "FROM " + tblName + 
                (filter.empty() ? "" : " WHERE " + filter) +
                " GROUP BY col" + std::to_string(i) +
                " HAVING COUNT(*) > 1;";
            auto query = conn.Query(qryStr);

            if (query->RowCount() == 0) {
                continue; // Exit early if no common values
            }

            std::vector<std::string> commonValues;
            for (int j = 0; j < query->RowCount(); j++) {
                commonValues.push_back(query->GetValue(0, j).ToString());
            }

            // For each common value, filter and recurse 
            for (const auto& val : commonValues) {
                // Extend filtering condition 
                std::string newFilter = (filter.empty() ? "" : filter + " AND ") +
                    "col" + std::to_string(i) + " = '" + val + "'";

                // Count distinct values 
                std::string cntQryStr = "SELECT COUNT(*) FROM " + tblName + 
                    " WHERE " + newFilter + ";";
                auto cnt = conn.Query(cntQryStr)->GetValue(0, 0).GetValue<int>();
                entropies[nextAttSet] += (cnt * log2(cnt));

                // Recurse
                runBUCFilter(tblName, nextAttSet, newFilter);
            }
        }
    }

    void runBUC(const std::string& tblName, AttributeSet attSet) {
        // std::cout << "Running BUC on table: " << tblName << " with attributes: " << toString(attSet) << '\n';
        // Iterate through possible partitionAtts remaining 
        int prevPartitionAtt = attSet.empty() ? -1 : *attSet.rbegin(); 

        for (int i = prevPartitionAtt + 1; i < attributeCount; i++) {
            // std::cout << "Partitioning on attribute: " << i << '\n';
            AttributeSet nextAttSet = attSet;
            nextAttSet.insert(i);
            
            // If we're at the last attribute, just count rather than partition
            if (i == attributeCount-1) {
                auto qry = conn.Query("SELECT SUM(cnt) FROM (SELECT col" + std::to_string(i) + ", COUNT(*) * LOG2(COUNT(*)) AS cnt FROM " + tblName + " GROUP BY col" + std::to_string(i) + " HAVING COUNT(*) > 1) AS t;");
                try {
                    auto cnt = qry->GetValue(0, 0).GetValue<double>();
                    entropies[nextAttSet] += cnt;
                } catch (const std::exception& e) {
                    // Catch NULL returns when there are no common values
                    continue;
                }
                return;
            }
            
            // Get common values of the partition attribute 
            auto query = conn.Query("SELECT col" + std::to_string(i) + " FROM " + tblName + " GROUP BY col" + std::to_string(i) + " HAVING COUNT(*) > 1;");
            if (query->RowCount() == 0) {
                // std::cout << "No common values found for attribute: " << i << "\n";
                continue; // Exit early if no common values
            }

            std::vector<std::string> commonValues;
            // std::cout << "Common values for attribute " << i << ": ";
            for (int j = 0; j < query->RowCount(); j++) {
                // std::cout << query->GetValue(0, j).ToString() << " ";
                commonValues.push_back(query->GetValue(0, j).ToString());
            }
            // std::cout << '\n';

            // For each common value, partition on value and recurse 
            for (const auto& val : commonValues) {
                std::string temp = "TEMP_" + std::to_string(strHasher(val)) + "_" + std::to_string(intHasher(i));
                // std::cout << "Creating temporary table: " << temp << " for value: " << val << '\n';
                conn.Query(
                    "CREATE TABLE " + temp + 
                    " AS SELECT * EXCLUDE (col" + std::to_string(i) + 
                    ") FROM " + tblName + 
                    " WHERE col" + std::to_string(i) + " = '" + val + "';"
                );
                // conn.Query("SELECT * FROM " + temp + ";")->Print();

                // Get count of distinct values 
                auto cnt = conn.Query("SELECT COUNT(*) FROM " + temp + ";")->GetValue(0, 0).GetValue<int>();
                // std::cout << "Adding count: " << (cnt * log2(cnt)) << " to entropy of " << toString(nextAttSet) << '\n';
                entropies[nextAttSet] += (cnt * log2(cnt));

                // Recurse
                runBUC(temp, nextAttSet);

                conn.Query("DROP TABLE " + temp + ";");
            }
        }
    }

public:
    SchemaMinerBUC(const std::string& csvPath, int attributeCount) : SchemaMiner(csvPath, attributeCount) {}

    void computeEntropies() override {
        // Insert data 
        std::string query = "CREATE TABLE data AS SELECT * FROM read_csv('" + csvPath + "', header=false, names=[";
        for (int i = 0; i < attributeCount; i++) {
            query += "'col" + std::to_string(i) + "'";
            if (i != attributeCount - 1) {
                query += ", ";
            }
        }
        query += "]);";
        conn.Query(query);

        tupleCount = conn.Query("SELECT COUNT(*) FROM data;")->GetValue(0, 0).GetValue<int>();

        reorderColumns();

        // Start
        runBUCFilter("data", {});

        // Convert raw counts to entropies 
        for (const auto& [attSet, entropy] : entropies) {
            entropies[attSet] = getLogN() - (entropy / tupleCount);
        }

    }
};

int main() {
    return 0;
}