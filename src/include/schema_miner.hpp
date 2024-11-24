#ifndef SCHEMA_MINER_HPP
#define SCHEMA_MINER_HPP

#include "duckdb.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <map>
#include <set>
#include <vector>
#include <chrono>
#include <algorithm>

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
protected:
    // Database 
    duckdb::DuckDB db;
    duckdb::Connection conn;

    // Relation info
    std::string csvPath;
    int attributeCount;
    int tupleCount;
    std::hash<std::string> strHasher;
    std::hash<int> intHasher;

    std::map<AttributeSet, double> entropies;

    std::map<int, int> attributeRenames;

    double getLogN() {
        return log2(tupleCount);
    }

    std::string getTblName(const AttributeSet &attrSet) {
        std::string name = "TBL_";
        for (const auto &val : attrSet) {
            name += std::to_string(val);
        }
        return name;
    }

    void reorderColumns() {
        std::vector<std::pair<int, int>> colCounts = {};
        for (int i = 0; i < attributeCount; i++) {
            colCounts.push_back({i, conn.Query("SELECT COUNT(DISTINCT col" + std::to_string(i) + ") FROM data;")->GetValue(0, 0).GetValue<int>()});
        }
    
        std::sort(colCounts.begin(), colCounts.end(), [](const std::pair<int, int>& a, const std::pair<int, int>& b) {
            return a.second > b.second; // Compare the second elem (distinct count)
        });

        // Temporarily rename columns 
        for (int i = 0; i < attributeCount; i++) {
            conn.Query("ALTER TABLE data RENAME COLUMN col" + std::to_string(i) + " TO temp_col" + std::to_string(i) + ";");
        }

        // Reorder
        for (int i = 0; i < attributeCount; i++) {
            conn.Query("ALTER TABLE data RENAME COLUMN temp_col" + std::to_string(colCounts[i].first) + " TO col" + std::to_string(i) + ";"); 
            attributeRenames[i] = colCounts[i].first;
        }
    }

    void renameEntropies() { 
        std::map<AttributeSet, double> newEntropies;
        for (const auto& [attSet, entropy] : entropies) {
            AttributeSet newAttSet;
            for (const auto& att : attSet) {
                newAttSet.insert(attributeRenames[att]);
            }
            newEntropies[newAttSet] = entropy;
        }
        entropies = newEntropies;
    }

public:
    SchemaMiner(std::string csvPath, int attributeCount) : db(nullptr), conn(db) {
        this->csvPath = csvPath;
        this->attributeCount = attributeCount;
    }

    void clearEntropies() {
        entropies.clear();
    }

    virtual void computeEntropies() = 0;

    void printEntropies() {
        for (const auto& [attSet, entropy] : entropies) {
            std::cout << "Entropy for ";
            std::cout << toString(attSet) << " ";
            std::cout << entropy << "\n";
        }
    }

};

#endif // SCHEMA_MINER_HPP