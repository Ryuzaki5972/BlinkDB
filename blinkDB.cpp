#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>
#include <sstream>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <bitset>
#include <shared_mutex>
#include <mutex>
#include <algorithm>
#include <memory>


#define PORT 9001
#define MAX_EVENTS 10
#define BUFFER_SIZE 1024
#define CACHE_SIZE 1000
#define BLOOM_FILTER_SIZE 10000

// Forward declarations
class DataType;
class StringType;
class ListType;
class SetType;
class HashType;

// Value types enum
enum class ValueType {
    STRING,
    LIST,
    SET,
    HASH
};

// Base abstract class for all data types
class DataType {
public:
    virtual ~DataType() = default;
    virtual ValueType get_type() const = 0;
    virtual std::string serialize() const = 0;
    virtual void deserialize(const std::string& data) = 0;
    virtual std::string to_string() const = 0;
};

// String data type
class StringType : public DataType {
private:
    std::string value;

public:
    explicit StringType(const std::string& val = "") : value(val) {}

    ValueType get_type() const override {
        return ValueType::STRING;
    }

    std::string serialize() const override {
        return value;
    }

    void deserialize(const std::string& data) override {
        value = data;
    }

    std::string to_string() const override {
        return value;
    }

    void set(const std::string& val) {
        value = val;
    }

    std::string get() const {
        return value;
    }
};

// List data type
class ListType : public DataType {
private:
    std::vector<std::string> elements;

public:
    ValueType get_type() const override {
        return ValueType::LIST;
    }

    // Serializes list to string format for storage
    std::string serialize() const override {
        std::string result = "L";
        for (const auto& element : elements) {
            result += std::to_string(element.size()) + ":" + element + ",";
        }
        return result;
    }

    // Deserializes string to list
    void deserialize(const std::string& data) override {
        elements.clear();
        if (data.empty() || data[0] != 'L') return;
        
        size_t pos = 1;
        while (pos < data.size()) {
            size_t colon_pos = data.find(':', pos);
            if (colon_pos == std::string::npos) break;
            
            int len = std::stoi(data.substr(pos, colon_pos - pos));
            std::string element = data.substr(colon_pos + 1, len);
            elements.push_back(element);
            
            pos = colon_pos + len + 2; // Skip past element and comma
        }
    }

    std::string to_string() const override {
        std::string result = "[";
        for (size_t i = 0; i < elements.size(); ++i) {
            if (i > 0) result += ", ";
            result += elements[i];
        }
        result += "]";
        return result;
    }

    // List operations
    void lpush(const std::string& value) {
        elements.insert(elements.begin(), value);
    }

    void rpush(const std::string& value) {
        elements.push_back(value);
    }

    std::string lpop() {
        if (elements.empty()) return "";
        std::string value = elements.front();
        elements.erase(elements.begin());
        return value;
    }

    std::string rpop() {
        if (elements.empty()) return "";
        std::string value = elements.back();
        elements.pop_back();
        return value;
    }

    std::string lindex(int index) const {
        if (index < 0) index = elements.size() + index;
        if (index < 0 || index >= static_cast<int>(elements.size())) return "";
        return elements[index];
    }

    int llen() const {
        return static_cast<int>(elements.size());
    }

    std::vector<std::string> lrange(int start, int end) const {
        if (elements.empty()) return {};
        
        if (start < 0) start = elements.size() + start;
        if (end < 0) end = elements.size() + end;
        
        start = std::max(0, start);
        end = std::min(static_cast<int>(elements.size()) - 1, end);
        
        if (start > end) return {};
        
        return std::vector<std::string>(elements.begin() + start, elements.begin() + end + 1);
    }
};

// Set data type
class SetType : public DataType {
private:
    std::unordered_set<std::string> elements;

public:
    ValueType get_type() const override {
        return ValueType::SET;
    }

    std::string serialize() const override {
        std::string result = "S";
        for (const auto& element : elements) {
            result += std::to_string(element.size()) + ":" + element + ",";
        }
        return result;
    }

    void deserialize(const std::string& data) override {
        elements.clear();
        if (data.empty() || data[0] != 'S') return;
        
        size_t pos = 1;
        while (pos < data.size()) {
            size_t colon_pos = data.find(':', pos);
            if (colon_pos == std::string::npos) break;
            
            int len = std::stoi(data.substr(pos, colon_pos - pos));
            std::string element = data.substr(colon_pos + 1, len);
            elements.insert(element);
            
            pos = colon_pos + len + 2; // Skip past element and comma
        }
    }

    std::string to_string() const override {
        std::string result = "{";
        bool first = true;
        for (const auto& element : elements) {
            if (!first) result += ", ";
            result += element;
            first = false;
        }
        result += "}";
        return result;
    }

    // Set operations
    bool sadd(const std::string& value) {
        auto [_, inserted] = elements.insert(value);
        return inserted;
    }

    bool sismember(const std::string& value) const {
        return elements.find(value) != elements.end();
    }

    bool srem(const std::string& value) {
        return elements.erase(value) > 0;
    }

    int scard() const {
        return static_cast<int>(elements.size());
    }

    std::vector<std::string> smembers() const {
        return std::vector<std::string>(elements.begin(), elements.end());
    }
};

// Hash data type (similar to Redis hashes)
class HashType : public DataType {
private:
    std::unordered_map<std::string, std::string> fields;

public:
    ValueType get_type() const override {
        return ValueType::HASH;
    }

    std::string serialize() const override {
        std::string result = "H";
        for (const auto& [field, value] : fields) {
            result += std::to_string(field.size()) + ":" + field + ":" + 
                      std::to_string(value.size()) + ":" + value + ",";
        }
        return result;
    }

    void deserialize(const std::string& data) override {
        fields.clear();
        if (data.empty() || data[0] != 'H') return;
        
        size_t pos = 1;
        while (pos < data.size()) {
            // Parse field
            size_t colon_pos1 = data.find(':', pos);
            if (colon_pos1 == std::string::npos) break;
            
            int field_len = std::stoi(data.substr(pos, colon_pos1 - pos));
            std::string field = data.substr(colon_pos1 + 1, field_len);
            
            // Parse value
            size_t colon_pos2 = data.find(':', colon_pos1 + field_len + 1);
            if (colon_pos2 == std::string::npos) break;
            
            int value_len = std::stoi(data.substr(colon_pos1 + field_len + 1, 
                                     colon_pos2 - (colon_pos1 + field_len + 1)));
            std::string value = data.substr(colon_pos2 + 1, value_len);
            
            fields[field] = value;
            pos = colon_pos2 + value_len + 2; // Skip past value and comma
        }
    }

    std::string to_string() const override {
        std::string result = "{";
        bool first = true;
        for (const auto& [field, value] : fields) {
            if (!first) result += ", ";
            result += field + ": " + value;
            first = false;
        }
        result += "}";
        return result;
    }

    // Hash operations
    bool hset(const std::string& field, const std::string& value) {
        bool is_new = fields.find(field) == fields.end();
        fields[field] = value;
        return is_new;
    }

    std::string hget(const std::string& field) const {
        auto it = fields.find(field);
        return (it != fields.end()) ? it->second : "";
    }

    bool hexists(const std::string& field) const {
        return fields.find(field) != fields.end();
    }

    bool hdel(const std::string& field) {
        return fields.erase(field) > 0;
    }

    int hlen() const {
        return static_cast<int>(fields.size());
    }

    std::vector<std::string> hkeys() const {
        std::vector<std::string> keys;
        keys.reserve(fields.size());
        for (const auto& [field, _] : fields) {
            keys.push_back(field);
        }
        return keys;
    }

    std::vector<std::string> hvals() const {
        std::vector<std::string> values;
        values.reserve(fields.size());
        for (const auto& [_, value] : fields) {
            values.push_back(value);
        }
        return values;
    }

    std::unordered_map<std::string, std::string> hgetall() const {
        return fields;
    }
};

// LRU Cache for key management
class LRUCache {
private:
    std::list<std::string> cache;
    std::unordered_map<std::string, std::list<std::string>::iterator> map;
    size_t max_size;

public:
    explicit LRUCache(size_t size) : max_size(size) {}

    void access(const std::string& key) {
        if (map.find(key) != map.end()) {
            cache.erase(map[key]);
        }
        cache.push_front(key);
        map[key] = cache.begin();

        if (cache.size() > max_size) {
            auto oldest = cache.back();
            map.erase(oldest);
            cache.pop_back();
        }
    }

    bool contains(const std::string& key) const {
        return map.find(key) != map.end();
    }

    void remove(const std::string& key) {
        if (map.find(key) != map.end()) {
            cache.erase(map[key]);
            map.erase(key);
        }
    }

    std::string get_oldest() const {
        if (cache.empty()) {
            throw std::runtime_error("Cache is empty");
        }
        return cache.back();
    }

    size_t size() const {
        return cache.size();
    }
};

// Bloom Filter for probabilistic key existence checking
class BloomFilter {
private:
    std::bitset<BLOOM_FILTER_SIZE> filter;
    
    size_t hash(const std::string& key) const {
        std::hash<std::string> hasher;
        return hasher(key) % BLOOM_FILTER_SIZE;
    }

public:
    void add(const std::string& key) {
        filter.set(hash(key));
    }

    bool contains(const std::string& key) const {
        return filter.test(hash(key));
    }
};

// Main database class supporting multiple data types
class BlinkDB {
private:
    std::unordered_map<std::string, std::unique_ptr<DataType>> store;
    LRUCache cache;
    BloomFilter bloom_filter;
    std::shared_mutex rw_lock;
    std::string persistence_file = "blinkdb_data.txt";
    
    void evict_if_needed() {
        if (cache.size() > CACHE_SIZE) {
            std::string oldest_key = cache.get_oldest();
            cache.remove(oldest_key);
            store.erase(oldest_key);
        }
    }

public:
    explicit BlinkDB() : cache(CACHE_SIZE) { load_from_disk(); }

    ~BlinkDB() { save_to_disk(); }

    // Basic operations
    void set(const std::string& key, const std::string& value) {
        std::unique_lock lock(rw_lock);
        auto string_value = std::make_unique<StringType>(value);
        store[key] = std::move(string_value);
        cache.access(key);
        evict_if_needed();
        bloom_filter.add(key);
    }

    std::string get(const std::string& key) {
        std::shared_lock lock(rw_lock);
        if (!bloom_filter.contains(key) || store.find(key) == store.end()) {
            return "NULL";
        }
        
        cache.access(key);
        
        // Check if the value is a string
        if (store[key]->get_type() == ValueType::STRING) {
            auto* string_value = dynamic_cast<StringType*>(store[key].get());
            return string_value->get();
        }
        
        return "WRONGTYPE Operation against a key holding the wrong kind of value";
    }

    void del(const std::string& key) {
        std::unique_lock lock(rw_lock);
        store.erase(key);
        cache.remove(key);
    }

    // Get type of a key
    std::string type(const std::string& key) {
        std::shared_lock lock(rw_lock);
        if (!bloom_filter.contains(key) || store.find(key) == store.end()) {
            return "none";
        }
        
        switch (store[key]->get_type()) {
            case ValueType::STRING: return "string";
            case ValueType::LIST: return "list";
            case ValueType::SET: return "set";
            case ValueType::HASH: return "hash";
            default: return "unknown";
        }
    }

    // List operations
    bool create_list_if_needed(const std::string& key) {
        if (store.find(key) == store.end()) {
            auto list_value = std::make_unique<ListType>();
            store[key] = std::move(list_value);
            bloom_filter.add(key);
            return true;
        } else if (store[key]->get_type() != ValueType::LIST) {
            return false;
        }
        return true;
    }

    std::string lpush(const std::string& key, const std::string& value) {
        std::unique_lock lock(rw_lock);
        
        if (!create_list_if_needed(key)) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        list->lpush(value);
        cache.access(key);
        evict_if_needed();
        
        return std::to_string(list->llen());
    }

    std::string rpush(const std::string& key, const std::string& value) {
        std::unique_lock lock(rw_lock);
        
        if (!create_list_if_needed(key)) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        list->rpush(value);
        cache.access(key);
        evict_if_needed();
        
        return std::to_string(list->llen());
    }

    std::string lpop(const std::string& key) {
        std::unique_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "NULL";
        }
        
        if (store[key]->get_type() != ValueType::LIST) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        std::string result = list->lpop();
        cache.access(key);
        
        if (list->llen() == 0) {
            store.erase(key);
            cache.remove(key);
        }
        
        return result.empty() ? "NULL" : result;
    }

    std::string rpop(const std::string& key) {
        std::unique_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "NULL";
        }
        
        if (store[key]->get_type() != ValueType::LIST) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        std::string result = list->rpop();
        cache.access(key);
        
        if (list->llen() == 0) {
            store.erase(key);
            cache.remove(key);
        }
        
        return result.empty() ? "NULL" : result;
    }

    std::string lindex(const std::string& key, int index) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "NULL";
        }
        
        if (store[key]->get_type() != ValueType::LIST) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        std::string result = list->lindex(index);
        cache.access(key);
        
        return result.empty() ? "NULL" : result;
    }

    std::string llen(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::LIST) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        cache.access(key);
        
        return std::to_string(list->llen());
    }

    std::string lrange(const std::string& key, int start, int end) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "*0\r\n";
        }
        
        if (store[key]->get_type() != ValueType::LIST) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* list = dynamic_cast<ListType*>(store[key].get());
        std::vector<std::string> results = list->lrange(start, end);
        cache.access(key);
        
        std::string response = "*" + std::to_string(results.size()) + "\r\n";
        for (const auto& item : results) {
            response += "$" + std::to_string(item.size()) + "\r\n" + item + "\r\n";
        }
        
        return response;
    }

    // Set operations
    bool create_set_if_needed(const std::string& key) {
        if (store.find(key) == store.end()) {
            auto set_value = std::make_unique<SetType>();
            store[key] = std::move(set_value);
            bloom_filter.add(key);
            return true;
        } else if (store[key]->get_type() != ValueType::SET) {
            return false;
        }
        return true;
    }

    std::string sadd(const std::string& key, const std::string& value) {
        std::unique_lock lock(rw_lock);
        
        if (!create_set_if_needed(key)) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* set = dynamic_cast<SetType*>(store[key].get());
        bool added = set->sadd(value);
        cache.access(key);
        evict_if_needed();
        
        return added ? "1" : "0";
    }

    std::string sismember(const std::string& key, const std::string& value) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::SET) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* set = dynamic_cast<SetType*>(store[key].get());
        bool is_member = set->sismember(value);
        cache.access(key);
        
        return is_member ? "1" : "0";
    }

    std::string srem(const std::string& key, const std::string& value) {
        std::unique_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::SET) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* set = dynamic_cast<SetType*>(store[key].get());
        bool removed = set->srem(value);
        cache.access(key);
        
        if (set->scard() == 0) {
            store.erase(key);
            cache.remove(key);
        }
        
        return removed ? "1" : "0";
    }

    std::string scard(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::SET) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* set = dynamic_cast<SetType*>(store[key].get());
        cache.access(key);
        
        return std::to_string(set->scard());
    }

    std::string smembers(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "*0\r\n";
        }
        
        if (store[key]->get_type() != ValueType::SET) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* set = dynamic_cast<SetType*>(store[key].get());
        std::vector<std::string> members = set->smembers();
        cache.access(key);
        
        std::string response = "*" + std::to_string(members.size()) + "\r\n";
        for (const auto& member : members) {
            response += "$" + std::to_string(member.size()) + "\r\n" + member + "\r\n";
        }
        
        return response;
    }

    // Hash operations
    bool create_hash_if_needed(const std::string& key) {
        if (store.find(key) == store.end()) {
            auto hash_value = std::make_unique<HashType>();
            store[key] = std::move(hash_value);
            bloom_filter.add(key);
            return true;
        } else if (store[key]->get_type() != ValueType::HASH) {
            return false;
        }
        return true;
    }

    std::string hset(const std::string& key, const std::string& field, const std::string& value) {
        std::unique_lock lock(rw_lock);
        
        if (!create_hash_if_needed(key)) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        bool added = hash->hset(field, value);
        cache.access(key);
        evict_if_needed();
        
        return added ? "1" : "0";
    }

    std::string hget(const std::string& key, const std::string& field) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "NULL";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        std::string result = hash->hget(field);
        cache.access(key);
        
        return result.empty() ? "NULL" : result;
    }

    std::string hexists(const std::string& key, const std::string& field) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        bool exists = hash->hexists(field);
        cache.access(key);
        
        return exists ? "1" : "0";
    }

    std::string hdel(const std::string& key, const std::string& field) {
        std::unique_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        bool removed = hash->hdel(field);
        cache.access(key);
        
        if (hash->hlen() == 0) {
            store.erase(key);
            cache.remove(key);
        }
        
        return removed ? "1" : "0";
    }

    std::string hlen(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "0";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        cache.access(key);
        
        return std::to_string(hash->hlen());
    }

    std::string hkeys(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "*0\r\n";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        std::vector<std::string> keys = hash->hkeys();
        cache.access(key);
        
        std::string response = "*" + std::to_string(keys.size()) + "\r\n";
        for (const auto& k : keys) {
            response += "$" + std::to_string(k.size()) + "\r\n" + k + "\r\n";
        }
        
        return response;
    }

    std::string hvals(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "*0\r\n";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        std::vector<std::string> values = hash->hvals();
        cache.access(key);
        
        std::string response = "*" + std::to_string(values.size()) + "\r\n";
        for (const auto& v : values) {
            response += "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
        }
        
        return response;
    }

std::string hgetall(const std::string& key) {
        std::shared_lock lock(rw_lock);
        
        if (store.find(key) == store.end()) {
            return "*0\r\n";
        }
        
        if (store[key]->get_type() != ValueType::HASH) {
            return "WRONGTYPE Operation against a key holding the wrong kind of value";
        }
        
        auto* hash = dynamic_cast<HashType*>(store[key].get());
        auto fields = hash->hgetall();
        cache.access(key);
        
        std::string response = "*" + std::to_string(fields.size() * 2) + "\r\n";
        for (const auto& [field, value] : fields) {
            response += "$" + std::to_string(field.size()) + "\r\n" + field + "\r\n"
                     +  "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
        }
        
        return response;
    }

    // Persistence operations
    void save_to_disk() {
        std::shared_lock lock(rw_lock);
        std::ofstream file(persistence_file);
        
        if (!file) {
            std::cerr << "Failed to open file for writing: " << persistence_file << std::endl;
            return;
        }
        
        for (const auto& [key, value] : store) {
            char type_char;
            switch (value->get_type()) {
                case ValueType::STRING: type_char = 'S'; break;
                case ValueType::LIST: type_char = 'L'; break;
                case ValueType::SET: type_char = 'E'; break;
                case ValueType::HASH: type_char = 'H'; break;
                default: continue;
            }
            
            file << type_char << " " << key << " " << value->serialize() << std::endl;
        }
        
        file.close();
    }

    void load_from_disk() {
        std::unique_lock lock(rw_lock);
        std::ifstream file(persistence_file);
        
        if (!file) {
            std::cerr << "No persistence file found or unable to open: " << persistence_file << std::endl;
            return;
        }
        
        std::string line;
        while (std::getline(file, line)) {
            if (line.empty() || line.size() < 3) continue;
            
            char type_char = line[0];
            size_t first_space = line.find(' ');
            size_t second_space = line.find(' ', first_space + 1);
            
            if (first_space == std::string::npos || second_space == std::string::npos) continue;
            
            std::string key = line.substr(first_space + 1, second_space - first_space - 1);
            std::string data = line.substr(second_space + 1);
            
            std::unique_ptr<DataType> value;
            
            switch (type_char) {
                case 'S': {
                    auto string_value = std::make_unique<StringType>();
                    string_value->deserialize(data);
                    value = std::move(string_value);
                    break;
                }
                case 'L': {
                    auto list_value = std::make_unique<ListType>();
                    list_value->deserialize(data);
                    value = std::move(list_value);
                    break;
                }
                case 'E': {
                    auto set_value = std::make_unique<SetType>();
                    set_value->deserialize(data);
                    value = std::move(set_value);
                    break;
                }
                case 'H': {
                    auto hash_value = std::make_unique<HashType>();
                    hash_value->deserialize(data);
                    value = std::move(hash_value);
                    break;
                }
                default:
                    continue;
            }
            
            store[key] = std::move(value);
            cache.access(key);
            bloom_filter.add(key);
        }
        
        file.close();
    }
};

// Protocol handler for parsing and processing Redis-like commands
class CommandHandler {
private:
    BlinkDB& db;

public:
    explicit CommandHandler(BlinkDB& database) : db(database) {}

    std::string process_command(const std::string& command_str) {
        std::vector<std::string> command_parts;
        std::istringstream iss(command_str);
        std::string part;
        
        while (iss >> part) {
            command_parts.push_back(part);
        }
        
        if (command_parts.empty()) {
            return "-ERR empty command\r\n";
        }
        
        std::string cmd = command_parts[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
        
        try {
            // String commands
            if (cmd == "set" && command_parts.size() >= 3) {
                db.set(command_parts[1], command_parts[2]);
                return "+OK\r\n";
            } else if (cmd == "get" && command_parts.size() >= 2) {
                std::string result = db.get(command_parts[1]);
                if (result == "NULL") {
                    return "$-1\r\n";
                } else if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return "$" + std::to_string(result.size()) + "\r\n" + result + "\r\n";
                }
            } else if (cmd == "del" && command_parts.size() >= 2) {
                db.del(command_parts[1]);
                return ":1\r\n";
            } else if (cmd == "type" && command_parts.size() >= 2) {
                std::string result = db.type(command_parts[1]);
                return "+" + result + "\r\n";
            }
            
            // List commands
            else if (cmd == "lpush" && command_parts.size() >= 3) {
                std::string result = db.lpush(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "rpush" && command_parts.size() >= 3) {
                std::string result = db.rpush(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "lpop" && command_parts.size() >= 2) {
                std::string result = db.lpop(command_parts[1]);
                if (result == "NULL") {
                    return "$-1\r\n";
                } else if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return "$" + std::to_string(result.size()) + "\r\n" + result + "\r\n";
                }
            } else if (cmd == "rpop" && command_parts.size() >= 2) {
                std::string result = db.rpop(command_parts[1]);
                if (result == "NULL") {
                    return "$-1\r\n";
                } else if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return "$" + std::to_string(result.size()) + "\r\n" + result + "\r\n";
                }
            } else if (cmd == "lindex" && command_parts.size() >= 3) {
                int index = std::stoi(command_parts[2]);
                std::string result = db.lindex(command_parts[1], index);
                if (result == "NULL") {
                    return "$-1\r\n";
                } else if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return "$" + std::to_string(result.size()) + "\r\n" + result + "\r\n";
                }
            } else if (cmd == "llen" && command_parts.size() >= 2) {
                std::string result = db.llen(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "lrange" && command_parts.size() >= 4) {
                int start = std::stoi(command_parts[2]);
                int end = std::stoi(command_parts[3]);
                std::string result = db.lrange(command_parts[1], start, end);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return result;
                }
            }
            
            // Set commands
            else if (cmd == "sadd" && command_parts.size() >= 3) {
                std::string result = db.sadd(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "sismember" && command_parts.size() >= 3) {
                std::string result = db.sismember(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "srem" && command_parts.size() >= 3) {
                std::string result = db.srem(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "scard" && command_parts.size() >= 2) {
                std::string result = db.scard(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "smembers" && command_parts.size() >= 2) {
                std::string result = db.smembers(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return result;
                }
            }
            
            // Hash commands
            else if (cmd == "hset" && command_parts.size() >= 4) {
                std::string result = db.hset(command_parts[1], command_parts[2], command_parts[3]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "hget" && command_parts.size() >= 3) {
                std::string result = db.hget(command_parts[1], command_parts[2]);
                if (result == "NULL") {
                    return "$-1\r\n";
                } else if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return "$" + std::to_string(result.size()) + "\r\n" + result + "\r\n";
                }
            } else if (cmd == "hexists" && command_parts.size() >= 3) {
                std::string result = db.hexists(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "hdel" && command_parts.size() >= 3) {
                std::string result = db.hdel(command_parts[1], command_parts[2]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "hlen" && command_parts.size() >= 2) {
                std::string result = db.hlen(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return ":" + result + "\r\n";
                }
            } else if (cmd == "hkeys" && command_parts.size() >= 2) {
                std::string result = db.hkeys(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return result;
                }
            } else if (cmd == "hvals" && command_parts.size() >= 2) {
                std::string result = db.hvals(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return result;
                }
            } else if (cmd == "hgetall" && command_parts.size() >= 2) {
                std::string result = db.hgetall(command_parts[1]);
                if (result.substr(0, 9) == "WRONGTYPE") {
                    return "-" + result + "\r\n";
                } else {
                    return result;
                }
            }
            
            // Ping command for testing connection
            else if (cmd == "ping") {
                return "+PONG\r\n";
            }
            
            else {
                return "-ERR unknown command '" + cmd + "'\r\n";
            }
        } catch (const std::exception& e) {
            return "-ERR " + std::string(e.what()) + "\r\n";
        }
    }
};

// Set socket to non-blocking mode
void set_nonblocking(int socket) {
    int flags = fcntl(socket, F_GETFL, 0);
    fcntl(socket, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    // Create socket
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        std::cerr << "Failed to create socket" << std::endl;
        return 1;
    }
    
    // Allow port reuse
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        std::cerr << "Failed to set socket options" << std::endl;
        close(server_socket);
        return 1;
    }
    
    // Bind socket to port
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);
    
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        std::cerr << "Failed to bind socket" << std::endl;
        close(server_socket);
        return 1;
    }
    
    // Listen for connections
    if (listen(server_socket, 10) == -1) {
        std::cerr << "Failed to listen on socket" << std::endl;
        close(server_socket);
        return 1;
    }
    
    // Set server socket to non-blocking mode
    set_nonblocking(server_socket);
    
    // Create epoll instance
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        std::cerr << "Failed to create epoll instance" << std::endl;
        close(server_socket);
        return 1;
    }
    
    // Add server socket to epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = server_socket;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_socket, &ev) == -1) {
        std::cerr << "Failed to add server socket to epoll" << std::endl;
        close(server_socket);
        close(epoll_fd);
        return 1;
    }
    
    // Initialize database and command handler
    BlinkDB db;
    CommandHandler handler(db);
    
    std::cout << "BlinkDB server started on port " << PORT << std::endl;
    
    // Event loop
    struct epoll_event events[MAX_EVENTS];
    std::unordered_map<int, std::string> client_buffers;
    
    while (true) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        
        for (int i = 0; i < num_events; ++i) {
            int fd = events[i].data.fd;
            
            // New connection
            if (fd == server_socket) {
                struct sockaddr_in client_addr;
                socklen_t client_addr_len = sizeof(client_addr);
                int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_addr_len);
                
                if (client_socket == -1) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        std::cerr << "Failed to accept connection" << std::endl;
                    }
                    continue;
                }
                
                set_nonblocking(client_socket);
                
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = client_socket;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_socket, &ev) == -1) {
                    std::cerr << "Failed to add client socket to epoll" << std::endl;
                    close(client_socket);
                    continue;
                }
                
                client_buffers[client_socket] = "";
                std::cout << "New client connected: " << client_socket << std::endl;
            }
            // Client data
            else {
                if (events[i].events & EPOLLIN) {
                    char buffer[BUFFER_SIZE];
                    ssize_t bytes_read;
                    
                    while ((bytes_read = read(fd, buffer, BUFFER_SIZE)) > 0) {
                        client_buffers[fd].append(buffer, bytes_read);
                    }
                    
                    if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
                        std::cerr << "Error reading from client: " << fd << std::endl;
                        close(fd);
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                        client_buffers.erase(fd);
                    } else if (bytes_read == 0) {
                        // Client disconnected
                        std::cout << "Client disconnected: " << fd << std::endl;
                        close(fd);
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                        client_buffers.erase(fd);
                    } else {
                        // Process commands in buffer
                        size_t pos = 0;
                        while ((pos = client_buffers[fd].find("\r\n")) != std::string::npos) {
                            std::string command = client_buffers[fd].substr(0, pos);
                            client_buffers[fd].erase(0, pos + 2);
                            
                            if (!command.empty()) {
                                std::string response = handler.process_command(command);
                                write(fd, response.c_str(), response.size());
                            }
                        }
                    }
                }
            }
        }
    }
    
    close(server_socket);
    close(epoll_fd);
    
    return 0;
}