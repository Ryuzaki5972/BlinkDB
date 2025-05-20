# BlinkDB

BlinkDB is a high-performance, in-memory database system inspired by Redis that supports multiple data types and provides a Redis-compatible protocol interface. This project implements a lightweight yet powerful database server written in C++ with support for strings, lists, sets, and hash data structures.

## Features

### Core Database Features
- **Multiple Data Types**: Support for strings, lists, sets, and hash maps
- **In-Memory Storage**: Fast data access with in-memory storage
- **Persistence**: Automatic data persistence to disk
- **LRU Cache**: Efficient memory management with Least Recently Used (LRU) eviction policy
- **Bloom Filter**: Probabilistic data structure for quick key existence checks
- **Concurrent Access**: Thread-safe operations with read-write locks

### Data Types and Operations

#### String Operations
- `SET`: Store a string value
- `GET`: Retrieve a string value
- `DEL`: Delete a key
- `TYPE`: Get the type of a key

#### List Operations
- `LPUSH`/`RPUSH`: Add an element to the beginning/end of a list
- `LPOP`/`RPOP`: Remove and return an element from the beginning/end of a list
- `LINDEX`: Get an element by index
- `LLEN`: Get the length of a list
- `LRANGE`: Get a range of elements

#### Set Operations
- `SADD`: Add a member to a set
- `SISMEMBER`: Check if a value is a member of a set
- `SREM`: Remove a member from a set
- `SCARD`: Get the number of members in a set
- `SMEMBERS`: Get all members of a set

#### Hash Operations
- `HSET`: Set a field in a hash
- `HGET`: Get a field from a hash
- `HEXISTS`: Check if a field exists in a hash
- `HDEL`: Delete a field from a hash
- `HLEN`: Get the number of fields in a hash
- `HKEYS`: Get all field names in a hash
- `HVALS`: Get all values in a hash
- `HGETALL`: Get all fields and values in a hash

## Building and Running

### Prerequisites
- C++17 compatible compiler
- Linux-based operating system (uses epoll)
- Standard development libraries

### Compilation
```bash
g++ -std=c++17 -o blinkdb ABD.cpp -pthread
```

### Running the Server
```bash
./blinkdb
```
The server will start on port 9001 by default.

## Connecting to BlinkDB

You can connect to BlinkDB using any Redis client by pointing it to the server's address and port:

```bash
redis-cli -p 9001
```

Or you can use a simple telnet connection:

```bash
telnet localhost 9001
```

## Example Usage

### String Examples
```
SET username "john_doe"
GET username
SET counter 100
SET user_profile "{\"name\":\"John\",\"age\":30,\"city\":\"New York\"}"
```

### List Examples
```
LPUSH notifications "New friend request"
RPUSH notifications "Payment received"
LRANGE notifications 0 -1
LLEN notifications
LINDEX notifications 0
```

### Set Examples
```
SADD interests "programming"
SADD interests "music"
SADD interests "travel"
SMEMBERS interests
SISMEMBER interests "programming"
```

### Hash Examples
```
HSET user:1000 username "johndoe"
HSET user:1000 email "john@example.com"
HSET user:1000 age "30"
HGETALL user:1000
HGET user:1000 email
```

## Architecture

BlinkDB is built with a modular architecture:

- **DataType**: Abstract base class for all data types
- **StringType, ListType, SetType, HashType**: Concrete implementations of data types
- **LRUCache**: Manages key eviction based on usage
- **BloomFilter**: Provides quick membership tests
- **BlinkDB**: Main database class that manages data storage and operations
- **CommandHandler**: Parses and processes Redis-compatible commands
- **Main**: Sets up the server socket and event loop

                
                                                 +---------------------+
                                                 |        Main         |
                                                 | (Server & Event Loop)|
                                                 +----------+----------+
                                                            |
                                                            | initializes
                                                            v
                                +------------+    +--------------------+    +--------------+
                                | BloomFilter|<-->|      BlinkDB       |<-->|  LRUCache    |
                                | (Key Check)|    | (Database Engine)  |    |(Key Eviction)|
                                +------------+    +--------+-----------+    +--------------+
                                                           |
                                                           | uses
                                                           v
                                                  +-------------------+
                                                  |  CommandHandler   |
                                                  | (Protocol Parser) |
                                                  +--------+----------+
                                                           |
                                                           | operates on
                                                           v
                                                  +-------------------+
                                                  |     DataType      |
                                                  |  (Abstract Base)  |
                                                  +-------------------+
                                                           |
                                                           | implemented by
                                                           |
                                                           v
                                        +------------+------------+------------+
                                        |            |            |            |            
                                        v            v            v            v            
                                 +-----------+ +-----------+ +-----------+ +-----------+
                                 |StringType | | ListType  | | SetType   | | HashType  |
                                 |           | |           | |           | |           |
                                 +-----------+ +-----------+ +-----------+ +-----------+



## Performance Considerations

- The database uses an LRU cache to manage memory usage, automatically evicting the least recently used keys when memory limits are reached.
- Bloom filters are used to quickly determine if a key might exist, reducing unnecessary lookups.
- Read-write locks ensure thread safety while allowing concurrent reads.
- Non-blocking I/O with epoll enables handling thousands of connections efficiently.

## Persistence

BlinkDB automatically saves data to disk in a file named `blinkdb_data.txt`. The data is loaded when the server starts and saved when it shuts down. The persistence format is simple and human-readable.

## Limitations

- Currently only supports a subset of Redis commands
- Limited to single-instance operation (no clustering)
- No transaction support
- No pub/sub messaging

## Future Enhancements

- Support for more Redis commands
- Cluster mode for distributed operation
- Transaction support
- Pub/sub messaging system
- Scripting support
- Time-to-live (TTL) for keys
