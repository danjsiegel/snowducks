#define DUCKDB_EXTENSION_MAIN

#include "snowducks_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <openssl/evp.h>
#include <sstream>
#include <iomanip>
#include <fstream>
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include <iostream>


namespace duckdb {

// Utility function to convert string to lowercase
string to_lowercase(const string &input) {
	string result = input;
	std::transform(result.begin(), result.end(), result.begin(), ::tolower);
	return result;
}

// Generate SHA256 hash and take the first 16 hex characters
string generate_sha256_hash(const string &input) {
	EVP_MD_CTX* context = EVP_MD_CTX_new();
	EVP_DigestInit_ex(context, EVP_sha256(), NULL);
	EVP_DigestUpdate(context, input.c_str(), input.length());
	
	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int lengthOfHash = 0;
	EVP_DigestFinal_ex(context, hash, &lengthOfHash);
	EVP_MD_CTX_free(context);
	
	std::ostringstream oss;
	oss << std::hex << std::setfill('0');
	for (unsigned int i = 0; i < lengthOfHash; i++) {
		oss << std::setw(2) << static_cast<int>(hash[i]);
	}
	return oss.str().substr(0, 16);
}

// Normalize query text
inline void SnowducksNormalizeQueryText(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t query) {
		string normalized = to_lowercase(query.GetString());
		std::istringstream iss(normalized);
		std::ostringstream oss;
		string word;
		bool first = true;
		while (iss >> word) {
			if (!first) oss << " ";
			oss << word;
			first = false;
		}
		return StringVector::AddString(result, oss.str());
	});
}

// Generate cache table name from query
inline void SnowducksGenerateCacheTableName(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t query) {
		// Normalization logic is the same as SnowducksNormalizeQueryText
		string normalized = to_lowercase(query.GetString());
		std::istringstream iss(normalized);
		std::ostringstream oss;
		string word;
		bool first = true;
		while (iss >> word) {
			if (!first) oss << " ";
			oss << word;
			first = false;
		}
		string clean_query = oss.str();
		
		string hash = generate_sha256_hash(clean_query);
		string table_name = "t_" + hash;
		
		return StringVector::AddString(result, table_name);
	});
}

// Simple info function for UI and testing
inline void SnowducksInfo(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Snowducks " + name.GetString() + " 🦆");
	});
}

// Helper function to convert DuckDB type strings to LogicalTypes
LogicalType parse_duckdb_type(const string &type_str) {
	string type_lower = to_lowercase(type_str);
	
	if (type_lower == "varchar" || type_lower.find("varchar") != string::npos) {
		return LogicalType::VARCHAR;
	}
	if (type_lower == "integer" || type_lower == "int" || type_lower == "int32") {
		return LogicalType::INTEGER;
	}
	if (type_lower == "bigint" || type_lower == "int64") {
		return LogicalType::BIGINT;
	}
	if (type_lower == "smallint" || type_lower == "int16") {
		return LogicalType::SMALLINT;
	}
	if (type_lower == "tinyint" || type_lower == "int8") {
		return LogicalType::TINYINT;
	}
	if (type_lower == "double" || type_lower == "float8") {
		return LogicalType::DOUBLE;
	}
	if (type_lower == "real" || type_lower == "float" || type_lower == "float4") {
		return LogicalType::FLOAT;
	}
	if (type_lower == "boolean" || type_lower == "bool") {
		return LogicalType::BOOLEAN;
	}
	if (type_lower == "date") {
		return LogicalType::DATE;
	}
	if (type_lower == "timestamp" || type_lower.find("timestamp") != string::npos) {
		return LogicalType::TIMESTAMP;
	}
	if (type_lower.find("decimal") != string::npos || type_lower.find("numeric") != string::npos) {
		return LogicalType::DECIMAL(18, 2); // Default precision/scale
	}
	
	// Default to VARCHAR for unknown types
	return LogicalType::VARCHAR;
}

struct SnowducksGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

class SnowducksTableFunction : public TableFunction {
public:
	SnowducksTableFunction() : TableFunction("snowducks_table", 
		{LogicalType::VARCHAR}, // SQL query parameter (required)
		SnowducksTableFunc, SnowducksTableBind, SnowducksTableInit) {
		named_parameters["limit"] = LogicalType::INTEGER;
		named_parameters["force_refresh"] = LogicalType::BOOLEAN;
		named_parameters["debug"] = LogicalType::BOOLEAN;
	}

private:
	struct SnowducksBindData : public TableFunctionData {
		string original_query;
		string cache_table_name;
		int limit;
		bool force_refresh;
		bool debug;
		bool is_cached; // Flag to indicate if the table is cached in DuckLake
		vector<LogicalType> column_types;
        vector<string> column_names;
	};

	static unique_ptr<FunctionData> SnowducksTableBind(ClientContext &context, TableFunctionBindInput &input,
	                                                   vector<LogicalType> &return_types, vector<string> &names) {

		auto result = make_uniq<SnowducksBindData>();
		result->original_query = input.inputs[0].GetValue<string>();

		// Handle named parameters
		result->limit = 1000; // Default limit
		result->force_refresh = false;
		result->debug = false;

		for (auto &kv : input.named_parameters) {
			string key_lower = to_lowercase(kv.first);
			if (key_lower == "limit") {
				result->limit = kv.second.GetValue<int32_t>();
			} else if (key_lower == "force_refresh") {
				result->force_refresh = kv.second.GetValue<bool>();
			} else if (key_lower == "debug") {
				result->debug = kv.second.GetValue<bool>();
			}
		}
		
		if (result->debug) {
			std::cout << "DEBUG: Binding with query: " << result->original_query << std::endl;
		}

		// Generate cache table name (without limit clause for caching)
		string query_without_limit = result->original_query;
		size_t limit_pos = to_lowercase(query_without_limit).find(" limit ");
		if (limit_pos != string::npos) {
			query_without_limit = query_without_limit.substr(0, limit_pos);
		}
		
		// Normalize the query the same way as the scalar function
		string normalized = to_lowercase(query_without_limit);
		std::istringstream iss(normalized);
		std::ostringstream oss;
		string word;
		bool first = true;
		while (iss >> word) {
			if (!first) oss << " ";
			oss << word;
			first = false;
		}
		string clean_query = oss.str();
		
		result->cache_table_name = "t_" + generate_sha256_hash(clean_query);
		
		if (result->debug) {
			std::cout << "DEBUG: Generated cache table name: " << result->cache_table_name << std::endl;
		}

		// --- Cache Detection Logic ---
		// We'll use a simple approach: try to connect to the metadata database
		// and check if the table exists. If it does, we'll set up to return that data.
		
		result->is_cached = false;
		
		// Try to check if the cache table exists by attempting a connection
		// This is done during bind time to determine the schema
		try {
			// Create a separate database instance for cache checking
			DuckDB cache_check_db(":memory:");
			Connection cache_check_conn(cache_check_db);
			
			// Install and load ducklake
			cache_check_conn.Query("INSTALL ducklake; LOAD ducklake;");
			
			// Get connection parameters from environment
			const char* data_path_env = std::getenv("DUCKLAKE_DATA_PATH");
			string data_path = data_path_env ? string(data_path_env) : string(std::getenv("HOME")) + "/.snowducks/data";
			const char* pg_host = std::getenv("PG_HOST") ? std::getenv("PG_HOST") : "localhost";
			const char* pg_port = std::getenv("PG_PORT") ? std::getenv("PG_PORT") : "5432";
			const char* pg_db = std::getenv("PG_DB") ? std::getenv("PG_DB") : "snowducks_metadata";
			const char* pg_user = std::getenv("PG_USER") ? std::getenv("PG_USER") : "snowducks_user";
			const char* pg_pass = std::getenv("PG_PASS") ? std::getenv("PG_PASS") : "snowducks_password";

			string attach_sql = "ATTACH 'ducklake:postgres:host=" + string(pg_host) +
								" port=" + string(pg_port) +
								" dbname=" + string(pg_db) +
								" user=" + string(pg_user) +
								" password=" + string(pg_pass) +
								"' AS metadata (DATA_PATH '" + data_path + "');";
			
			auto attach_result = cache_check_conn.Query(attach_sql);
			if (!attach_result->HasError()) {
				cache_check_conn.Query("USE metadata;");
				
				// Try to describe the table to get its schema
				string describe_sql = "DESCRIBE " + result->cache_table_name + ";";
				auto describe_result = cache_check_conn.Query(describe_sql);
				
				if (!describe_result->HasError() && describe_result->RowCount() > 0) {
					result->is_cached = true;
					
					if (result->debug) {
						std::cout << "DEBUG: Found cached table " << result->cache_table_name << " with " << describe_result->RowCount() << " columns" << std::endl;
					}
					
					// Extract column information from DESCRIBE result
					while (true) {
						auto chunk = describe_result->Fetch();
						if (!chunk || chunk->size() == 0) break;
						
						for (idx_t i = 0; i < chunk->size(); i++) {
							string col_name = chunk->GetValue(0, i).ToString();
							string col_type = chunk->GetValue(1, i).ToString();
							
							names.push_back(col_name);
							return_types.push_back(parse_duckdb_type(col_type));
							
							if (result->debug) {
								std::cout << "DEBUG: Column: " << col_name << " -> " << col_type << std::endl;
							}
						}
					}
				}
			}
		} catch (const std::exception& e) {
			if (result->debug) {
				std::cout << "DEBUG: Cache check failed: " << e.what() << std::endl;
			}
		}
		
		// If no cached table found, provide a simple schema for status messages
		if (!result->is_cached) {
			names.push_back("message");
			return_types.push_back(LogicalType::VARCHAR);
		}
		
		result->column_names = names;
		result->column_types = return_types;

		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> SnowducksTableInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<GlobalTableFunctionState>();
	}

	static void SnowducksTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &data = (SnowducksBindData &)*data_p.bind_data;
		auto &gstate = (SnowducksGlobalState &)*data_p.global_state;

		if (gstate.finished) {
			return;
		}
		
		// If the table is cached, try to return the actual data
		if (data.is_cached && !data.force_refresh) {
			try {
				// Create a separate database instance for data retrieval
				DuckDB data_db(":memory:");
				Connection data_conn(data_db);
				
				// Install and load ducklake
				data_conn.Query("INSTALL ducklake; LOAD ducklake;");
				
				// Get connection parameters from environment
				const char* data_path_env = std::getenv("DUCKLAKE_DATA_PATH");
				string data_path = data_path_env ? string(data_path_env) : string(std::getenv("HOME")) + "/.snowducks/data";
				const char* pg_host = std::getenv("PG_HOST") ? std::getenv("PG_HOST") : "localhost";
				const char* pg_port = std::getenv("PG_PORT") ? std::getenv("PG_PORT") : "5432";
				const char* pg_db = std::getenv("PG_DB") ? std::getenv("PG_DB") : "snowducks_metadata";
				const char* pg_user = std::getenv("PG_USER") ? std::getenv("PG_USER") : "snowducks_user";
				const char* pg_pass = std::getenv("PG_PASS") ? std::getenv("PG_PASS") : "snowducks_password";

				string attach_sql = "ATTACH 'ducklake:postgres:host=" + string(pg_host) +
									" port=" + string(pg_port) +
									" dbname=" + string(pg_db) +
									" user=" + string(pg_user) +
									" password=" + string(pg_pass) +
									"' AS metadata (DATA_PATH '" + data_path + "');";
				
				auto attach_result = data_conn.Query(attach_sql);
				if (!attach_result->HasError()) {
					data_conn.Query("USE metadata;");
					
					// Query the cached data with the specified limit
					string query_sql = "SELECT * FROM " + data.cache_table_name + " LIMIT " + std::to_string(data.limit) + ";";
					
					if (data.debug) {
						std::cout << "DEBUG: Executing query: " << query_sql << std::endl;
					}
					
					auto query_result = data_conn.Query(query_sql);
					
					if (!query_result->HasError()) {
						auto chunk = query_result->Fetch();
						if (chunk && chunk->size() > 0) {
							// Copy the data to the output
							output.SetCardinality(chunk->size());
							for (idx_t col_idx = 0; col_idx < chunk->ColumnCount() && col_idx < output.ColumnCount(); col_idx++) {
								output.data[col_idx].Reference(chunk->data[col_idx]);
							}
							
							if (data.debug) {
								std::cout << "DEBUG: Returned " << chunk->size() << " rows from cached table" << std::endl;
							}
							
							gstate.finished = true;
							return;
						}
					} else {
						if (data.debug) {
							std::cout << "DEBUG: Query failed: " << query_result->GetError() << std::endl;
						}
					}
				} else {
					if (data.debug) {
						std::cout << "DEBUG: Attach failed: " << attach_result->GetError() << std::endl;
					}
				}
			} catch (const std::exception& e) {
				if (data.debug) {
					std::cout << "DEBUG: Data retrieval failed: " << e.what() << std::endl;
				}
			}
		}
		
		// Fallback: return instructions or error messages
		string msg;
		
		if (data.force_refresh) {
			msg = "Force refresh requested. Run: python -m snowducks.cli '" + data.original_query + "' --force";
		} else if (!data.is_cached) {
			msg = "Cache miss for table " + data.cache_table_name + ". Run: python -m snowducks.cli '" + data.original_query + "'";
		} else {
			msg = "Failed to retrieve cached data for table " + data.cache_table_name + ". Check logs for details.";
		}
		
		output.SetCardinality(1);
		output.SetValue(0, 0, Value(msg));
		gstate.finished = true;
	}
};

static void LoadInternal(DatabaseInstance &instance) {
    // Register the table function
    auto snowducks_table_func = SnowducksTableFunction();
    ExtensionUtil::RegisterFunction(instance, snowducks_table_func);

	// Register scalar functions
    ExtensionUtil::RegisterFunction(instance, ScalarFunction("snowducks_normalize_query", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksNormalizeQueryText));
    ExtensionUtil::RegisterFunction(instance, ScalarFunction("snowducks_cache_table_name", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksGenerateCacheTableName));
    ExtensionUtil::RegisterFunction(instance, ScalarFunction("snowducks_info", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksInfo));
}

void SnowducksExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string SnowducksExtension::Name() {
	return "snowducks";
}

std::string SnowducksExtension::Version() const {
#ifdef EXT_VERSION_SNOWDUCKS
	return EXT_VERSION_SNOWDUCKS;
#else
	return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void snowducks_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SnowducksExtension>();
}

DUCKDB_EXTENSION_API const char *snowducks_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}