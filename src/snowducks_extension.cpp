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
		result->cache_table_name = "t_" + generate_sha256_hash(query_without_limit);

		// --- DuckLake Integration Logic ---
		Connection con(*context.db);
		
		try {
			// 1. Install and Load DuckLake
			con.Query("INSTALL ducklake; LOAD ducklake;");

			// 2. Attach to the PostgreSQL metadata backend
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
			
			// Detach if already attached, ignore error if not.
			try { con.Query("DETACH metadata;"); } catch (...) {}
			
			con.Query(attach_sql);
			con.Query("USE metadata;");
			
			if (result->debug) {
				std::cout << "DEBUG: Successfully attached to DuckLake metadata." << std::endl;
			}

			// 3. Check if table exists in DuckLake metadata and get its schema
			result->is_cached = false;
			auto check_query = "DESCRIBE " + result->cache_table_name + ";";
			auto presult = con.Query(check_query);
			if (presult && !presult->HasError() && presult->collection.Count() > 0) {
				result->is_cached = true;
				if (result->debug) {
					std::cout << "DEBUG: Cache check successful. Table '" << result->cache_table_name << "' exists in metadata." << std::endl;
				}
                // Bind the return types and names from the cached table
                for (auto &row : presult->collection) {
                    string col_name = row.GetValue(0).ToString();
                    string col_type_str = row.GetValue(1).ToString();
                    names.push_back(col_name);
                    // This is a simplification. A full type parser would be needed for complex types.
                    return_types.push_back(TransformStringToLogicalType(col_type_str));
                }
                result->column_names = names;
                result->column_types = return_types;
			}
		} catch (const std::exception& e) {
			if (result->debug) {
				std::cout << "DEBUG: Failed to query metadata or table does not exist. Exception: " << e.what() << std::endl;
			}
		}
		
		// If not cached, we can't know the schema. Return a status message.
        if (!result->is_cached) {
            names.push_back("status");
            return_types.push_back(LogicalType::VARCHAR);
        }

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
		
		if (!data.is_cached || data.force_refresh) {
			string msg = data.force_refresh ? "Cache refresh forced. " : "Cache miss. ";
			msg += "Please use the Python CLI to populate the cache.";
			output.SetCardinality(1);
			output.SetValue(0, 0, Value(msg));
			gstate.finished = true;
			return;
		}

		// --- This part is unsafe and will be replaced ---
		// Running a SELECT query inside a table function for the same connection
		// can lead to deadlocks. For this milestone, we return a message.
		string msg = "Table is cached: " + data.cache_table_name + ". Query it directly from 'metadata' DB for now.";
		output.SetCardinality(1);
		output.SetValue(0, 0, Value(msg));
		// --- End unsafe part ---

		gstate.finished = true;
	}
};

static void LoadInternal(DatabaseInstance &instance) {
    // Register the table function
    auto snowducks_table_func = SnowducksTableFunction();
    ExtensionUtil::RegisterTableFunction(instance, snowducks_table_func);

	// Register scalar functions
    ExtensionUtil::RegisterFunction(instance, ScalarFunction("snowducks_normalize_query", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksNormalizeQueryText));
    ExtensionUtil::RegisterFunction(instance, ScalarFunction("snowducks_cache_table_name", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksGenerateCacheTableName));
}

void SnowducksExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string SnowducksExtension::Name() {
	return "snowducks";
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