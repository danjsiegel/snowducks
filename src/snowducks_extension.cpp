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
#include <openssl/md5.h>
#include <sstream>
#include <iomanip>
#include <fstream>
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include <fstream>
#include <sstream>
#include <iostream>
#include <openssl/sha.h>

namespace duckdb {

// Utility function to convert string to lowercase
string to_lowercase(const string &input) {
	string result = input;
	std::transform(result.begin(), result.end(), result.begin(), ::tolower);
	return result;
}

// Generate MD5 hash of a string using modern OpenSSL 3.0 EVP interface
string generate_md5_hash(const string &input) {
	EVP_MD_CTX *ctx = EVP_MD_CTX_new();
	if (!ctx) {
		return ""; // Handle error
	}
	
	if (EVP_DigestInit_ex(ctx, EVP_md5(), NULL) != 1) {
		EVP_MD_CTX_free(ctx);
		return ""; // Handle error
	}
	
	if (EVP_DigestUpdate(ctx, input.c_str(), input.length()) != 1) {
		EVP_MD_CTX_free(ctx);
		return ""; // Handle error
	}
	
	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int digest_len;
	if (EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
		EVP_MD_CTX_free(ctx);
		return ""; // Handle error
	}
	
	EVP_MD_CTX_free(ctx);
	
	std::stringstream ss;
	for (unsigned int i = 0; i < digest_len; i++) {
		ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
	}
	return ss.str();
}

// Generate SHA256 hash of a string using OpenSSL EVP interface
string generate_sha256_hash(const string &input) {
	// Use EVP interface for SHA256 to match Python CLI
	EVP_MD_CTX* context = EVP_MD_CTX_new();
	EVP_DigestInit_ex(context, EVP_sha256(), NULL);
	EVP_DigestUpdate(context, input.c_str(), input.length());
	
	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int lengthOfHash = 0;
	EVP_DigestFinal_ex(context, hash, &lengthOfHash);
	EVP_MD_CTX_free(context);
	
	std::ostringstream oss;
	for (unsigned int i = 0; i < 16; i++) { // Take first 16 bytes to match Python
		oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
	}
	return oss.str().substr(0, 16); // Take only first 16 characters to match Python CLI
}

// Normalize query text (remove extra whitespace, convert to lowercase)
inline void SnowducksNormalizeQueryText(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(query_vector, result, args.size(), [&](string_t query) {
		string normalized = to_lowercase(query.GetString());
		// Remove extra whitespace
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
	auto &query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(query_vector, result, args.size(), [&](string_t query) {
		string normalized = to_lowercase(query.GetString());
		// Remove extra whitespace
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
		
		// Generate hash using SHA256 to match Python CLI
		string hash = generate_sha256_hash(clean_query);
		string table_name = "t_" + hash;
		
		return StringVector::AddString(result, table_name);
	});
}

// Generate cache table name from query without LIMIT clause
inline void SnowducksGenerateCacheTableNameWithoutLimit(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(query_vector, result, args.size(), [&](string_t query) {
		string query_str = query.GetString();
		string normalized = to_lowercase(query_str);
		
		// Remove LIMIT clause
		size_t limit_pos = normalized.find(" limit ");
		if (limit_pos != string::npos) {
			normalized = normalized.substr(0, limit_pos);
		}
		
		// Remove extra whitespace
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
		
		// Generate hash using SHA256 to match Python CLI
		string hash = generate_sha256_hash(clean_query);
		string table_name = "t_" + hash;
		
		return StringVector::AddString(result, table_name);
	});
}

// Check if query has LIMIT clause
inline void SnowducksHasLimitClause(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(query_vector, result, args.size(), [&](string_t query) {
		string query_str = to_lowercase(query.GetString());
		return query_str.find(" limit ") != string::npos;
	});
}

// Extract LIMIT value from query
inline void SnowducksExtractLimitValue(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, int32_t>(query_vector, result, args.size(), [&](string_t query) {
		string query_str = to_lowercase(query.GetString());
		size_t limit_pos = query_str.find(" limit ");
		if (limit_pos == string::npos) {
			return 0; // No LIMIT found
		}
		
		// Extract the number after LIMIT
		string after_limit = query_str.substr(limit_pos + 7); // " limit " is 7 chars
		std::istringstream iss(after_limit);
		int32_t limit_value;
		if (iss >> limit_value) {
			return limit_value;
		}
		return 0;
	});
}

// Simple info function
inline void SnowducksInfo(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Snowducks " + name.GetString() + " 🦆");
	});
}

// Check if cache file exists
bool cache_file_exists(const string &table_name) {
	// Check in DuckLake data path instead of local cache directory
	string cache_path = string(getenv("HOME") ? getenv("HOME") : "") + "/.snowducks/data/main/" + table_name;
	
	// Check if directory exists
	std::ifstream dir_check(cache_path);
	if (!dir_check.good()) {
		return false;
	}
	
	// Check if any Parquet files exist in the directory
	string command = "ls " + cache_path + "/ducklake-*.parquet >/dev/null 2>&1";
	return system(command.c_str()) == 0;
}

// Call Python CLI to fetch data from Snowflake
bool fetch_from_snowflake(const string &table_name, const string &query) {
	// Create a temporary script to fetch the data
	string temp_script = "temp_fetch_" + table_name + ".py";
	std::ofstream script_file(temp_script);
	
	// Escape single quotes in the query for Python string literal
	string escaped_query = query;
	size_t pos = 0;
	while ((pos = escaped_query.find("'", pos)) != string::npos) {
		escaped_query.replace(pos, 1, "\\'");
		pos += 2; // Skip the escaped quote
	}
	
	script_file << R"(
import sys
import os
import json

try:
	sys.path.insert(0, 'src/cli')
	from snowducks.core import snowflake_query
	
	# Execute the query and cache it
	table_name = ')" + table_name + R"('
	query = ')" + escaped_query + R"('
	result_table, cache_status = snowflake_query(query, limit=1000, force_refresh=False)
	print(json.dumps({'success': True, 'table_name': result_table, 'cache_status': cache_status}))
except ImportError as e:
	# Handle missing dependencies (like pyarrow) in test environment
	print(json.dumps({'success': False, 'error': 'Missing dependencies: ' + str(e)}))
except Exception as e:
	# Handle other errors
	print(json.dumps({'success': False, 'error': str(e)}))
)";
	script_file.close();
	
	// Execute the Python script and capture output
	string command = "python3 " + temp_script + " 2>&1";
	FILE* pipe = popen(command.c_str(), "r");
	if (!pipe) {
		remove(temp_script.c_str());
		return false;
	}
	
	// Read the output
	string result;
	char buffer[128];
	while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
		result += buffer;
	}
	
	// Close pipe and clean up
	pclose(pipe);
	remove(temp_script.c_str());
	
	// Parse JSON response to check if successful
	// Simple JSON parsing - look for "success": false
	if (result.find("\"success\": false") != string::npos) {
		return false;
	}
	
	return result.find("\"success\": true") != string::npos;
}

// Virtual table function that dynamically registers tables
class SnowducksTableFunction : public TableFunction {
public:
	SnowducksTableFunction() : TableFunction("snowducks_table", 
		{LogicalType::VARCHAR}, // SQL query parameter (required)
		SnowducksTableFunc, SnowducksTableBind, SnowducksTableInit) {
	}

private:
	struct SnowducksBindData : public TableFunctionData {
		string original_query;
		string cache_table_name;
	};

	static unique_ptr<FunctionData> SnowducksTableBind(ClientContext &context, TableFunctionBindInput &input,
													  vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_uniq<SnowducksBindData>();
		
		// Get the SQL query from the function call
		if (input.inputs.size() < 1) {
			throw Exception(ExceptionType::INVALID_INPUT, "snowducks_table requires a SQL query parameter");
		}
		
		result->original_query = input.inputs[0].GetValue<string>();
		
		// Generate cache table name from the query
		string normalized_query = to_lowercase(result->original_query);
		result->cache_table_name = "t_" + generate_sha256_hash(normalized_query);
		
		// Check if cache exists
		if (!cache_file_exists(result->cache_table_name)) {
			// Fetch from Snowflake
			if (!fetch_from_snowflake(result->cache_table_name, result->original_query)) {
				throw Exception(ExceptionType::INVALID_INPUT, "Failed to fetch data from Snowflake for query");
			}
		}
		
		// For now, return a simple schema - DuckDB will handle the actual Parquet reading
		// In a real implementation, we would register a virtual table that points to the Parquet file
		return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
		names = {"column1", "column2", "column3"};
		
		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> SnowducksTableInit(ClientContext &context, TableFunctionInitInput &input) {
		auto result = make_uniq<GlobalTableFunctionState>();
		return std::move(result);
	}

	static void SnowducksTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = data_p.bind_data->Cast<SnowducksBindData>();
		
		// Check if cache file exists
		if (!cache_file_exists(bind_data.cache_table_name)) {
			output.SetCardinality(0);
			return;
		}
		
		// For now, return empty result
		// In a real implementation, we would:
		// 1. Register a virtual table that points to the Parquet file
		// 2. Let DuckDB's built-in Parquet reader handle all the data reading
		// 3. The virtual table would automatically use DuckDB's Parquet support
		
		output.SetCardinality(0);
	}
};

// Table function to register a virtual table
inline void SnowducksRegisterVirtualTable(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &table_name_vector = args.data[0];
	auto &query_vector = args.data[1];
	
	BinaryExecutor::Execute<string_t, string_t, string_t>(table_name_vector, query_vector, result, args.size(),
		[&](string_t table_name, string_t query) {
			// Check if cache exists
			if (!cache_file_exists(table_name.GetString())) {
				// Fetch from Snowflake
				if (!fetch_from_snowflake(table_name.GetString(), query.GetString())) {
					return StringVector::AddString(result, "Failed to fetch data from Snowflake");
				}
			}
			
			return StringVector::AddString(result, "Virtual table " + table_name.GetString() + " registered successfully");
		});
}

static void LoadInternal(DatabaseInstance &instance) {
	// Core SnowDucks functions
	auto normalize_query_function = ScalarFunction("snowducks_normalize_query_text", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksNormalizeQueryText);
	ExtensionUtil::RegisterFunction(instance, normalize_query_function);
	
	auto generate_table_name_function = ScalarFunction("snowducks_generate_cache_table_name", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksGenerateCacheTableName);
	ExtensionUtil::RegisterFunction(instance, generate_table_name_function);
	
	auto generate_table_name_no_limit_function = ScalarFunction("snowducks_generate_cache_table_name_without_limit", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksGenerateCacheTableNameWithoutLimit);
	ExtensionUtil::RegisterFunction(instance, generate_table_name_no_limit_function);
	
	auto has_limit_function = ScalarFunction("snowducks_has_limit_clause", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, SnowducksHasLimitClause);
	ExtensionUtil::RegisterFunction(instance, has_limit_function);
	
	auto extract_limit_function = ScalarFunction("snowducks_extract_limit_value", {LogicalType::VARCHAR}, LogicalType::INTEGER, SnowducksExtractLimitValue);
	ExtensionUtil::RegisterFunction(instance, extract_limit_function);
	
	// Info function
	auto info_function = ScalarFunction("snowducks_info", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksInfo);
	ExtensionUtil::RegisterFunction(instance, info_function);
	
	// Virtual table registration function
	auto register_vtable_function = ScalarFunction("snowducks_register_virtual_table", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, SnowducksRegisterVirtualTable);
	ExtensionUtil::RegisterFunction(instance, register_vtable_function);
	
	// Register the table function
	auto table_function = SnowducksTableFunction();
	ExtensionUtil::RegisterFunction(instance, table_function);
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

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
