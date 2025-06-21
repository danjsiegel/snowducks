#define DUCKDB_EXTENSION_MAIN

#include "snowducks_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <openssl/md5.h>
#include <sstream>
#include <iomanip>

namespace duckdb {

// Utility function to convert string to lowercase
string to_lowercase(const string &input) {
	string result = input;
	std::transform(result.begin(), result.end(), result.begin(), ::tolower);
	return result;
}

// Generate MD5 hash of a string
string generate_md5_hash(const string &input) {
	unsigned char digest[MD5_DIGEST_LENGTH];
	MD5_CTX ctx;
	MD5_Init(&ctx);
	MD5_Update(&ctx, input.c_str(), input.length());
	MD5_Final(digest, &ctx);
	
	std::stringstream ss;
	for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
		ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
	}
	return ss.str();
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
		
		// Generate hash
		string hash = generate_md5_hash(clean_query);
		string table_name = "t_" + hash.substr(0, 16);
		
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
		
		// Generate hash
		string hash = generate_md5_hash(clean_query);
		string table_name = "t_" + hash.substr(0, 16);
		
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
