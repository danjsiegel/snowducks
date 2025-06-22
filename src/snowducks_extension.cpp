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
#include <glob.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

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
		
		// Use the same hash generation as Python CLI: generate_normalized_query_hash
		// This ensures C++ and Python generate the same table names
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

struct SnowducksTableGlobalState : public GlobalTableFunctionState {
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
	struct SnowducksTableBindData : public TableFunctionData {
		string original_query;
		string cache_table_name;
		int limit;
		bool force_refresh;
		bool debug;
		bool is_cached; // Flag to indicate if the table is cached in DuckLake
		vector<LogicalType> column_types;
        vector<string> column_names;
		string fetch_error;
	};

	static unique_ptr<FunctionData> SnowducksTableBind(ClientContext &context, TableFunctionBindInput &input,
	                                                   vector<LogicalType> &return_types, vector<string> &names) {

		auto result = make_uniq<SnowducksTableBindData>();
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
			std::cout << "DEBUG: Starting bind phase with query: " << result->original_query << std::endl;
		}

		// Generate cache table name (without limit clause for caching)
		string query_without_limit = result->original_query;
		size_t limit_pos = to_lowercase(query_without_limit).find(" limit ");
		if (limit_pos != string::npos) {
			query_without_limit = query_without_limit.substr(0, limit_pos);
		}
		
		// Normalize the query the same way as the Python CLI
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
		
		// Use the same hash generation as Python CLI: generate_normalized_query_hash
		// This ensures C++ and Python generate the same table names
		result->cache_table_name = "t_" + generate_sha256_hash(clean_query);
		
		if (result->debug) {
			std::cout << "DEBUG: Generated cache table name: " << result->cache_table_name << std::endl;
		}

		// Check if table exists in cache using DuckLake
		try {
			// Create a separate database instance for cache checking
			DuckDB cache_check_db(":memory:");
			Connection cache_check_conn(cache_check_db);
			// Install and load ducklake
			cache_check_conn.Query("INSTALL ducklake; LOAD ducklake;");
			// Get connection parameters from environment
			const char* data_path_env = std::getenv("DUCKLAKE_DATA_PATH");
			string data_path = data_path_env ? string(data_path_env) : string(std::getenv("HOME")) + "/.snowducks/data";
			
			// Get PostgreSQL connection parameters - require environment variables
			const char* pg_host = std::getenv("PG_HOST");
			const char* pg_port = std::getenv("PG_PORT");
			const char* pg_db = std::getenv("PG_DB");
			const char* pg_user = std::getenv("PG_USER");
			const char* pg_pass = std::getenv("PG_PASS");
			
			// Check if all required PostgreSQL environment variables are set
			if (!pg_host || !pg_port || !pg_db || !pg_user || !pg_pass) {
				if (result->debug) {
					std::cout << "DEBUG: Missing PostgreSQL environment variables. Required: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS" << std::endl;
				}
				// Return a simple schema for error messages
				names.push_back("error");
				return_types.push_back(LogicalType::VARCHAR);
				result->fetch_error = "Missing PostgreSQL environment variables. Required: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS";
				result->column_names = names;
				result->column_types = return_types;
				return std::move(result);
			}

			string attach_sql = "ATTACH 'ducklake:postgres:host=" + string(pg_host) +
								" port=" + string(pg_port) +
								" dbname=" + string(pg_db) +
								" user=" + string(pg_user) +
								" password=" + string(pg_pass) +
								"' AS metadata (DATA_PATH '" + data_path + "');";

			auto attach_result = cache_check_conn.Query(attach_sql);
			if (!attach_result->HasError()) {
				cache_check_conn.Query("USE metadata;");
				
				// Get the schema name for the table
				string schema_name = "main"; // Default schema name - Python CLI creates tables in main schema
				const char* schema_env = std::getenv("DUCKLAKE_SCHEMA");
				if (schema_env) {
					schema_name = string(schema_env);
				}
				
				// Try to describe the table to get its schema
				string full_table_name = schema_name + "." + result->cache_table_name;
				string describe_sql = "DESCRIBE " + full_table_name + ";";
				
				if (result->debug) {
					std::cout << "DEBUG: Checking table: " << full_table_name << std::endl;
				}
				
				auto describe_result = cache_check_conn.Query(describe_sql);
				if (!describe_result->HasError() && describe_result->RowCount() > 0) {
					result->is_cached = true;
					if (result->debug) {
						std::cout << "DEBUG: Found cached table " << result->cache_table_name << " with " << describe_result->RowCount() << " columns" << std::endl;
					}
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
				} else {
					if (result->debug) {
						std::cout << "DEBUG: Table " << result->cache_table_name << " not found in cache" << std::endl;
					}
				}
			} else {
				if (result->debug) {
					std::cout << "DEBUG: Failed to attach DuckLake: " << attach_result->GetError() << std::endl;
				}
			}
		} catch (const std::exception& e) {
			if (result->debug) {
				std::cout << "DEBUG: Cache check failed: " << e.what() << std::endl;
			}
		}
		
		// If no cached table found, get schema from query parsing
		if (!result->is_cached) {
			if (result->debug) {
				std::cout << "DEBUG: Getting schema from query parsing" << std::endl;
			}
			
			// Call Python CLI to get schema from query parsing
			string python_cmd = "cd " + string(getenv("HOME") ? getenv("HOME") : "") + "/Documents/projects/snowducks && source venv/bin/activate && python -m snowducks.cli get-schema " + result->cache_table_name + " \"" + result->original_query + "\" 2>&1";
			
			if (result->debug) {
				std::cout << "DEBUG: Getting schema from query parsing: " << python_cmd << std::endl;
			}
			
			FILE* pipe = popen(python_cmd.c_str(), "r");
			if (pipe) {
				// Read the output
				string cli_result;
				char buffer[128];
				while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
					cli_result += buffer;
				}
				
				int status = pclose(pipe);
				
				if (status == 0) {
					if (result->debug) {
						std::cout << "DEBUG: Python CLI succeeded, parsing schema from: " << cli_result << std::endl;
					}
					
					// Parse schema from Python CLI output
					// Expected format: {"status": "success", "schema": [{"name": "col1", "type": "VARCHAR"}, ...]}
					bool schema_parsed = false;
					try {
						// Simple JSON parsing for schema
						size_t schema_start = cli_result.find("\"schema\":");
						if (schema_start != string::npos) {
							if (result->debug) {
								std::cout << "DEBUG: Found schema key at position " << schema_start << std::endl;
							}
							
							size_t schema_array_start = cli_result.find("[", schema_start);
							size_t schema_array_end = cli_result.find("]", schema_array_start);
							
							if (schema_array_start != string::npos && schema_array_end != string::npos) {
								if (result->debug) {
									std::cout << "DEBUG: Found schema array from " << schema_array_start << " to " << schema_array_end << std::endl;
								}
								
								string schema_json = cli_result.substr(schema_array_start + 1, schema_array_end - schema_array_start - 1);
								
								if (result->debug) {
									std::cout << "DEBUG: Schema JSON content: '" << schema_json << "'" << std::endl;
								}
								
								// Parse each column definition
								size_t pos = 0;
								int column_count = 0;
								while (pos < schema_json.length()) {
									// Find next column definition
									size_t col_start = schema_json.find("{", pos);
									if (col_start == string::npos) {
										if (result->debug) {
											std::cout << "DEBUG: No more column definitions found at position " << pos << std::endl;
										}
										break;
									}
									
									size_t col_end = schema_json.find("}", col_start);
									if (col_end == string::npos) {
										if (result->debug) {
											std::cout << "DEBUG: No closing brace found for column at position " << col_start << std::endl;
										}
										break;
									}
									
									string col_def = schema_json.substr(col_start + 1, col_end - col_start - 1);
									
									if (result->debug) {
										std::cout << "DEBUG: Parsing column definition: '" << col_def << "'" << std::endl;
									}
									
									// Extract name and type
									size_t name_start = col_def.find("\"name\":");
									size_t type_start = col_def.find("\"type\":");
									
									if (name_start != string::npos && type_start != string::npos) {
										name_start += 7; // Skip "name":
										// Skip any whitespace after the colon
										while (name_start < col_def.length() && (col_def[name_start] == ' ' || col_def[name_start] == '\t')) {
											name_start++;
										}
										// Skip the opening quote
										if (name_start < col_def.length() && col_def[name_start] == '"') {
											name_start++;
										}
										size_t name_end = col_def.find("\"", name_start);
										
										type_start += 7; // Skip "type":
										// Skip any whitespace after the colon
										while (type_start < col_def.length() && (col_def[type_start] == ' ' || col_def[type_start] == '\t')) {
											type_start++;
										}
										// Skip the opening quote
										if (type_start < col_def.length() && col_def[type_start] == '"') {
											type_start++;
										}
										size_t type_end = col_def.find("\"", type_start);
										
										if (name_end != string::npos && type_end != string::npos) {
											string col_name = col_def.substr(name_start, name_end - name_start);
											string col_type = col_def.substr(type_start, type_end - type_start);
											
											if (result->debug) {
												std::cout << "DEBUG: Extracted column: name='" << col_name << "', type='" << col_type << "'" << std::endl;
											}
											
											// Convert type string to LogicalType
											LogicalType logical_type = LogicalType::VARCHAR; // Default
											if (col_type == "VARCHAR" || col_type == "STRING" || col_type == "TEXT") {
												logical_type = LogicalType::VARCHAR;
											} else if (col_type == "INTEGER" || col_type == "INT") {
												logical_type = LogicalType::INTEGER;
											} else if (col_type == "BIGINT") {
												logical_type = LogicalType::BIGINT;
											} else if (col_type == "DOUBLE" || col_type == "FLOAT") {
												logical_type = LogicalType::DOUBLE;
											} else if (col_type == "BOOLEAN" || col_type == "BOOL") {
												logical_type = LogicalType::BOOLEAN;
											} else if (col_type == "DATE") {
												logical_type = LogicalType::DATE;
											} else if (col_type == "TIMESTAMP") {
												logical_type = LogicalType::TIMESTAMP;
											}
											
											names.push_back(col_name);
											return_types.push_back(logical_type);
											column_count++;
											
											if (result->debug) {
												std::cout << "DEBUG: Added column " << column_count << ": " << col_name << " -> " << col_type << std::endl;
											}
										} else {
											if (result->debug) {
												std::cout << "DEBUG: Could not find name or type end quotes" << std::endl;
											}
										}
									} else {
										if (result->debug) {
											std::cout << "DEBUG: Could not find name or type keys in column definition" << std::endl;
										}
									}
									
									pos = col_end + 1;
								}
								
								if (result->debug) {
									std::cout << "DEBUG: Finished parsing, found " << column_count << " columns" << std::endl;
								}
								
								schema_parsed = true;
							} else {
								if (result->debug) {
									std::cout << "DEBUG: Could not find schema array brackets" << std::endl;
								}
							}
						} else {
							if (result->debug) {
								std::cout << "DEBUG: Could not find schema key in JSON" << std::endl;
							}
						}
					} catch (const std::exception& e) {
						if (result->debug) {
							std::cout << "DEBUG: Schema parsing failed: " << e.what() << std::endl;
						}
					}
					
					if (!schema_parsed) {
						if (result->debug) {
							std::cout << "DEBUG: Could not parse schema from Python output, using default" << std::endl;
						}
						// Fallback to default schema
						names.push_back("message");
						return_types.push_back(LogicalType::VARCHAR);
						result->fetch_error = "Could not parse schema from Python CLI output";
					} else {
						if (result->debug) {
							std::cout << "DEBUG: Successfully parsed schema with " << names.size() << " columns" << std::endl;
						}
					}
				} else {
					if (result->debug) {
						std::cout << "DEBUG: Python CLI failed with status " << status << std::endl;
						std::cout << "DEBUG: Output: " << cli_result << std::endl;
					}
					// Set up error schema
					names.push_back("message");
					return_types.push_back(LogicalType::VARCHAR);
					result->fetch_error = "Failed to get schema from query parsing: " + cli_result;
				}
			} else {
				if (result->debug) {
					std::cout << "DEBUG: Failed to execute Python CLI" << std::endl;
				}
				names.push_back("message");
				return_types.push_back(LogicalType::VARCHAR);
				result->fetch_error = "Failed to execute Python CLI";
			}
		}
		
		result->column_names = names;
		result->column_types = return_types;

		if (result->debug) {
			std::cout << "DEBUG: Bind phase complete, returning schema with " << names.size() << " columns" << std::endl;
		}

		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> SnowducksTableInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<SnowducksTableGlobalState>();
	}

	static void SnowducksTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = (SnowducksTableBindData &)*data_p.bind_data;
		auto &global_state = (SnowducksTableGlobalState &)*data_p.global_state;
		
		if (global_state.finished) {
			return;
		}
		
		// Check if we need to fetch data
		if (!bind_data.is_cached) {
			if (bind_data.debug) {
				std::cout << "DEBUG: Table not cached, fetching from Snowflake" << std::endl;
			}
			
			// Call Python CLI to fetch and cache the data
			string python_cmd = "cd " + string(getenv("HOME") ? getenv("HOME") : "") + "/Documents/projects/snowducks && source venv/bin/activate && python -m snowducks.cli query --query \"" + bind_data.original_query + "\" 2>&1";
			
			if (bind_data.debug) {
				std::cout << "DEBUG: Executing Python CLI: " << python_cmd << std::endl;
			}
			
			FILE* pipe = popen(python_cmd.c_str(), "r");
			if (pipe) {
				string cli_result;
				char buffer[128];
				while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
					cli_result += buffer;
				}
				
				int status = pclose(pipe);
				
				if (status != 0) {
					if (bind_data.debug) {
						std::cout << "DEBUG: Python CLI failed with status " << status << std::endl;
						std::cout << "DEBUG: Output: " << cli_result << std::endl;
					}
					
					// Extract error message from Python CLI output
					string error_message = "Failed to fetch data from Snowflake";
					
					// Look for common error patterns in the output
					if (cli_result.find("Error:") != string::npos) {
						// Extract the line starting with "Error:"
						size_t error_pos = cli_result.find("Error:");
						size_t newline_pos = cli_result.find('\n', error_pos);
						if (newline_pos != string::npos) {
							error_message = cli_result.substr(error_pos, newline_pos - error_pos);
						} else {
							error_message = cli_result.substr(error_pos);
						}
					} else if (cli_result.find("NOT_FOUND:") != string::npos) {
						// Extract Snowflake error messages
						size_t error_pos = cli_result.find("NOT_FOUND:");
						size_t newline_pos = cli_result.find('\n', error_pos);
						if (newline_pos != string::npos) {
							error_message = cli_result.substr(error_pos, newline_pos - error_pos);
						} else {
							error_message = cli_result.substr(error_pos);
						}
					} else if (cli_result.find("SQL compilation error:") != string::npos) {
						// Extract SQL compilation errors
						size_t error_pos = cli_result.find("SQL compilation error:");
						size_t newline_pos = cli_result.find('\n', error_pos);
						if (newline_pos != string::npos) {
							error_message = cli_result.substr(error_pos, newline_pos - error_pos);
						} else {
							error_message = cli_result.substr(error_pos);
						}
					} else if (!bind_data.fetch_error.empty()) {
						// Use stored fetch error if available
						error_message = bind_data.fetch_error;
					}
					
					// Return error message
					output.SetCardinality(1);
					output.data[0].SetValue(0, Value(error_message));
					global_state.finished = true;
					return;
				}
				
				if (bind_data.debug) {
					std::cout << "DEBUG: Python CLI succeeded, data cached" << std::endl;
				}
			} else {
				if (bind_data.debug) {
					std::cout << "DEBUG: Failed to execute Python CLI" << std::endl;
				}
				
				// Return error message
				output.SetCardinality(1);
				output.data[0].SetValue(0, Value("Failed to execute Python CLI"));
				global_state.finished = true;
				return;
			}
		}
		
		// Now read from the cached Parquet file
		try {
			// Create a separate database instance for reading cached data
			DuckDB read_db(":memory:");
			Connection read_conn(read_db);
			// Install and load ducklake
			read_conn.Query("INSTALL ducklake; LOAD ducklake;");
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

			auto attach_result = read_conn.Query(attach_sql);
			if (!attach_result->HasError()) {
				read_conn.Query("USE metadata;");
				
				// Get the schema name for the table
				string schema_name = "main"; // Default schema name - Python CLI creates tables in main schema
				const char* schema_env = std::getenv("DUCKLAKE_SCHEMA");
				if (schema_env) {
					schema_name = string(schema_env);
				}
				
				// Read from the cached table with full schema name
				string full_table_name = schema_name + "." + bind_data.cache_table_name;
				string select_sql = "SELECT * FROM " + full_table_name + ";";
				
				if (bind_data.debug) {
					std::cout << "DEBUG: Reading from table: " << full_table_name << std::endl;
				}
				
				// Add retry mechanism for timing issues
				int max_retries = 3;
				int retry_count = 0;
				auto select_result = read_conn.Query(select_sql);
				
				while (select_result->HasError() && retry_count < max_retries) {
					if (bind_data.debug) {
						std::cout << "DEBUG: Table not found, retrying in 1 second... (attempt " << (retry_count + 1) << "/" << max_retries << ")" << std::endl;
					}
					
					// Wait 1 second before retrying
					#ifdef _WIN32
					Sleep(1000);
					#else
					sleep(1);
					#endif
					
					retry_count++;
					select_result = read_conn.Query(select_sql);
				}
				
				if (!select_result->HasError()) {
					if (bind_data.debug) {
						std::cout << "DEBUG: Successfully read from cached table, " << select_result->RowCount() << " rows" << std::endl;
					}
					
					// Copy data to output chunk
					idx_t row_count = 0;
					while (true) {
						auto chunk = select_result->Fetch();
						if (!chunk || chunk->size() == 0) break;
						
						idx_t chunk_size = chunk->size();
						output.SetCardinality(chunk_size);
						
						for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
							for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
								output.data[col_idx].SetValue(row_idx, chunk->GetValue(col_idx, row_idx));
							}
						}
						
						row_count += chunk_size;
						if (bind_data.debug) {
							std::cout << "DEBUG: Output chunk with " << chunk_size << " rows" << std::endl;
						}
						
						// Return this chunk
						global_state.finished = (row_count >= select_result->RowCount());
						return;
					}
					
					global_state.finished = true;
				} else {
					if (bind_data.debug) {
						std::cout << "DEBUG: Failed to read from cached table: " << select_result->GetError() << std::endl;
					}
					
					// Return error message
					output.SetCardinality(1);
					output.data[0].SetValue(0, Value("Failed to read from cached table: " + select_result->GetError()));
					global_state.finished = true;
				}
			} else {
				if (bind_data.debug) {
					std::cout << "DEBUG: Failed to attach DuckLake for reading: " << attach_result->GetError() << std::endl;
				}
				
				// Return error message
				output.SetCardinality(1);
				output.data[0].SetValue(0, Value("Failed to attach DuckLake for reading: " + attach_result->GetError()));
				global_state.finished = true;
			}
		} catch (const std::exception& e) {
			if (bind_data.debug) {
				std::cout << "DEBUG: Error reading cached data: " << e.what() << std::endl;
			}
			
			// Return error message
			output.SetCardinality(1);
			output.data[0].SetValue(0, Value("Error reading cached data: " + string(e.what())));
			global_state.finished = true;
		}
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