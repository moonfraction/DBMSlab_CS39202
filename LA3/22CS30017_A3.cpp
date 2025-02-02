#include <iostream>
#include <string>
#include <vector>
#include <sql.h>
#include <sqlext.h>

// Error handling class
class SQLError {
public:
    static void extract_error(const char* fn, SQLHANDLE handle, SQLSMALLINT type) {
        SQLSMALLINT i = 0;
        SQLINTEGER native;
        SQLCHAR state[7];
        SQLCHAR text[256];
        SQLSMALLINT len;
        SQLRETURN ret;

        std::cout << "\nThe driver reported the following diagnostics whilst running " << fn << "\n";
        do {
            ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text), &len);
            if (SQL_SUCCEEDED(ret)) {
                std::cout << state << ":" << i << ":" << native << ":" << text << "\n";
            }
        } while (ret == SQL_SUCCESS);
    }
};

// Database handler class
class DatabaseHandler {
private:
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;

public:
    DatabaseHandler() : env(nullptr), dbc(nullptr), stmt(nullptr) {}
    
    bool initialize() {
        SQLRETURN ret;

        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) return false;

        ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
        if (!SQL_SUCCEEDED(ret)) return false;

        ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
        if (!SQL_SUCCEEDED(ret)) return false;

        return true;
    }

    bool connect(const std::string& connStr) {
        SQLCHAR outStr[1024];
        SQLSMALLINT outStrLen;
        
        SQLRETURN ret = SQLDriverConnect(dbc, NULL,
            (SQLCHAR*)connStr.c_str(), SQL_NTS,
            outStr, sizeof(outStr), &outStrLen,
            SQL_DRIVER_NOPROMPT);

        if (!SQL_SUCCEEDED(ret)) {
            SQLError::extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
            return false;
        }

        ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
        return SQL_SUCCEEDED(ret);
    }

    bool execute_query(const std::string& query) {
        SQLRETURN ret = SQLExecDirect(stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
        if (!SQL_SUCCEEDED(ret)) {
            SQLError::extract_error("SQLExecDirect", stmt, SQL_HANDLE_STMT);
            return false;
        }
        SQLCloseCursor(stmt);
        return true;
    }


    void setup_database() {
        std::vector<std::string> create_tables = {
            "DROP TABLE IF EXISTS household, citizen, land_record, panchayat_employee, asset, welfare_scheme, scheme_enrollment, vaccination, census_data;",

            "CREATE TABLE household (household_id INT PRIMARY KEY, address TEXT NOT NULL, income DECIMAL(10, 2) NOT NULL);",

            "CREATE TABLE citizen (citizen_ID INT PRIMARY KEY, name VARCHAR(100) NOT NULL, gender CHAR(1) NOT NULL, date_of_birth DATE NOT NULL, household_id INT, education_qualification VARCHAR(50), FOREIGN KEY (household_id) REFERENCES household(household_id));",

            "CREATE TABLE land_record (land_id INT PRIMARY KEY, citizen_id INT, area_acres DECIMAL(10, 2) NOT NULL, crop_type TEXT NOT NULL, FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id));",

            "CREATE TABLE panchayat_employee (employee_id INT PRIMARY KEY, citizen_id INT, role TEXT NOT NULL, FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id));",

            "CREATE TABLE asset (asset_id INT PRIMARY KEY, type TEXT NOT NULL, location TEXT NOT NULL, installation_date DATE NOT NULL);",

            "CREATE TABLE welfare_scheme (scheme_id INT PRIMARY KEY, name TEXT NOT NULL, description TEXT);",

            "CREATE TABLE scheme_enrollment (enrollment_id INT PRIMARY KEY, citizen_id INT, scheme_id INT, enrollment_date DATE NOT NULL, FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id), FOREIGN KEY (scheme_id) REFERENCES welfare_scheme(scheme_id));",

            "CREATE TABLE vaccination (vaccination_id INT PRIMARY KEY, citizen_id INT, vaccine_type TEXT NOT NULL, date_administered DATE NOT NULL, FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id));",

            "CREATE TABLE census_data (household_id INT, citizen_id INT, event_type TEXT NOT NULL, event_date DATE NOT NULL, FOREIGN KEY (household_id) REFERENCES household(household_id), FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id));"
        };

        std::vector<std::string> insert_data = {
                // Insert household data
                "INSERT INTO household (household_id, address, income) VALUES "
                "(1, '123, MG Road, Mumbai', 95000.00),"
                "(2, '456, Park Street, Kolkata', 125000.00),"
                "(3, '789, Brigade Road, Bangalore', 75000.00),"
                "(4, '101, Anna Salai, Chennai', 145000.00),"
                "(5, '202, Connaught Place, Delhi', 82000.00),"
                "(6, '303, Banjara Hills, Hyderabad', 102500.00),"
                "(7, '404, Marine Drive, Kochi', 98000.00),"
                "(8, '505, Law Garden, Ahmedabad', 110000.00),"
                "(9, '606, Civil Lines, Jaipur', 87000.00),"
                "(10, '707, Rajwada, Indore', 130000.00);",

                // Insert citizen data
                "INSERT INTO citizen (citizen_id, name, gender, date_of_birth, household_id, education_qualification) VALUES "
                "(1, 'Amit Sharma', 'M', '1990-05-12', 1, 'Graduate'),"
                "(2, 'Priya Singh', 'F', '2005-09-15', 2, '10th'),"
                "(3, 'Anjali Gupta', 'F', '2010-11-25', 3, 'Primary'),"
                "(4, 'Rohit Verma', 'M', '1998-01-20', 4, '12th'),"
                "(5, 'Sneha Patel', 'F', '2012-03-05', 5, 'Primary'),"
                "(6, 'Vikram Rao', 'M', '1985-07-19', 6, 'Post-Graduate'),"
                "(7, 'Pooja Mehta', 'F', '2008-08-17', 10, '12th'),"
                "(8, 'Arjun Nair', 'M', '2000-12-30', 8, 'Secondary'),"
                "(9, 'Kavya Iyer', 'F', '2003-04-14', 9, 'Graduate'),"
                "(10, 'Rohan Desai', 'M', '2015-06-10', 10, 'Primary'),"
                "(11, 'Neha Singh', 'F', '1995-02-28', 1, 'Post-Graduate'),"
                "(12, 'Rajesh Kumar', 'M', '1980-10-05', 2, 'Graduate'),"
                "(13, 'Sunita Devi', 'F', '1975-12-15', 3, 'Secondary'),"
                "(14, 'Rahul Kumar', 'M', '2002-05-12', 1, '10th'),"
                "(15, 'Meena Sharma', 'F', '1965-06-05', 5, 'Graduate'),"
                "(16, 'Ramesh Patel', 'M', '1960-07-19', 6, 'Secondary'),"
                "(17, 'Geeta Rao', 'F', '1955-08-17', 7, 'Post-Graduate'),"
                "(18, 'Rajesh Nair', 'M', '1950-12-30', 8, 'Graduate'),"
                "(19, 'Kamini Iyer', 'F', '1945-04-14', 9, 'Secondary'),"
                "(20, 'Raj Desai', 'M', '1940-06-10', 10, 'Post-Graduate'),"
                "(21, 'Vijay Singh', 'M', '1970-04-20', 4, 'Post-Graduate'),"
                "(22, 'Sita Devi', 'F', '2004-09-15', 2, '10th'),"
                "(23, 'Mohan Das', 'M', '2001-11-25', 3, '10th'),"
                "(24, 'Gita Verma', 'F', '1999-01-20', 4, '12th'),"
                "(25, 'Raj Patel', 'M', '2013-03-05', 1, 'Primary'),"
                "(26, 'Meena Rao', 'F', '1986-07-19', 6, 'Post-Graduate'),"
                "(27, 'Ramesh Mehta', 'M', '2009-08-17', 7, '10th'),"
                "(28, 'Geeta Nair', 'F', '2002-12-30', 8, 'Secondary'),"
                "(29, 'Rajesh Iyer', 'M', '2005-04-14', 9, 'Graduate'),"
                "(30, 'Kamal Desai', 'M', '2016-06-10', 10, 'Primary'),"
                "(31, 'eheh', 'M', '2024-04-04', 3, 'Primary');",

                // Insert land record data
                "INSERT INTO land_record (land_id, citizen_id, area_acres, crop_type) VALUES "
                "(1, 1, 1.5, 'Rice'),"
                "(2, 2, 0.8, 'Wheat'),"
                "(3, 3, 2.0, 'Cotton'),"
                "(4, 4, 0.5, 'Rice'),"
                "(5, 5, 1.2, 'Maize'),"
                "(6, 6, 1.8, 'Rice'),"
                "(7, 7, 0.6, 'Wheat'),"
                "(8, 8, 2.5, 'Sugarcane'),"
                "(9, 9, 1.0, 'Rice'),"
                "(10, 10, 0.9, 'Cotton');",

                // Insert panchayat employee data
                "INSERT INTO panchayat_employee (employee_id, citizen_id, role) VALUES "
                "(1, 1, 'Panchayat Pradhan'),"
                "(2, 2, 'Secretary'),"
                "(3, 6, 'Member'),"
                "(4, 8, 'Treasurer'),"
                "(5, 10, 'Member'),"
                "(6, 11, 'Vice President'),"
                "(7, 12, 'Member'),"
                "(8, 16, 'Secretary'),"
                "(9, 18, 'Auditor'),"
                "(10, 20, 'Treasurer'),"
                "(11, 21, 'Clerk'),"
                "(12, 22, 'Supervisor'),"
                "(13, 23, 'Assistant'),"
                "(14, 24, 'Coordinator'),"
                "(15, 25, 'Advisor');",

                // Insert asset data
                "INSERT INTO asset (asset_id, type, location, installation_date) VALUES "
                "(1, 'Street Light', 'Phulera', '2024-01-15'),"
                "(2, 'Street Light', 'Phulera', '2024-02-20'),"
                "(3, 'Water Pump', 'XYZ Village', '2023-08-10'),"
                "(4, 'Road', 'ABC Village', '2022-09-05'),"
                "(5, 'Street Light', 'Phulera', '2024-03-18'),"
                "(6, 'Water Pump', 'XYZ Village', '2023-10-25'),"
                "(7, 'Road', 'ABC Village', '2022-11-30'),"
                "(8, 'Street Light', 'Phulera', '2024-04-15'),"
                "(9, 'Water Pump', 'XYZ Village', '2023-12-20'),"
                "(10, 'Road', 'ABC Village', '2023-01-25');",

                // Insert welfare scheme data
                "INSERT INTO welfare_scheme (scheme_id, name, description) VALUES "
                "(1, 'MNREGA', 'Employment Guarantee Scheme'),"
                "(2, 'PMAY', 'Affordable Housing Scheme'),"
                "(3, 'Midday Meal', 'School Lunch Program'),"
                "(4, 'PMKSY', 'Irrigation Scheme'),"
                "(5, 'PMFBY', 'Crop Insurance Scheme');",

                // Insert scheme enrollment data
                "INSERT INTO scheme_enrollment (enrollment_id, citizen_id, scheme_id, enrollment_date) VALUES "
                "(1, 2, 1, '2023-07-10'),"
                "(2, 3, 2, '2024-01-15'),"
                "(3, 4, 3, '2024-02-01'),"
                "(4, 5, 1, '2024-03-20'),"
                "(5, 6, 2, '2024-04-05'),"
                "(6, 7, 3, '2024-05-10'),"
                "(7, 8, 1, '2024-06-15'),"
                "(8, 9, 2, '2024-07-20'),"
                "(9, 10, 3, '2024-08-25');",

                // Insert vaccination data
                "INSERT INTO vaccination (vaccination_id, citizen_id, vaccine_type, date_administered) VALUES "
                "(1, 5, 'Covid-19', '2024-05-20'),"
                "(2, 7, 'Polio', '2024-03-10'),"
                "(3, 10, 'Hepatitis', '2024-06-15'),"
                "(4, 1, 'Covid-19', '2024-07-20'),"
                "(5, 3, 'Polio', '2024-08-10'),"
                "(6, 31, 'Polio', '2024-06-06');",

                // Insert child birth data for all households
                "INSERT INTO citizen (citizen_id, name, gender, date_of_birth, household_id, education_qualification) "
                "SELECT "
                "    (SELECT MAX(citizen_id) FROM citizen) + ROW_NUMBER() OVER (ORDER BY h.household_id), "
                "    'Child ' || ROW_NUMBER() OVER (ORDER BY h.household_id), "
                "    CASE WHEN RANDOM() < 0.5 THEN 'M' ELSE 'F' END, "
                "    DATE '2024-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 365), "
                "    h.household_id, "
                "    'Primary' "
                "FROM household h "
                "WHERE h.household_id IN (SELECT household_id FROM household);",

                // Census data queries
                "INSERT INTO census_data (household_id, citizen_id, event_type, event_date) "
                "SELECT household_id, citizen_id, 'Birth', date_of_birth "
                "FROM citizen "
                "WHERE date_of_birth IS NOT NULL;",

                // death
                "INSERT INTO census_data (household_id, citizen_id, event_type, event_date) "
                "SELECT household_id, citizen_id, 'Death', "
                "DATE '2024-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 365) "
                "FROM citizen "
                "WHERE citizen_id NOT IN (SELECT citizen_id FROM panchayat_employee) "
                "AND date_of_birth < '1980-01-01';",

                // marriage
                "INSERT INTO census_data (household_id, citizen_id, event_type, event_date) "
                "SELECT household_id, citizen_id, 'Marriage', "
                "DATE '2000-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 9131) "
                "FROM citizen "
                "WHERE date_of_birth < '2000-01-01';",

                // divorce
                "INSERT INTO census_data (household_id, citizen_id, event_type, event_date) "
                "SELECT household_id, citizen_id, 'Divorce', "
                "DATE '2000-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 1826) "
                "FROM citizen "
                "WHERE date_of_birth >= '1990-01-01' "
                "AND date_of_birth <= '2000-12-31';"
            };

        // Execute CREATE TABLE statements
        for (const auto& query : create_tables) {
            if (!execute_query(query)) {
                std::cerr << "Failed to create table\n";
                return;
            }
        }

        // Execute INSERT statements
        for (const auto& query : insert_data) {
            if (!execute_query(query)) {
                std::cerr << "Failed to insert data\n";
                return;
            }
        }
    }


    std::vector<std::pair<std::string, std::string>> queries = {
        {
            "SELECT c.name "
            "FROM citizen c "
            "    JOIN land_record l ON c.citizen_id = l.citizen_id "
            "WHERE l.area_acres > 1",
            "Citizens with more than 1 acre of land"
        },
        {
            "SELECT c.name "
            "FROM household h "
            "    JOIN citizen c ON c.household_id = h.household_id "
            "WHERE h.income < 100000.00 "
            "    AND c.gender = 'F' "
            "    AND c.education_qualification in ('Primary', 'Secondary', '10th', '12th')",
            "Female students with low household income"
        },
        {
            "SELECT sum(area_acres) as total_acres "
            "FROM land_record "
            "WHERE crop_type = 'Rice'",
            "Total rice cultivation land"
        },
        {
            "SELECT count(citizen_id) as count "
            "FROM citizen "
            "WHERE date_of_birth > '2000-01-01' "
            "    AND education_qualification = '10th'",
            "Number of citizens born after 1.1.2000 with 10th class education"
        },
        {
            "SELECT c.name "
            "FROM citizen c "
            "    JOIN panchayat_employee pe ON c.citizen_id = pe.citizen_id "
            "    JOIN land_record l ON c.citizen_id = l.citizen_id "
            "WHERE l.area_acres > 1",
            "Panchayat employees who hold more than 1 acre land"
        },
        {
            "SELECT c.name "
            "FROM citizen c "
            "WHERE c.household_id = ("
            "    SELECT ci.household_id "
            "    FROM citizen ci "
            "        JOIN panchayat_employee pe ON ci.citizen_id = pe.citizen_id "
            "    WHERE pe.role = 'Panchayat Pradhan')",
            "Household members of Panchayat Pradhan"
        },
        {
            "SELECT count(asset_id) as count "
            "FROM asset "
            "WHERE location = 'Phulera' "
            "    AND installation_date >= '2024-01-01' "
            "    AND installation_date <= '2024-12-31' "
            "    AND type = 'Street Light'",
            "Street lights installed in Phulera in 2024"
        },
        {
            "SELECT count(DISTINCT c.citizen_id) as count "
            "FROM citizen c "
            "    JOIN household h ON c.household_id = h.household_id "
            "    JOIN citizen c2 ON h.household_id = c2.household_id "
            "    JOIN vaccination v ON c2.citizen_id = v.citizen_id "
            "WHERE c.education_qualification = '10th' "
            "    AND v.date_administered >= '2024-01-01' "
            "    AND v.date_administered <= '2024-12-31' "
            "    AND c2.date_of_birth > c.date_of_birth "
            "    AND EXTRACT(YEAR FROM AGE(c2.date_of_birth)) < 18",
            "Vaccinations for children of 10th pass citizens in 2024"
        },
        {
            "SELECT count(event_type) as count "
            "FROM census_data "
            "WHERE event_type = 'Birth' "
            "    AND event_date >= '2024-01-01' "
            "    AND event_date <= '2024-12-31'",
            "Total births in 2024"
        },
        {
            "SELECT count(citizen_id) as count "
            "FROM citizen "
            "WHERE household_id IN ("
            "    SELECT household_id "
            "    FROM citizen "
            "    WHERE citizen_id IN ("
            "        SELECT citizen_id "
            "        FROM panchayat_employee))",
            "Citizens in households with panchayat employees"
        }
    };

    void execute_queries() {
        std::cout << "\nExecuting queries...\n";
        for (const auto& query : queries) {
            std::cout << "\n" << query.second << ":\n";
            SQLRETURN ret = SQLExecDirect(stmt, (SQLCHAR*)query.first.c_str(), SQL_NTS);
            
            if (SQL_SUCCEEDED(ret)) {
                while (SQL_SUCCEEDED(SQLFetch(stmt))) {
                    SQLCHAR result[256];
                    SQLLEN indicator;
                    ret = SQLGetData(stmt, 1, SQL_C_CHAR, result, sizeof(result), &indicator);
                    if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
                        std::cout << result << "\n";
                    }
                }
            } else {
                SQLError::extract_error("SQLExecDirect", stmt, SQL_HANDLE_STMT);
            }
            SQLCloseCursor(stmt);
        }
    }

    void cleanup() {
        if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        if (dbc) {
            SQLDisconnect(dbc);
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        }
        if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
    }

    SQLHSTMT get_statement() const { return stmt; }

    ~DatabaseHandler() {
        cleanup();
    }
};

int main() {
    DatabaseHandler db;
    
    if (!db.initialize()) {
        std::cerr << "Failed to initialize database handler\n";
        return 1;
    }

    std::string connStr = "DRIVER={PostgreSQL UNICODE};"
                         "SERVER=10.5.18.72;"
                         "PORT=5432;"
                         "DATABASE=22CS30017;"
                         "UID=22CS30017;"
                         "PWD=Asdfghjkl@1234567890;";

    if (!db.connect(connStr)) {
        std::cerr << "Failed to connect to database\n";
        return 1;
    }

    // Setup database tables and initial data
    db.setup_database();

    // Execute queries
    db.execute_queries();

    std::cout << "\nAll queries executed successfully.\n";
    return 0;
}