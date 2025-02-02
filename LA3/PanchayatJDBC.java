import java.sql.*;

// javac -cp postgresql-42.7.1.jar PanchayatJDBC.java
// java -cp .:postgresql-42.7.1.jar PanchayatJDBC

public class PanchayatJDBC {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://10.5.18.72:5432/22CS30017";
        String user = "22CS30017";
        String password = "Asdfghjkl@1234567890";

        try {
            // Load the PostgreSQL JDBC driver
            Class.forName("org.postgresql.Driver");

            // Establish the connection
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.\n");

            // Create tables
            createTables(conn);
            
            // Insert data
            insertData(conn);
            
            // Execute queries
            executeQueries(conn);

            // Close connection
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTables(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        
        // Drop existing tables
        String dropTables = """
            DROP TABLE IF EXISTS household, citizen, land_record, panchayat_employee, 
            asset, welfare_scheme, scheme_enrollment, vaccination, census_data;
        """;
        stmt.execute(dropTables);

        // Create tables
        String createHousehold = """
            CREATE TABLE household (
                household_id INT PRIMARY KEY,
                address TEXT NOT NULL,
                income DECIMAL(10, 2) NOT NULL
            );
        """;
        stmt.execute(createHousehold);

        String createCitizen = """
            CREATE TABLE citizen (
                citizen_ID INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                gender CHAR(1) NOT NULL,
                date_of_birth DATE NOT NULL,
                household_id INT,
                education_qualification VARCHAR(50),
                FOREIGN KEY (household_id) REFERENCES household(household_id)
            );
        """;
        stmt.execute(createCitizen);
        
        String createLandRecord = """
            CREATE TABLE land_record (
                land_id INT PRIMARY KEY,
                citizen_id INT,
                area_acres DECIMAL(10, 2) NOT NULL,
                crop_type TEXT NOT NULL,
                FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
            );
        """;
        stmt.execute(createLandRecord);

        String createPanchayatEmployee = """
        CREATE TABLE panchayat_employee (
            employee_id INT PRIMARY KEY,
            citizen_id INT,
            role TEXT NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """;
        stmt.execute(createPanchayatEmployee);


        String createAsset = """
        CREATE TABLE asset (
            asset_id INT PRIMARY KEY,
            type TEXT NOT NULL,
            location TEXT NOT NULL,
            installation_date DATE NOT NULL
        );
        """;
        stmt.execute(createAsset);

        String createWelfareScheme = """
        CREATE TABLE welfare_scheme (
            scheme_id INT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        );
        """;
        stmt.execute(createWelfareScheme);

        String createSchemeEnrollment = """
        CREATE TABLE scheme_enrollment (
            enrollment_id INT PRIMARY KEY,
            citizen_id INT,
            scheme_id INT,
            enrollment_date DATE NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id),
            FOREIGN KEY (scheme_id) REFERENCES welfare_scheme(scheme_id)
        );
        """;
        stmt.execute(createSchemeEnrollment);

        String createVaccination = """
        CREATE TABLE vaccination (
            vaccination_id INT PRIMARY KEY,
            citizen_id INT,
            vaccine_type TEXT NOT NULL,
            date_administered DATE NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """;
        stmt.execute(createVaccination);

        String createCensusData = """
        CREATE TABLE census_data (
            household_id INT,
            citizen_id INT,
            event_type TEXT NOT NULL,
            event_date DATE NOT NULL,
            FOREIGN KEY (household_id) REFERENCES household(household_id),
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """;
        stmt.execute(createCensusData);

        stmt.close();
    }

    private static void insertData(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        // Insert household data
        String insertHouseholds = """
            INSERT INTO household (household_id, address, income) VALUES 
            (1, '123, MG Road, Mumbai', 95000.00),
            (2, '456, Park Street, Kolkata', 125000.00),
            (3, '789, Brigade Road, Bangalore', 75000.00),
            (4, '101, Anna Salai, Chennai', 145000.00),
            (5, '202, Connaught Place, Delhi', 82000.00),
            (6, '303, Banjara Hills, Hyderabad', 102500.00),
            (7, '404, Marine Drive, Kochi', 98000.00),
            (8, '505, Law Garden, Ahmedabad', 110000.00),
            (9, '606, Civil Lines, Jaipur', 87000.00),
            (10, '707, Rajwada, Indore', 130000.00);
        """;
        stmt.execute(insertHouseholds);

        // insert citizen data
        String insertCitizens = """
        INSERT INTO citizen (citizen_id, name, gender, date_of_birth, household_id, education_qualification) VALUES 
        (1, 'Amit Sharma', 'M', '1990-05-12', 1, 'Graduate'),
        (2, 'Priya Singh', 'F', '2005-09-15', 2, '10th'),
        (3, 'Anjali Gupta', 'F', '2010-11-25', 3, 'Primary'),
        (4, 'Rohit Verma', 'M', '1998-01-20', 4, '12th'),
        (5, 'Sneha Patel', 'F', '2012-03-05', 5, 'Primary'),
        (6, 'Vikram Rao', 'M', '1985-07-19', 6, 'Post-Graduate'),
        (7, 'Pooja Mehta', 'F', '2008-08-17', 10, '12th'),
        (8, 'Arjun Nair', 'M', '2000-12-30', 8, 'Secondary'),
        (9, 'Kavya Iyer', 'F', '2003-04-14', 9, 'Graduate'),
        (10, 'Rohan Desai', 'M', '2015-06-10', 10, 'Primary'),
        (11, 'Neha Singh', 'F', '1995-02-28', 1, 'Post-Graduate'),
        (12, 'Rajesh Kumar', 'M', '1980-10-05', 2, 'Graduate'),
        (13, 'Sunita Devi', 'F', '1975-12-15', 3, 'Secondary'),
        (14, 'Rahul Kumar', 'M', '2002-05-12', 1, '10th'),
        (15, 'Meena Sharma', 'F', '1965-06-05', 5, 'Graduate'),
        (16, 'Ramesh Patel', 'M', '1960-07-19', 6, 'Secondary'),
        (17, 'Geeta Rao', 'F', '1955-08-17', 7, 'Post-Graduate'),
        (18, 'Rajesh Nair', 'M', '1950-12-30', 8, 'Graduate'),
        (19, 'Kamini Iyer', 'F', '1945-04-14', 9, 'Secondary'),
        (20, 'Raj Desai', 'M', '1940-06-10', 10, 'Post-Graduate'),
        (21, 'Vijay Singh', 'M', '1970-04-20', 4, 'Post-Graduate'),
        (22, 'Sita Devi', 'F', '2004-09-15', 2, '10th'),
        (23, 'Mohan Das', 'M', '2001-11-25', 3, '10th'),
        (24, 'Gita Verma', 'F', '1999-01-20', 4, '12th'),
        (25, 'Raj Patel', 'M', '2013-03-05', 1, 'Primary'),
        (26, 'Meena Rao', 'F', '1986-07-19', 6, 'Post-Graduate'),
        (27, 'Ramesh Mehta', 'M', '2009-08-17', 7, '10th'),
        (28, 'Geeta Nair', 'F', '2002-12-30', 8, 'Secondary'),
        (29, 'Rajesh Iyer', 'M', '2005-04-14', 9, 'Graduate'),
        (30, 'Kamal Desai', 'M', '2016-06-10', 10, 'Primary'),
        (31, 'eheh', 'M', '2024-04-04', 3, 'Primary');
    """;
    stmt.execute(insertCitizens);
    
    String insertLandRecords = """
        INSERT INTO land_record (land_id, citizen_id, area_acres, crop_type) VALUES 
        (1, 1, 1.5, 'Rice'),
        (2, 2, 0.8, 'Wheat'),
        (3, 3, 2.0, 'Cotton'),
        (4, 4, 0.5, 'Rice'),
        (5, 5, 1.2, 'Maize'),
        (6, 6, 1.8, 'Rice'),
        (7, 7, 0.6, 'Wheat'),
        (8, 8, 2.5, 'Sugarcane'),
        (9, 9, 1.0, 'Rice'),
        (10, 10, 0.9, 'Cotton');
    """;
    stmt.execute(insertLandRecords);

    String insertPanchayatEmployees = """
        INSERT INTO panchayat_employee (employee_id, citizen_id, role) VALUES 
        (1, 1, 'Panchayat Pradhan'),
        (2, 2, 'Secretary'),
        (3, 6, 'Member'),
        (4, 8, 'Treasurer'),
        (5, 10, 'Member'),
        (6, 11, 'Vice President'),
        (7, 12, 'Member'),
        (8, 16, 'Secretary'),
        (9, 18, 'Auditor'),
        (10, 20, 'Treasurer'),
        (11, 21, 'Clerk'),
        (12, 22, 'Supervisor'),
        (13, 23, 'Assistant'),
        (14, 24, 'Coordinator'),
        (15, 25, 'Advisor');
    """;
    stmt.execute(insertPanchayatEmployees);

    String insertAssets = """
        INSERT INTO asset (asset_id, type, location, installation_date) VALUES 
        (1, 'Street Light', 'Phulera', '2024-01-15'),
        (2, 'Street Light', 'Phulera', '2024-02-20'),
        (3, 'Water Pump', 'XYZ Village', '2023-08-10'),
        (4, 'Road', 'ABC Village', '2022-09-05'),
        (5, 'Street Light', 'Phulera', '2024-03-18'),
        (6, 'Water Pump', 'XYZ Village', '2023-10-25'),
        (7, 'Road', 'ABC Village', '2022-11-30'),
        (8, 'Street Light', 'Phulera', '2024-04-15'),
        (9, 'Water Pump', 'XYZ Village', '2023-12-20'),
        (10, 'Road', 'ABC Village', '2023-01-25'),
        (11, 'Street Light', 'XYZ Village', '2024-05-10'),
        (12, 'Street Light', 'ABC Village', '2024-06-15'),
        (13, 'Street Light', 'LMN Village', '2024-07-20'),
        (14, 'Street Light', 'PQR Village', '2024-08-25'),
        (15, 'Street Light', 'DEF Village', '2024-09-30');
    """;
    stmt.execute(insertAssets);

    String insertWelfareSchemes = """
        INSERT INTO welfare_scheme (scheme_id, name, description) VALUES 
        (1, 'MNREGA', 'Employment Guarantee Scheme'),
        (2, 'PMAY', 'Affordable Housing Scheme'),
        (3, 'Midday Meal', 'School Lunch Program'),
        (4, 'PMKSY', 'Irrigation Scheme'),
        (5, 'PMFBY', 'Crop Insurance Scheme');
    """;
    stmt.execute(insertWelfareSchemes);

    String insertSchemeEnrollments = """
        INSERT INTO scheme_enrollment (enrollment_id, citizen_id, scheme_id, enrollment_date) VALUES 
        (1, 2, 1, '2023-07-10'),
        (2, 3, 2, '2024-01-15'),
        (3, 4, 3, '2024-02-01'),
        (4, 5, 1, '2024-03-20'),
        (5, 6, 2, '2024-04-05'),
        (6, 7, 3, '2024-05-10'),
        (7, 8, 1, '2024-06-15'),
        (8, 9, 2, '2024-07-20'),
        (9, 10, 3, '2024-08-25');
    """;
    stmt.execute(insertSchemeEnrollments);

    String insertVaccinations = """
        INSERT INTO vaccination (vaccination_id, citizen_id, vaccine_type, date_administered) VALUES 
        (1, 5, 'Covid-19', '2024-05-20'),
        (2, 7, 'Polio', '2024-03-10'),
        (3, 10, 'Hepatitis', '2024-06-15'),
        (4, 1, 'Covid-19', '2024-07-20'),
        (5, 3, 'Polio', '2024-08-10'),
        (6, 31, 'Polio', '2024-06-06');
    """;
    stmt.execute(insertVaccinations);

    String insertCensusData = """
        INSERT INTO citizen (citizen_id, name, gender, date_of_birth, household_id, education_qualification)
        SELECT 
            (SELECT MAX(citizen_id) FROM citizen) + ROW_NUMBER() OVER (ORDER BY h.household_id),
            'Child ' || ROW_NUMBER() OVER (ORDER BY h.household_id),
            CASE WHEN RANDOM() < 0.5 THEN 'M' ELSE 'F' END,
            DATE '2024-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 365),
            h.household_id,
            'Primary'
        FROM household h
        WHERE h.household_id IN (SELECT household_id FROM household);
    """;
    stmt.execute(insertCensusData);

    String census_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
        SELECT household_id, citizen_id, 'Birth', date_of_birth
            FROM citizen
            WHERE date_of_birth IS NOT NULL;
    """;
    stmt.execute(census_data);

    String death_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
        SELECT household_id, citizen_id, 'Death', 
            DATE '2024-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 365)
        FROM citizen
        WHERE citizen_id NOT IN (SELECT citizen_id FROM panchayat_employee) 
            AND date_of_birth < '1980-01-01';
    """;
    stmt.execute(death_data);

    String marriage_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
        SELECT household_id, citizen_id, 'Marriage', 
            DATE '2000-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 9131)
        FROM citizen
        WHERE date_of_birth < '2000-01-01';
    """;
    stmt.execute(marriage_data);

    String divorce_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
            SELECT household_id, citizen_id, 'Divorce', 
                DATE '2000-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 1826)
            FROM citizen
            WHERE date_of_birth >= '1990-01-01' 
                AND date_of_birth <= '2000-12-31';
    """;
    stmt.execute(divorce_data);

        stmt.close();
    }


    private static void executeQueries(Connection conn) throws SQLException {
        // Query 1: Citizens with more than 1 acre of land
        String query1 = """
            SELECT c.name
            FROM citizen c
            JOIN land_record l ON c.citizen_id = l.citizen_id
            WHERE l.area_acres > 1;
        """;
        
        Statement stmt1 = conn.createStatement();
        ResultSet rs1 = stmt1.executeQuery(query1);

        System.out.println("Citizens with more than 1 acre of land:");
        System.out.println("Name");
        System.out.println("--------------------");
        while (rs1.next()) {
            System.out.println(rs1.getString("name"));
        }
        System.out.println();

        // Query 2: Female students with household income < 1 Lakh
        String query2 = """
            SELECT c.name
            FROM household h
            JOIN citizen c ON c.household_id = h.household_id
            WHERE h.income < 100000.00
            AND c.gender = 'F' 
            AND c.education_qualification in ('Primary', 'Secondary', '10th', '12th');
        """;
        
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(query2);

        System.out.println("Female students with household income < 1 Lakh:");
        System.out.println("Name");
        System.out.println("--------------------");
        while (rs2.next()) {
            System.out.println(rs2.getString("name"));
        }
        System.out.println();

        // Query 3: Total rice cultivation land
        String query3 = """
            SELECT sum(area_acres) as total_acres
            FROM land_record
            WHERE crop_type = 'Rice';
        """;
        
        Statement stmt3 = conn.createStatement();
        ResultSet rs3 = stmt3.executeQuery(query3);

        System.out.println("Total rice cultivation land:");
        System.out.println("Total Acres");
        System.out.println("--------------------");
        while (rs3.next()) {
            System.out.println(rs3.getDouble("total_acres"));
        }
        System.out.println();

        // Query 4: Citizens born after 1.1.2000 with 10th education
        String query4 = """
            SELECT count(citizen_id) as count
            FROM citizen
            WHERE date_of_birth > '2000-01-01' 
            AND education_qualification = '10th';
        """;
        
        Statement stmt4 = conn.createStatement();
        ResultSet rs4 = stmt4.executeQuery(query4);

        System.out.println("Number of citizens born after 1.1.2000 with 10th education:");
        System.out.println("Count");
        System.out.println("--------------------");
        while (rs4.next()) {
            System.out.println(rs4.getInt("count"));
        }
        System.out.println();

        // Query 5: Panchayat employees with more than 1 acre land
        String query5 = """
            SELECT c.name
            FROM citizen c
            JOIN panchayat_employee pe ON c.citizen_id = pe.citizen_id
            JOIN land_record l ON c.citizen_id = l.citizen_id
            WHERE l.area_acres > 1;
        """;
        
        Statement stmt5 = conn.createStatement();
        ResultSet rs5 = stmt5.executeQuery(query5);

        System.out.println("Panchayat employees with more than 1 acre land:");
        System.out.println("Name");
        System.out.println("--------------------");
        while (rs5.next()) {
            System.out.println(rs5.getString("name"));
        }
        System.out.println();

        // Query 6: Household members of Panchayat Pradhan
        String query6 = """
            SELECT c.name
            FROM citizen c
            WHERE c.household_id = (
                SELECT ci.household_id
                FROM citizen ci
                JOIN panchayat_employee pe ON ci.citizen_id = pe.citizen_id
                WHERE pe.role = 'Panchayat Pradhan'
            );
        """;
        
        Statement stmt6 = conn.createStatement();
        ResultSet rs6 = stmt6.executeQuery(query6);

        System.out.println("Household members of Panchayat Pradhan:");
        System.out.println("Name");
        System.out.println("--------------------");
        while (rs6.next()) {
            System.out.println(rs6.getString("name"));
        }
        System.out.println();

        // Query 7: Street lights in Phulera installed in 2024
        String query7 = """
            SELECT count(asset_id) as count
            FROM asset
            WHERE location = 'Phulera' 
            AND installation_date >= '2024-01-01' 
            AND installation_date <= '2024-12-31' 
            AND type = 'Street Light';
        """;
        
        Statement stmt7 = conn.createStatement();
        ResultSet rs7 = stmt7.executeQuery(query7);

        System.out.println("Street lights in Phulera installed in 2024:");
        System.out.println("Count");
        System.out.println("--------------------");
        while (rs7.next()) {
            System.out.println(rs7.getInt("count"));
        }
        System.out.println();

        // Query 8: Vaccinations in 2024 for children of 10th pass citizens
        String query8 = """
            SELECT count(DISTINCT c.citizen_id) as count
            FROM citizen c
            JOIN household h ON c.household_id = h.household_id 
            JOIN citizen c2 ON h.household_id = c2.household_id 
            JOIN vaccination v ON c2.citizen_id = v.citizen_id
            WHERE c.education_qualification = '10th'
            AND v.date_administered >= '2024-01-01'
            AND v.date_administered <= '2024-12-31'
            AND c2.date_of_birth > c.date_of_birth
            AND EXTRACT(YEAR FROM AGE(c2.date_of_birth)) < 18;
        """;
        
        Statement stmt8 = conn.createStatement();
        ResultSet rs8 = stmt8.executeQuery(query8);

        System.out.println("Vaccinations in 2024 for children of 10th pass citizens:");
        System.out.println("Count");
        System.out.println("--------------------");
        while (rs8.next()) {
            System.out.println(rs8.getInt("count"));
        }
        System.out.println();

        // Query 9: Boy births in 2024
        String query9 = """
            SELECT count(event_type) as count
            FROM census_data
            WHERE event_type = 'Birth' 
            AND event_date >= '2024-01-01' 
            AND event_date <= '2024-12-31';
        """;
        
        Statement stmt9 = conn.createStatement();
        ResultSet rs9 = stmt9.executeQuery(query9);

        System.out.println("Boy births in 2024:");
        System.out.println("Count");
        System.out.println("--------------------");
        while (rs9.next()) {
            System.out.println(rs9.getInt("count"));
        }
        System.out.println();

        // Query 10: Citizens in panchayat employee households
        String query10 = """
            SELECT count(citizen_id) as count
            FROM citizen
            WHERE household_id IN (
                SELECT household_id 
                FROM citizen 
                WHERE citizen_id IN (
                    SELECT citizen_id 
                    FROM panchayat_employee
                )
            );
        """;
        
        Statement stmt10 = conn.createStatement();
        ResultSet rs10 = stmt10.executeQuery(query10);

        System.out.println("Citizens in panchayat employee households:");
        System.out.println("Count");
        System.out.println("--------------------");
        while (rs10.next()) {
            System.out.println(rs10.getInt("count"));
        }
        System.out.println();

        // Close all resources
        rs1.close(); stmt1.close();
        rs2.close(); stmt2.close();
        rs3.close(); stmt3.close();
        rs4.close(); stmt4.close();
        rs5.close(); stmt5.close();
        rs6.close(); stmt6.close();
        rs7.close(); stmt7.close();
        rs8.close(); stmt8.close();
        rs9.close(); stmt9.close();
        rs10.close(); stmt10.close();
    }
}