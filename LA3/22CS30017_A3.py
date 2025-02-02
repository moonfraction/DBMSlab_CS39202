import psycopg2

def main():
    conn_params = {
        "host": "10.5.18.72",
        "database": "22CS30017",
        "user": "22CS30017",
        "password": "Asdfghjkl@1234567890",
        "port": "5432"
    }

    try:
        # Connect to database
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        print("Connected to database successfully\n")

        # Create tables and insert data
        create_tables(cursor)
        insert_data(cursor)
        conn.commit()

        # Execute queries
        execute_queries(cursor)

        # Close cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error: {e}")

def create_tables(cursor):
    # Drop existing tables
    drop_tables = """
        DROP TABLE IF EXISTS household, citizen, land_record, panchayat_employee, 
        asset, welfare_scheme, scheme_enrollment, vaccination, census_data CASCADE;
    """
    cursor.execute(drop_tables)

    # Create tables
    tables = [
        """
        CREATE TABLE household (
            household_id INT PRIMARY KEY,
            address TEXT NOT NULL,
            income DECIMAL(10, 2) NOT NULL
        );
        """,
        """
        CREATE TABLE citizen (
            citizen_ID INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            gender CHAR(1) NOT NULL,
            date_of_birth DATE NOT NULL,
            household_id INT,
            education_qualification VARCHAR(50),
            FOREIGN KEY (household_id) REFERENCES household(household_id)
        );
        """,
        """
        CREATE TABLE land_record (
            land_id INT PRIMARY KEY,
            citizen_id INT,
            area_acres DECIMAL(10, 2) NOT NULL,
            crop_type TEXT NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """,
        """
        CREATE TABLE panchayat_employee (
            employee_id INT PRIMARY KEY,
            citizen_id INT,
            role TEXT NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """,
        """
        CREATE TABLE asset (
            asset_id INT PRIMARY KEY,
            type TEXT NOT NULL,
            location TEXT NOT NULL,
            installation_date DATE NOT NULL
        );
        """,
        """
        CREATE TABLE welfare_scheme (
            scheme_id INT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        );
        """,
        """
        CREATE TABLE scheme_enrollment (
            enrollment_id INT PRIMARY KEY,
            citizen_id INT,
            scheme_id INT,
            enrollment_date DATE NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id),
            FOREIGN KEY (scheme_id) REFERENCES welfare_scheme(scheme_id)
        );
        """,
        """
        CREATE TABLE vaccination (
            vaccination_id INT PRIMARY KEY,
            citizen_id INT,
            vaccine_type TEXT NOT NULL,
            date_administered DATE NOT NULL,
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """,
        """
        CREATE TABLE census_data (
            household_id INT,
            citizen_id INT,
            event_type TEXT NOT NULL,
            event_date DATE NOT NULL,
            FOREIGN KEY (household_id) REFERENCES household(household_id),
            FOREIGN KEY (citizen_id) REFERENCES citizen(citizen_id)
        );
        """
    ]

    for table in tables:
        cursor.execute(table)

def insert_data(cursor):
    # Insert household data
    household_data = """
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
    """
    cursor.execute(household_data)

    # Insert citizen data
    citizen_data = """
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
    """
    cursor.execute(citizen_data)

    # Insert land record data
    land_record_data = """
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
    """
    cursor.execute(land_record_data)
    
    # Insert panachayat employee data
    panchayat_employee_data = """
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
    """
    cursor.execute(panchayat_employee_data)

    # Insert asset data
    asset_data = """
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
    """
    cursor.execute(asset_data)

    # Insert welfare scheme data
    welfare_scheme_data = """
        INSERT INTO welfare_scheme (scheme_id, name, description) VALUES 
        (1, 'MNREGA', 'Employment Guarantee Scheme'),
        (2, 'PMAY', 'Affordable Housing Scheme'),
        (3, 'Midday Meal', 'School Lunch Program'),
        (4, 'PMKSY', 'Irrigation Scheme'),
        (5, 'PMFBY', 'Crop Insurance Scheme');
    """
    cursor.execute(welfare_scheme_data)

    # Insert scheme enrollment data
    scheme_enrollment_data = """
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
    """
    cursor.execute(scheme_enrollment_data)

    # Insert vaccination data
    vaccination_data = """
        INSERT INTO vaccination (vaccination_id, citizen_id, vaccine_type, date_administered) VALUES 
        (1, 5, 'Covid-19', '2024-05-20'),
        (2, 7, 'Polio', '2024-03-10'),
        (3, 10, 'Hepatitis', '2024-06-15'),
        (4, 1, 'Covid-19', '2024-07-20'),
        (5, 3, 'Polio', '2024-08-10'),
        (6, 31, 'Polio', '2024-06-06');
    """
    cursor.execute(vaccination_data)

    # add child birth events in citizen data for year 2024
    child_birth_data = """
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
    """
    cursor.execute(child_birth_data)

    # census data
    census_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
        SELECT household_id, citizen_id, 'Birth', date_of_birth
            FROM citizen
            WHERE date_of_birth IS NOT NULL;
    """
    cursor.execute(census_data)

    # query to add death event
    death_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
        SELECT household_id, citizen_id, 'Death', 
            DATE '2024-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 365)
        FROM citizen
        WHERE citizen_id NOT IN (SELECT citizen_id FROM panchayat_employee) 
            AND date_of_birth < '1980-01-01';
    """
    cursor.execute(death_data)

    # query to add marriage event
    marriage_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
        SELECT household_id, citizen_id, 'Marriage', 
            DATE '2000-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 9131)
        FROM citizen
        WHERE date_of_birth < '2000-01-01';
    """
    cursor.execute(marriage_data)

    # query to add divorce event
    divorce_data = """
        INSERT INTO census_data (household_id, citizen_id, event_type, event_date)
            SELECT household_id, citizen_id, 'Divorce', 
                DATE '2000-01-01' + INTERVAL '1' DAY * FLOOR(RANDOM() * 1826)
            FROM citizen
            WHERE date_of_birth >= '1990-01-01' 
                AND date_of_birth <= '2000-12-31';
    """
    cursor.execute(divorce_data)


def execute_queries(cursor):
    queries = [
        # Query 1: Citizens with more than 1 acre of land
        """
        SELECT c.name
            FROM citizen c
                JOIN land_record l 
                    ON c.citizen_id = l.citizen_id
            WHERE l.area_acres > 1;
        """,
        
        # Query 2: Female students with low household income
        """
        SELECT c.name
            FROM household h
                JOIN citizen c 
                    ON c.household_id = h.household_id
            WHERE h.income < 100000.00
                AND c.gender = 'F' 
                AND c.education_qualification in ('Primary', 'Secondary', '10th', '12th');
        """,
        
        # Query 3: Total rice cultivation land
        """
        SELECT sum(area_acres) as total_acres
            FROM land_record
            WHERE crop_type = 'Rice';
        """,

        # Number of citizens who are born after 1.1.2000 and have educational qualification of 10th class
        """
        SELECT count(citizen_id) as count
            FROM citizen
            WHERE date_of_birth > '2000-01-01' 
                AND education_qualification = '10th';
        """,

        # Name of all employees of panchayat who also hold more than 1 acre land
        """
        SELECT c.name
            FROM citizen c
                JOIN panchayat_employee pe 
                    ON c.citizen_id = pe.citizen_id
                JOIN land_record l 
                    ON c.citizen_id = l.citizen_id
            WHERE l.area_acres > 1;
        """,

        # Name of the household members of Panchayat Pradhan
        """
        SELECT c.name
        FROM citizen c
        WHERE c.household_id = (
            SELECT ci.household_id
            FROM citizen ci
                JOIN panchayat_employee pe 
                    ON ci.citizen_id = pe.citizen_id
            WHERE pe.role = 'Panchayat Pradhan'
        );
        """,

        # Total number of street light assets installed in a particular locality named Phulera that are installed in 2024
        """
        SELECT count(asset_id) as count
            FROM asset
            WHERE location = 'Phulera' 
                AND installation_date >= '2024-01-01' 
                AND installation_date <= '2024-12-31' 
                AND type = 'Street Light';
        """,

        # Number of vaccinations done in 2024 for the children of citizens whose educational qualification is class 10
        """
        SELECT count(DISTINCT c.citizen_id) as count
            FROM citizen c
                -- Join household to get household details of the citizen
                JOIN household h ON c.household_id = h.household_id 
                -- Join citizen again to get other members of the same household
                JOIN citizen c2 ON h.household_id = c2.household_id 
                -- Join vaccination to get vaccination details of household members
                JOIN vaccination v ON c2.citizen_id = v.citizen_id
            WHERE c.education_qualification = '10th' -- Filter citizens with education qualification of class 10
                AND v.date_administered >= '2024-01-01' -- Filter vaccinations administered in 2024
                AND v.date_administered <= '2024-12-31'
                AND c2.date_of_birth > c.date_of_birth -- Filter children of the citizens
                AND EXTRACT(YEAR FROM AGE(c2.date_of_birth)) < 18; -- Check if child's age is less than 18
        """,

        # Total number of births of boy child in the year 2024
        """
        SELECT count(event_type) as count
            FROM census_data
            WHERE event_type = 'Birth' 
                AND event_date >= '2024-01-01' 
                AND event_date <= '2024-12-31';
        """,

        # Number of citizens who belong to the household of at least one panchayat employee
        """
        SELECT count(citizen_id) as count
            FROM citizen
            WHERE household_id IN 
                (
                    SELECT household_id 
                        FROM citizen 
                    WHERE citizen_id IN 
                        (
                            SELECT citizen_id 
                                FROM panchayat_employee
                        )
                );
        """,

    ]

    # Execute each query
    for i, query in enumerate(queries):
        print(f"\nQuery {i+1}:")
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("No results found")
            continue

        # Get column names
        colnames = [desc[0] for desc in cursor.description]
        
        # Get max width for each column
        widths = []
        for col in range(len(colnames)):
            # Include column name length in width calculation
            width = len(colnames[col])
            # Check width of data in this column
            for row in rows:
                width = max(width, len(str(row[col])))
            widths.append(width + 2)  # Add padding
        
        # Print header
        header = ""
        separator = ""
        for i, col in enumerate(colnames):
            header += f" {col:<{widths[i]}}"
            separator += "-" * (widths[i] + 1)
        
        print(header)
        print(separator)
        
        # Print rows
        for row in rows:
            line = ""
            for i, val in enumerate(row):
                line += f" {str(val):<{widths[i]}}"
            print(line)
        
        print(f"({len(rows)} rows)")

if __name__ == "__main__":
    main()