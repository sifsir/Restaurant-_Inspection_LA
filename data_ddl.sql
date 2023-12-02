/* 



Name: Sifra Siregar




 the program is creating a database table to store information about restaurant inspections, and it's populating this table with data from a CSV file.

*/

-- Membuat tabel Transactions

CREATE TABLE table_m3 (
    business_id INT,
    business_name VARCHAR(255),
    business_address VARCHAR(255),
    business_city VARCHAR(100),
    business_state CHAR(2),
    business_postal_code VARCHAR(50),
    business_latitude FLOAT,
    business_longitude FLOAT,
    business_location TEXT,
    business_phone_number VARCHAR(100),
    inspection_id TEXT,
    inspection_date DATE,
    inspection_score FLOAT,
    inspection_type VARCHAR(100),
    violation_id VARCHAR(100),
    violation_description TEXT,
    risk_category VARCHAR(50),
    Neighborhoods FLOAT,
    SF_Find_Neighborhoods FLOAT,
    Current_Police_Districts FLOAT,
    Current_Supervisor_Districts FLOAT,
    Analysis_Neighborhoods FLOAT
);

-- Menginput value tabel Segments
COPY table_m3
FROM '/files/data_raw.csv'
DELIMITER ','
CSV HEADER;