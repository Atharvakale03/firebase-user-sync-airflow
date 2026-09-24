-- Main Database
-- Database: agrobeet_main

CREATE TABLE IF NOT EXISTS user (
    uid VARCHAR(255) PRIMARY KEY,
    full_name VARCHAR(255),
    mobile VARCHAR(50),
    email VARCHAR(255),
    credit_amount_total DECIMAL(15,2) DEFAULT 0,
    debit_amount_total DECIMAL(15,2) DEFAULT 0,
    active TINYINT DEFAULT 1,
    json_row JSON
);


-- AAN Database
-- Database: agrobeet_aan

CREATE TABLE IF NOT EXISTS jhi_user (
    login VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(255),
    email VARCHAR(255),
    activated TINYINT DEFAULT 1
);


-- AFC Database
-- Database: agrobeet_afc

CREATE TABLE IF NOT EXISTS jhi_user (
    login VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(255),
    email VARCHAR(255),
    activated TINYINT DEFAULT 1,
    firebase_id VARCHAR(255)
);
