# Firebase User Sync with Apache Airflow

An Apache Airflow ETL workflow that reads users from Firebase Firestore and synchronizes them into multiple MySQL databases based on their assigned user roles.

## Project Overview

This project automates Firebase-to-MySQL user synchronization using an Apache Airflow DAG.

The workflow:

1. Connects to Firebase Firestore.
2. Reads documents from the `user` collection.
3. Extracts user information and assigned roles.
4. Checks the user's role.
5. Synchronizes AM users to the Main MySQL database.
6. Synchronizes AAN users to the AAN MySQL database.
7. Synchronizes AFC users to the AFC MySQL database.
8. Uses MySQL `ON DUPLICATE KEY UPDATE` to update existing users.
9. Commits the changes to the respective databases.
10. Closes all database connections after processing.

## Architecture

Firebase Firestore
        |
        v
Apache Airflow DAG
        |
        +-------------------+
        |                   |
        v                   v
   Role Detection       User Data
        |
        +----------+----------+
        |          |          |
       AM         AAN        AFC
        |          |          |
        v          v          v
    Main DB     AAN DB      AFC DB

## Technologies Used

- Python
- Apache Airflow
- Firebase Firestore
- Firebase Admin SDK
- MySQL
- mysql-connector-python
- Docker-compatible Airflow environment

## Firebase Source

The DAG reads users from:

`user`

Each Firebase document is expected to contain fields such as:

- `fullName`
- `mobile`
- `email`
- `userTypes`
- `creditAmountTotal`
- `debitAmountTotal`
- `active`

Example:

```json
{
    "fullName": "Test User",
    "mobile": "9876543210",
    "email": "test@example.com",
    "userTypes": ["AM"],
    "creditAmountTotal": 1000,
    "debitAmountTotal": 200,
    "active": 1
}
