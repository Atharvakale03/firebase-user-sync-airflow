from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import firebase_admin
from firebase_admin import credentials, firestore
import mysql.connector
import json
import os


def firebase_user_sync():

    # Firebase Init
    if not firebase_admin._apps:
        cred = credentials.Certificate(
            "/opt/airflow/serviceAccountKey.json"
        )
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    # MySQL configuration
    mysql_password = os.getenv("MYSQL_PASSWORD")

    # Main DB
    main_conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password=mysql_password,
        database="agrobeet_main"
    )

    # AAN DB
    aan_conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password=mysql_password,
        database="agrobeet_aan"
    )

    # AFC DB
    afc_conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password=mysql_password,
        database="agrobeet_afc"
    )

    main_cursor = main_conn.cursor()
    aan_cursor = aan_conn.cursor()
    afc_cursor = afc_conn.cursor()

    users_ref = db.collection("user")
    docs = users_ref.stream()

    for doc in docs:

        data = doc.to_dict()
        uid = doc.id

        print("Processing:", uid)

        roles = data.get("userTypes", [])

        # AM → MAIN DB
        if "AM" in roles:
            try:
                main_cursor.execute("""
                    INSERT INTO user (
                        uid,
                        full_name,
                        mobile,
                        email,
                        credit_amount_total,
                        debit_amount_total,
                        active,
                        json_row
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)

                    ON DUPLICATE KEY UPDATE
                        full_name=VALUES(full_name),
                        mobile=VALUES(mobile),
                        email=VALUES(email),
                        credit_amount_total=VALUES(credit_amount_total),
                        debit_amount_total=VALUES(debit_amount_total),
                        active=VALUES(active)
                """, (
                    uid,
                    data.get("fullName"),
                    data.get("mobile"),
                    data.get("email"),
                    data.get("creditAmountTotal", 0),
                    data.get("debitAmountTotal", 0),
                    data.get("active", 1),
                    json.dumps(data)
                ))

                main_conn.commit()
                print("Inserted in MAIN")

            except Exception as e:
                print("MAIN DB Error:", e)

        # AAN DB
        if "AAN" in roles:
            try:
                aan_cursor.execute("""
                    INSERT INTO jhi_user (
                        login,
                        password_hash,
                        email,
                        activated
                    )
                    VALUES (%s,%s,%s,%s)

                    ON DUPLICATE KEY UPDATE
                        email=VALUES(email),
                        activated=VALUES(activated)
                """, (
                    data.get("email"),
                    "DUMMY_HASH",
                    data.get("email"),
                    1
                ))

                aan_conn.commit()
                print("Inserted in AAN")

            except Exception as e:
                print("AAN DB Error:", e)

        # AFC DB
        if "AFC" in roles:
            try:
                afc_cursor.execute("""
                    INSERT INTO jhi_user (
                        login,
                        password_hash,
                        email,
                        activated,
                        firebase_id
                    )
                    VALUES (%s,%s,%s,%s,%s)

                    ON DUPLICATE KEY UPDATE
                        email=VALUES(email),
                        activated=VALUES(activated)
                """, (
                    data.get("email"),
                    "DUMMY_HASH",
                    data.get("email"),
                    1,
                    uid
                ))

                afc_conn.commit()
                print("Inserted in AFC")

            except Exception as e:
                print("AFC DB Error:", e)

    main_cursor.close()
    aan_cursor.close()
    afc_cursor.close()

    main_conn.close()
    aan_conn.close()
    afc_conn.close()

    print("Firebase User Sync Completed")


default_args = {
    "owner": "atharva",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


dag = DAG(
    "firebase_user_sync",
    default_args=default_args,
    description="Sync Firebase users to MySQL databases",
    schedule_interval=None,
    catchup=False,
)


sync_task = PythonOperator(
    task_id="firebase_user_sync_task",
    python_callable=firebase_user_sync,
    dag=dag,
)
