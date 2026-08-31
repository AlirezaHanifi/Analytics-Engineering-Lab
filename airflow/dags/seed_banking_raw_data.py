from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from utils.clickhouse_manager import ClickHouseManager
from utils.models import (
    Merchant,
    Transaction,
    User,
    UserActivityEvent,
    UserCreditScore,
)

CH_HOST: str = "clickhouse"
CH_PORT: int = 9000


@dag(
    dag_id="seed_banking_raw_data",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "raw", "banking"],
)
def seed_raw_data() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ============================================================
    # SETUP TASK GROUP
    # CHANGED: TaskGroup now contains the tasks but does NOT
    # return a tuple of tasks.
    # ============================================================

    @task_group(group_id="setup")
    def setup_group():
        @task
        def users_schema() -> None:
            db = ClickHouseManager(host=CH_HOST, port=CH_PORT)

            db.execute_query("""
                CREATE TABLE IF NOT EXISTS raw.users (
                    user_id UInt32,
                    age UInt8,
                    income_band String,
                    baseline_credit_score UInt16,
                    join_date Date
                ) ENGINE = MergeTree()
                ORDER BY user_id
            """)

            db.execute_query("TRUNCATE TABLE IF EXISTS raw.users")

        @task
        def merchants_schema() -> None:
            db = ClickHouseManager(host=CH_HOST, port=CH_PORT)

            db.execute_query("""
                CREATE TABLE IF NOT EXISTS raw.merchants (
                    merchant_id UInt32,
                    merchant_name String,
                    category String
                ) ENGINE = MergeTree()
                ORDER BY merchant_id
            """)

            db.execute_query("TRUNCATE TABLE IF EXISTS raw.merchants")

        @task
        def transactions_schema() -> None:
            db = ClickHouseManager(host=CH_HOST, port=CH_PORT)

            db.execute_query("""
                CREATE TABLE IF NOT EXISTS raw.transactions (
                    transaction_id UUID,
                    user_id UInt32,
                    merchant_id UInt32,
                    amount Float32,
                    transaction_time DateTime,
                    status String
                ) ENGINE = MergeTree()
                ORDER BY (transaction_time, merchant_id, user_id)
            """)

            db.execute_query("TRUNCATE TABLE IF EXISTS raw.transactions")

        @task
        def user_activity_schema() -> None:
            db = ClickHouseManager(host=CH_HOST, port=CH_PORT)

            db.execute_query("""
                CREATE TABLE IF NOT EXISTS raw.user_activity_events (
                    event_id UUID,
                    user_id UInt32,
                    event_time DateTime,
                    event_type String,
                    session_id String,
                    channel String,
                    is_new_user UInt8
                ) ENGINE = MergeTree()
                ORDER BY (event_time, user_id)
            """)

            db.execute_query("TRUNCATE TABLE IF EXISTS raw.user_activity_events")

        @task
        def user_credit_schema() -> None:
            db = ClickHouseManager(host=CH_HOST, port=CH_PORT)

            db.execute_query("""
                CREATE TABLE IF NOT EXISTS raw.user_credit_scores (
                    score_date Date,
                    user_id UInt32,
                    credit_score UInt16,
                    risk_band String,
                    loan_eligible UInt8,
                    income_band String,
                    monthly_spend Float32
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (score_date, user_id)
            """)

            db.execute_query("TRUNCATE TABLE IF EXISTS raw.user_credit_scores")

        # CHANGED:
        # No `return (...)`.
        #
        # These five tasks have NO dependencies between each other,
        # therefore Airflow can run all five in parallel.

        users_schema()
        merchants_schema()
        transactions_schema()
        user_activity_schema()
        user_credit_schema()

    # ============================================================
    # BASE DATA TASK GROUP
    # ============================================================

    @task_group(group_id="generate_base_data")
    def generate_base_data_group():
        @task
        def users() -> int:
            import numpy as np

            db = ClickHouseManager(
                host=CH_HOST,
                port=CH_PORT,
            )

            np.random.seed(42)

            n_users: int = 5000

            income_bands: list[str] = [
                "Low",
                "Medium-Low",
                "Medium-High",
                "High",
            ]

            users_data: list[tuple] = []

            for i in range(1, n_users + 1):
                user = User(
                    user_id=i,
                    age=int(np.random.randint(18, 75)),
                    income_band=str(
                        np.random.choice(
                            income_bands,
                            p=[0.2, 0.4, 0.3, 0.1],
                        )
                    ),
                    baseline_credit_score=int(
                        max(
                            300,
                            min(
                                850,
                                np.random.normal(
                                    650,
                                    80,
                                ),
                            ),
                        )
                    ),
                    join_date=(
                        datetime.now().date()
                        - timedelta(
                            days=int(
                                np.random.randint(
                                    0,
                                    1000,
                                )
                            )
                        )
                    ),
                )

                users_data.append(user.to_tuple())

            db.execute_query(
                "INSERT INTO raw.users VALUES",
                users_data,
            )

            return n_users

        @task
        def merchants() -> int:
            import random

            db = ClickHouseManager(
                host=CH_HOST,
                port=CH_PORT,
            )

            categories: list[str] = [
                "Supermarket",
                "Electronics",
                "Digital Services",
                "Restaurant",
                "Transport",
                "Healthcare",
            ]

            merchants_data: list[tuple] = []
            n_merchants: int = 200

            for i in range(1, n_merchants + 1):
                category = random.choice(categories)

                merchant = Merchant(
                    merchant_id=i,
                    merchant_name=f"{category} Merchant {i}",
                    category=category,
                )

                merchants_data.append(merchant.to_tuple())

            db.execute_query(
                "INSERT INTO raw.merchants VALUES",
                merchants_data,
            )

            return n_merchants

        users_task = users()
        merchants_task = merchants()

        # CHANGED:
        # Return ONLY the TaskFlow outputs that downstream
        # tasks actually need.
        return users_task, merchants_task

    # ============================================================
    # DEPENDENT DATA TASK GROUP
    # ============================================================

    @task_group(group_id="generate_dependent_data")
    def generate_dependent_data_group(
        n_users,
        n_merchants,
    ):
        @task
        def transactions(
            n_users: int,
            n_merchants: int,
        ) -> None:
            import random
            import uuid

            import numpy as np

            db = ClickHouseManager(
                host=CH_HOST,
                port=CH_PORT,
            )

            np.random.seed(100)

            n_transactions: int = 150000
            batch_size: int = 50000

            statuses: list[str] = [
                "Completed",
                "Completed",
                "Completed",
                "Failed",
                "Refunded",
            ]

            now = datetime.now()

            for _ in range(
                0,
                n_transactions,
                batch_size,
            ):
                tx_data: list[tuple] = []

                for _ in range(batch_size):
                    transaction = Transaction(
                        transaction_id=uuid.uuid4(),
                        user_id=int(
                            np.random.randint(
                                1,
                                n_users + 1,
                            )
                        ),
                        merchant_id=int(
                            np.random.randint(
                                1,
                                n_merchants + 1,
                            )
                        ),
                        amount=round(
                            float(
                                np.random.exponential(
                                    scale=120.0,
                                )
                            ),
                            2,
                        ),
                        transaction_time=(
                            now
                            - timedelta(
                                minutes=int(
                                    np.random.randint(
                                        0,
                                        500000,
                                    )
                                )
                            )
                        ),
                        status=str(random.choice(statuses)),
                    )

                    tx_data.append(transaction.to_tuple())

                db.execute_query(
                    "INSERT INTO raw.transactions VALUES",
                    tx_data,
                )

        @task
        def activity(n_users: int) -> None:
            import random
            import uuid

            import numpy as np

            db = ClickHouseManager(
                host=CH_HOST,
                port=CH_PORT,
            )

            np.random.seed(200)

            event_types: list[str] = [
                "login",
                "view_product",
                "checkout",
                "search",
                "logout",
            ]

            channels: list[str] = [
                "web",
                "mobile",
                "pos",
                "partner_api",
            ]

            now = datetime.now()
            event_rows: list[tuple] = []

            for _ in range(90000):
                event = UserActivityEvent(
                    event_id=uuid.uuid4(),
                    user_id=int(
                        np.random.randint(
                            1,
                            n_users + 1,
                        )
                    ),
                    event_time=(
                        now
                        - timedelta(
                            minutes=int(
                                np.random.randint(
                                    0,
                                    500000,
                                )
                            )
                        )
                    ),
                    event_type=str(random.choice(event_types)),
                    session_id=(f"session-{uuid.uuid4()}"),
                    channel=str(random.choice(channels)),
                    is_new_user=int(np.random.random() < 0.25),
                )

                event_rows.append(event.to_tuple())

            db.execute_query(
                "INSERT INTO raw.user_activity_events VALUES",
                event_rows,
            )

        @task
        def credit(n_users: int) -> None:
            import numpy as np

            db = ClickHouseManager(
                host=CH_HOST,
                port=CH_PORT,
            )

            np.random.seed(300)

            score_rows: list[tuple] = []

            income_bands: list[str] = [
                "Low",
                "Medium-Low",
                "Medium-High",
                "High",
            ]

            now_date = datetime.now().date()

            for user_id in range(
                1,
                n_users + 1,
            ):
                income_band = str(
                    np.random.choice(
                        income_bands,
                        p=[0.2, 0.4, 0.3, 0.1],
                    )
                )

                baseline = int(
                    np.random.randint(
                        450,
                        850,
                    )
                )

                score = max(
                    300,
                    min(
                        850,
                        baseline
                        + int(
                            np.random.normal(
                                0,
                                40,
                            )
                        ),
                    ),
                )

                risk_band = "Low" if score >= 700 else "Medium" if score >= 550 else "High"

                loan_eligible = int(score >= 640 and income_band in {"Medium-High", "High"})

                monthly_spend = round(
                    float(
                        np.random.uniform(
                            50.0,
                            2000.0,
                        )
                    ),
                    2,
                )

                record = UserCreditScore(
                    score_date=now_date,
                    user_id=user_id,
                    credit_score=score,
                    risk_band=risk_band,
                    loan_eligible=loan_eligible,
                    income_band=income_band,
                    monthly_spend=monthly_spend,
                )

                score_rows.append(record.to_tuple())

            db.execute_query(
                "INSERT INTO raw.user_credit_scores VALUES",
                score_rows,
            )

        # CHANGED:
        # These dependencies are created automatically because
        # n_users/n_merchants are XComArg values.

        transactions_task = transactions(
            n_users=n_users,
            n_merchants=n_merchants,
        )

        activity_task = activity(
            n_users=n_users,
        )

        credit_task = credit(
            n_users=n_users,
        )

        return (
            transactions_task,
            activity_task,
            credit_task,
        )

    # ============================================================
    # DAG STRUCTURE
    # ============================================================

    setup = setup_group()
    users_task, merchants_task = generate_base_data_group()
    transactions_task, activity_task, credit_task = generate_dependent_data_group(
        n_users=users_task,
        n_merchants=merchants_task,
    )
    start >> setup >> [users_task, merchants_task]
    [transactions_task, activity_task, credit_task] >> end


seed_raw_data()
