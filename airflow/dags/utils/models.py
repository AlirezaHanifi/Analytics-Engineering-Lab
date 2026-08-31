from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID


@dataclass
class User:
    user_id: int
    age: int
    income_band: str
    baseline_credit_score: int
    join_date: date

    def to_tuple(self) -> tuple[int, int, str, int, date]:
        return (self.user_id, self.age, self.income_band, self.baseline_credit_score, self.join_date)


@dataclass
class Merchant:
    merchant_id: int
    merchant_name: str
    category: str

    def to_tuple(self) -> tuple[int, str, str]:
        return (self.merchant_id, self.merchant_name, self.category)


@dataclass
class Transaction:
    transaction_id: UUID
    user_id: int
    merchant_id: int
    amount: float
    transaction_time: datetime
    status: str

    def to_tuple(self) -> tuple[UUID, int, int, float, datetime, str]:
        return (self.transaction_id, self.user_id, self.merchant_id, self.amount, self.transaction_time, self.status)


@dataclass
class UserActivityEvent:
    event_id: UUID
    user_id: int
    event_time: datetime
    event_type: str
    session_id: str
    channel: str
    is_new_user: int

    def to_tuple(self) -> tuple[UUID, int, datetime, str, str, str, int]:
        return (
            self.event_id,
            self.user_id,
            self.event_time,
            self.event_type,
            self.session_id,
            self.channel,
            self.is_new_user,
        )


@dataclass
class UserCreditScore:
    score_date: date
    user_id: int
    credit_score: int
    risk_band: str
    loan_eligible: int
    income_band: str
    monthly_spend: float

    def to_tuple(self) -> tuple[date, int, int, str, int, str, float]:
        return (
            self.score_date,
            self.user_id,
            self.credit_score,
            self.risk_band,
            self.loan_eligible,
            self.income_band,
            self.monthly_spend,
        )
