import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, JSON, ForeignKey, text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

engine = create_engine("sqlite:///database.db", echo=False)
Session = sessionmaker(bind=engine)


def generate_uuid():
    return str(uuid.uuid4())


def utcnow():
    return datetime.now(timezone.utc)


class Job(Base):
    __tablename__ = "job"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False)
    payload = Column(JSON, nullable=True)
    schedule_type = Column(String(20), nullable=False)
    run_at = Column(DateTime, nullable=False)
    interval_seconds = Column(Integer, nullable=True)
    max_retries = Column(Integer, nullable=False, default=0)
    retry_count = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="SCHEDULED")
    created_at = Column(DateTime, nullable=False, default=utcnow)

    executions = relationship("JobExecution", backref="job", order_by="JobExecution.attempt_number.desc()")

    @property
    def next_run_at(self):
        if self.status in ("COMPLETED", "FAILED"):
            return None
        return self.run_at

    def advance_to_next_interval(self):
        now = utcnow()
        next_time = self.run_at + timedelta(seconds=self.interval_seconds)
        while next_time <= now.replace(tzinfo=None):
            next_time += timedelta(seconds=self.interval_seconds)
        self.run_at = next_time
        return self.run_at

    def __repr__(self):
        return f"<Job {self.id} name={self.name} status={self.status}>"


class JobExecution(Base):
    __tablename__ = "job_execution"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    job_id = Column(String(36), ForeignKey("job.id"), nullable=False, index=True)
    attempt_number = Column(Integer, nullable=False)
    started_at = Column(DateTime, nullable=False, default=utcnow)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=True)
    error_message = Column(Text, nullable=True)

    def __repr__(self):
        return f"<JobExecution {self.id} job={self.job_id} attempt={self.attempt_number}>"


Base.metadata.create_all(engine)

with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.commit()
