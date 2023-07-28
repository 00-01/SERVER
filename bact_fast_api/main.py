import datetime
from fastapi import FastAPI, Depends, HTTPException
import mysql.connector
from sqlalchemy import Date, Float, create_engine, String, Integer, Column, sql
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
import uvicorn

from pydantic import BaseModel
from typing import List


## ---------------------------------------------------------------- CONSTANTS

id = f"rowan"
pw = f"rowan0810"
host = f"34.64.46.213"
port = f"3306"
db = f"BA_SYS"

## ------------------------------------------------ DATABASE 1

db_config = {
    "host": host,
    "user": id,
    "password": pw,
    "database": db,
}
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

## ------------------------------------------------ DATABASE 2

DB_URL = f"mysql+pymysql://{id}:{pw}@{host}:{port}/{db}"
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


## ---------------------------------------------------------------- FUNCTIONS


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


## ---------------------------------------------------------------- ETC


app = FastAPI()


@app.on_event("startup")
async def startup():
    Base.metadata.create_all(bind=engine)


@app.on_event("shutdown")
async def shutdown():
    pass


## ---------------------------------------------------------------- USER


class User(Base):
    __tablename__ = "user"

    user_id = Column(Integer, primary_key=True, index=True)
    sex = Column(String, index=True)
    age = Column(Integer, index=True)


class UserBase(BaseModel):
    user_id: int
    sex: str
    age: int


class UserCreate(UserBase):
    pass


class UserRead(UserBase):
    user_id: int

    class Config:
        orm_mode = True


@app.post("/user/", response_model=UserCreate)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    result = User(**user.model_dump())
    print([a for a in result.__dir__()[:] if "_" not in a])
    db.add(result)
    db.commit()
    db.refresh(result)
    return result


@app.get("/user/", response_model=List[UserRead])
def read_users(skip: int = 0, limit: int = 30, db: Session = Depends(get_db)):
    result = db.query(User).offset(skip).limit(limit).all()
    print([a for a in result[0].__dir__()[:] if "_" not in a])
    return result


@app.get("/user/{user_id}", response_model=UserRead)
def read_user(user_id: int, db: Session = Depends(get_db)):
    result = db.query(User).filter(User.user_id == user_id).first()
    print([a for a in result.__dir__()[:] if "_" not in a])
    if result is None:
        raise HTTPException(status_code=404, detail="User not found")
    return result


## json return method
@app.get("/test_conn/{user_id}")
def read_item(user_id: int):
    query = "SELECT * FROM BA_SYS.user WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    fetch = cursor.fetchone()
    if not fetch:
        return {"error": "Item not found"}
    result = {
        "user_id": fetch[0],
        "sex": fetch[1],
        "age": fetch[2],
    }
    return result


## ---------------------------------------------------------------- LOG


class Log(Base):
    __tablename__ = "activity_log"

    log_num = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    date = Column(Date, index=True)
    time = Column(String, index=True)
    content_id = Column(Integer, index=True)
    score = Column(Integer, index=True)


class LogBase(BaseModel):
    log_num: int
    user_id: int
    date: datetime.date
    time: str
    content_id: int
    score: int
    # metadata: sql.schema.MetaData


class LogCreate(LogBase):
    pass


class LogRead(LogBase):
    log_num: int

    class Config:
        orm_mode = True


@app.post("/log/", response_model=LogCreate)
def create_user(user: LogCreate, db: Session = Depends(get_db)):
    result = Log(**user.model_dump())
    print([a for a in result.__dir__()[:] if "_" not in a])
    db.add(result)
    db.commit()
    db.refresh(result)
    return result


@app.get("/log/", response_model=List[LogRead])
def read_users(skip: int = 0, limit: int = 30, db: Session = Depends(get_db)):
    result = db.query(Log).offset(skip).limit(limit).all()
    print(type(result[0].metadata))
    print([a for a in result[0].__dir__()[:] if "_" not in a])
    return result


@app.get("/log/{user_id}", response_model=LogRead)
def read_user(user_id: int, db: Session = Depends(get_db)):
    result = db.query(Log).filter(Log.user_id == user_id).first()
    print([a for a in result.__dir__()[:] if "_" not in a])
    if result is None:
        raise HTTPException(status_code=404, detail="Log not found")
    return result


## ---------------------------------------------------------------- LOG_VIEW


class VLog(Base):
    __tablename__ = "activity_log_view"

    log_num = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    sex = Column(String, index=True)
    age = Column(Integer, index=True)
    week = Column(Integer, index=True)
    week_date = Column(Date, index=True)
    weekly_depression_score = Column(Float, index=True)
    date = Column(Date, index=True)
    time = Column(String, index=True)
    content_id = Column(Integer, index=True)
    score = Column(Integer, index=True)


class VLogBase(BaseModel):
    log_num: int
    user_id: int
    sex: str
    age: int
    week: int
    week_date: datetime.date
    weekly_depression_score: float
    date: datetime.date
    time: str
    content_id: int
    score: int
    # metadata: sql.schema.MetaData


# class VLogCreate(VLogBase):
#     pass


class VLogRead(VLogBase):
    log_num: int

    class Config:
        orm_mode = True


# @app.post("/log_view/", response_model=VLogCreate)
# def create_user(user: VLogCreate, db: Session = Depends(get_db)):
#     result = VLog(**user.model_dump())
#     print([a for a in result.__dir__()[:] if "_" not in a])
#     db.add(result)
#     db.commit()
#     db.refresh(result)
#     return result


@app.get("/log_view/", response_model=List[VLogRead])
def read_users(skip: int = 0, limit: int = 30, db: Session = Depends(get_db)):
    result = db.query(VLog).offset(skip).limit(limit).all()
    print(type(result[0].metadata))
    print([a for a in result[0].__dir__()[:] if "_" not in a])
    return result


@app.get("/log_view/{user_id}", response_model=VLogRead)
def read_user(user_id: int, db: Session = Depends(get_db)):
    result = db.query(VLog).filter(VLog.user_id == user_id).first()
    print([a for a in result.__dir__()[:] if "_" not in a])
    if result is None:
        raise HTTPException(status_code=404, detail="Log not found")
    return result


if __name__ == "__main__":
    uvicorn.run(
        app="main:app",
        host="0.0.0.0",
        port=8000,
        # reload=True,
        # loop='uvloop',
        # workers=3,
        # access_log=False,
    )
