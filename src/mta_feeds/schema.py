from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

import os

DB_UN = os.environ['DB_UN']
DB_PW = os.environ['DB_PW']
DB_LOC = os.environ['DB_LOC']

db_string = f'postgres://{DB_UN}:{DB_PW}@{DB_LOC}'
engine = create_engine(db_string)

Base = declarative_base()


class Trip(Base):
    __tablename__ = 'trip_test'

    id = Column(Integer, autoincrement=True,primary_key=True)
    direction = Column(String, nullable=False)
    train = Column(String, nullable=False)
    trip = Column(String, unique=True, nullable=False)

    actions = relationship('Action', backref='trip')

class Stop(Base):
    __tablename__ = 'stop_test'

    id = Column(Integer, autoincrement=True, primary_key=True)
    stop = Column(String, unique=True, nullable=False)
    stop_number = Column(Integer)
    on_line = Column(String)
    stop_name = Column(String)
    zone_id = Column(String)
    stop_url = Column(String)
    parent_station = Column(String)
    stop_lat = Column(Float)
    stop_long = Column(Float)
    location_type = Column(Integer)

    actions = relationship('Action', backref='stop')

class Action(Base):
    __tablename__ = 'action_test'

    id = Column(Integer, autoincrement=True, primary_key=True)
    trip_id = Column(Integer, ForeignKey('trip_test.id'))
    stop_id = Column(Integer, ForeignKey('stop_test.id'))
    type = Column(String, nullable=False)
    time = Column(String, nullable=True)

if __name__ == '__main__':

    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    test_trip = Trip(direction='North', train='2', trip='123')
    test_stop = Stop(stop='ABC123')
    test_action = Action(trip_id=test_trip.id, stop_id=test_stop.id, type='arrival')

    session.add(test_trip)
    session.add(test_stop)
    session.add(test_action)

    print(session.new)
    session.commit()
    session.fetchall()

