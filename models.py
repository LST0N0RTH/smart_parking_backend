from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import enum


class SlotStatus(str, enum.Enum):
    available = "available"
    occupied  = "occupied"
    reserved  = "reserved"

class User(Base):
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    name            = Column(String, nullable=False)
    email           = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    license_plate   = Column(String, nullable=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())

    username        = Column(String, unique=True, index=True, nullable=True)
    role            = Column(String, default="user") 
    is_active       = Column(Boolean, default=True) 

    bookings = relationship("Booking", back_populates="user")
    vehicles = relationship("Vehicle", back_populates="user")

class Vehicle(Base):
    __tablename__ = "vehicles"

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "plate_number",
            "province",
            name="uq_vehicles_user_plate_province",
        ),
    )
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    plate_number = Column(String, nullable=False)
    province = Column(String, nullable=False, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="vehicles")


class Slot(Base):
    __tablename__ = "slots"

    id     = Column(Integer, primary_key=True, index=True)
    name   = Column(String, unique=True, nullable=False)
    status = Column(Enum(SlotStatus), default=SlotStatus.available)

    bookings = relationship("Booking", back_populates="slot")


class Booking(Base):
    __tablename__ = "bookings"

    id            = Column(Integer, primary_key=True, index=True)
    user_id       = Column(Integer, ForeignKey("users.id"), nullable=False, index=True) 
    slot_id       = Column(Integer, ForeignKey("slots.id"), nullable=False, index=True) 
    license_plate = Column(String, nullable=True)
    start_time    = Column(DateTime(timezone=True), nullable=False)
    end_time      = Column(DateTime(timezone=True), nullable=False)
    status        = Column(String, default="active", index=True)
    total_amount  = Column(Integer, default=0)  
    created_at    = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="bookings")
    slot = relationship("Slot", back_populates="bookings")
    payment = relationship("Payment", back_populates="booking")


class Payment(Base):
    __tablename__ = "payments"

    id         = Column(Integer, primary_key=True, index=True)
    booking_id = Column(Integer, ForeignKey("bookings.id"), nullable=False, index=True)
    amount     = Column(Integer, nullable=False)         
    method     = Column(String,  default="promptpay")     
    status     = Column(String,  default="pending")      
    paid_at    = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    booking = relationship("Booking", back_populates="payment")

class DeviceStatus(Base):
    __tablename__ = "devices"

    id           = Column(Integer, primary_key=True, index=True)
    device_name  = Column(String, unique=True, nullable=False, index=True)
    status       = Column(String, nullable=False)
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class HardwareLog(Base):
    __tablename__ = "hardwares"

    id          = Column(Integer, primary_key=True, index=True)
    device_name = Column(String, nullable=False, index=True)
    status      = Column(String, nullable=False)
    detail      = Column(String, nullable=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())