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
    __tablename__ = "Users"

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
    payment_cards = relationship("PaymentCard",back_populates="user",cascade="all, delete-orphan")

class Vehicle(Base):
    __tablename__ = "Vehicles"

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "plate_number",
            "province",
            name="uq_vehicles_user_plate_province",
        ),
    )
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("Users.id"), nullable=False, index=True,)
    plate_number = Column(String, nullable=False)
    province = Column(String, nullable=False, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="vehicles")

class Slot(Base):
    __tablename__ = "Slots"

    id     = Column(Integer, primary_key=True, index=True)
    name   = Column(String, unique=True, nullable=False)
    status = Column(Enum(SlotStatus), default=SlotStatus.available)

    bookings = relationship("Booking", back_populates="slot")


class Booking(Base):
    __tablename__ = "Bookings"

    id            = Column(Integer, primary_key=True, index=True)
    user_id       = Column(Integer, ForeignKey("Users.id"), nullable=False, index=True) 
    slot_id       = Column(Integer, ForeignKey("Slots.id"), nullable=False, index=True) 
    vehicle_id    = Column(Integer, ForeignKey("Vehicles.id", ondelete="SET NULL"), nullable=True, index=True,)
    license_plate = Column(String, nullable=True)
    start_time    = Column(DateTime(timezone=True), nullable=False)
    end_time      = Column(DateTime(timezone=True), nullable=False)
    status        = Column(String, default="active", index=True)
    total_amount  = Column(Integer, default=0)  
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    booking_code = Column(String, nullable=False, index=True)
    qr_tokens = relationship("BookingQrToken", back_populates="booking", cascade="all, delete-orphan",)

    user = relationship("User", back_populates="bookings")
    slot = relationship("Slot", back_populates="bookings")
    payment = relationship("Payment", back_populates="booking", uselist=False,)
    
class Payment(Base):
    __tablename__ = "Payments"

    id         = Column(Integer, primary_key=True, index=True)
    booking_id = Column(Integer, ForeignKey("Bookings.id"), nullable=False, index=True)
    amount     = Column(Integer, nullable=False)         
    method     = Column(String,  default="promptpay")     
    status     = Column(String,  default="pending")      
    paid_at    = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    booking = relationship("Booking", back_populates="payment")

class PaymentCard(Base):
    __tablename__ = "PaymentCards"

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "card_number",
            name="uq_payment_cards_user_card_number",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("Users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    card_number = Column(String, nullable=False)
    expiry_date = Column(String, nullable=False)
    cvv = Column(String, nullable=False)
    holder_name = Column(String, nullable=False)
    card_brand = Column(String, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(),)
    user = relationship("User", back_populates="payment_cards")

    @property
    def last4(self) -> str:
        return self.card_number[-4:]

class DeviceStatus(Base):
    __tablename__ = "Devices"

    id           = Column(Integer, primary_key=True, index=True)
    device_name  = Column(String, unique=True, nullable=False, index=True)
    status       = Column(String, nullable=False)
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class HardwareLog(Base):
    __tablename__ = "Hardwares"

    id          = Column(Integer, primary_key=True, index=True)
    device_name = Column(String, nullable=False, index=True)
    status      = Column(String, nullable=False)
    detail      = Column(String, nullable=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())

class BookingQrToken(Base):
    __tablename__ = "BookingQrTokens"

    id = Column(Integer, primary_key=True, index=True)
    booking_id = Column(
        Integer,
        ForeignKey("Bookings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    qr_type = Column(String, nullable=False)  # entry / exit
    token = Column(String, unique=True, nullable=False, index=True)
    issued_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at = Column(DateTime(timezone=True), nullable=False)
    scanned_at = Column(DateTime(timezone=True), nullable=True)
    used_at = Column(DateTime(timezone=True), nullable=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True)

    booking = relationship("Booking", back_populates="qr_tokens")
    commands = relationship("HardwareCommand", back_populates="qr_token",)

class HardwareCommand(Base):
    __tablename__ = "HardwareCommands"

    id = Column(Integer, primary_key=True, index=True)
    command_id = Column(String, unique=True, nullable=False, index=True)
    qr_token_id = Column(
        Integer,
        ForeignKey("BookingQrTokens.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    booking_id = Column(
        Integer,
        ForeignKey("Bookings.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    gate = Column(String, nullable=False)       # entry / exit
    device_name = Column(String, nullable=False)
    action = Column(String, nullable=False)     # open / close
    source = Column(String, nullable=False)     # qr / admin
    status = Column(String, nullable=False)     # queued / acknowledged / failed
    detail = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(),)
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)

    qr_token = relationship("BookingQrToken", back_populates="commands")