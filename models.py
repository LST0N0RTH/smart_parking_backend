from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
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
    username_changed_at = Column(DateTime(timezone=True), nullable=True)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    last_logout_at = Column(DateTime(timezone=True), nullable=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
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
    vehicle_plate_number = Column(String, nullable=True)
    vehicle_province = Column(String, nullable=True)
    start_time    = Column(DateTime(timezone=True), nullable=False)
    end_time      = Column(DateTime(timezone=True), nullable=False)
    entered_at    = Column(DateTime(timezone=True), nullable=True)
    exited_at     = Column(DateTime(timezone=True), nullable=True)
    status        = Column(String, default="active", index=True)
    total_amount  = Column(Integer, default=0)  
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    booking_code = Column(String, nullable=False, unique=True, index=True)
    qr_tokens = relationship("BookingQrToken", back_populates="booking", cascade="all, delete-orphan",)

    user = relationship("User", back_populates="bookings")
    slot = relationship("Slot", back_populates="bookings")
    payment = relationship("Payment", back_populates="booking", uselist=False,)
    
class Payment(Base):
    __tablename__ = "Payments"

    id         = Column(Integer, primary_key=True, index=True)
    booking_id = Column(Integer, ForeignKey("Bookings.id"), nullable=False, unique=True, index=True)
    amount     = Column(Integer, nullable=False)         
    method     = Column(String, nullable=True)
    status     = Column(String,  default="pending")      
    paid_at    = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    base_amount = Column(Integer, nullable=True)
    overstay_fee = Column(Integer, nullable=True)
    payment_card_id = Column(Integer, ForeignKey("PaymentCards.id", ondelete="SET NULL"), nullable=True, index=True,)
    card_brand = Column(String, nullable=True)
    card_last4 = Column(String, nullable=True)

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
    last_seen_at = Column(DateTime(timezone=True), nullable=True)
    last_processed_at = Column(DateTime(timezone=True), nullable=True)

class HardwareLog(Base):
    __tablename__ = "Hardwares"

    id = Column(Integer, primary_key=True, index=True)
    device_name = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)
    detail = Column(String, nullable=True)
    command_id = Column(String, nullable=True, index=True)
    source = Column(String, nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(),)

class GateState(Base):
    __tablename__ = "GateStates"

    id = Column(Integer, primary_key=True, index=True)
    gate = Column(String, unique=True, nullable=False, index=True)
    last_action = Column(String, nullable=True)  # open / close
    source = Column(String, nullable=True)
    command_id = Column(String, nullable=True, index=True)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

class BookingQrToken(Base):
    __tablename__ = "BookingQrTokens"

    id = Column(Integer, primary_key=True, index=True)
    booking_id = Column(
        Integer,
        ForeignKey("Bookings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    qr_type = Column(String, nullable=False)  # entry / payment / exit
    token = Column(String, unique=True, nullable=False, index=True)
    issued_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)
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

    gate = Column(String, nullable=False)  # entry / exit / slot / admin
    device_name = Column(String, nullable=False)
    action = Column(String, nullable=False)  # open / close / set_status
    source = Column(String, nullable=False)
    status = Column(String, nullable=False)  # queued / acknowledged / failed / no_response
    detail = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    published_at = Column(DateTime(timezone=True), nullable=True)
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    target_device = Column(String, nullable=True, index=True)
    payload = Column(JSON, nullable=True)
    result = Column(JSON, nullable=True)
    result_received_at = Column(DateTime(timezone=True), nullable=True)

    qr_token = relationship("BookingQrToken", back_populates="commands")

class AuthSession(Base):
    __tablename__ = "AuthSessions"

    token_id = Column(String, primary_key=True)
    user_id = Column(Integer, ForeignKey("Users.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=True)
    logged_out_at = Column(DateTime(timezone=True), nullable=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True)

class GateEvent(Base):
    __tablename__ = "GateEvents"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, nullable=False, unique=True, index=True)
    device_name = Column(String, nullable=False)
    gate = Column(String, nullable=False)
    sensor = Column(String, nullable=False)
    event = Column(String, nullable=False, index=True)
    command_id = Column(
        String,
        ForeignKey("HardwareCommands.command_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    booking_id = Column(
        Integer,
        ForeignKey("Bookings.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    payload = Column(JSON, nullable=True)
    received_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

class SlotStatusLog(Base):
    __tablename__ = "SlotStatusLogs"

    id = Column(Integer, primary_key=True, index=True)
    slot_id = Column(Integer, ForeignKey("Slots.id"), nullable=False, index=True)
    status = Column(Enum(SlotStatus), nullable=False)
    source = Column(String, nullable=False)
    recorded_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

class SupportMessage(Base):
    __tablename__ = "SupportMessages"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("Users.id"), nullable=False, index=True)
    sender_id = Column(Integer, ForeignKey("Users.id"), nullable=False, index=True)
    sender_role = Column(String, nullable=False)
    body = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    read_at = Column(DateTime(timezone=True), nullable=True)
