import re
from datetime import datetime
from typing import Any, List, Literal, Optional

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)
from models import SlotStatus

class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

class OutputModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

class VehicleInput(RequestModel):
    plate_number: str
    province: str = ""

    @field_validator("plate_number")
    @classmethod
    def plate_number_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("กรุณากรอกทะเบียนรถ")
        return cleaned_value

    @field_validator("province")
    @classmethod
    def normalize_optional_province(cls, value: str) -> str:
        return value.strip()

class VehicleOut(OutputModel):
    id: int
    plate_number: str
    province: str

class UserCreate(RequestModel):
    name: str
    username: str
    email: EmailStr
    password: str = Field(min_length=8)
    password_confirmation: str = Field(min_length=8)
    license_plate: Optional[str] = None
    vehicles: Optional[List[VehicleInput]] = None

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("กรุณากรอกชื่อ-นามสกุล")
        return cleaned_value

    @field_validator("username")
    @classmethod
    def username_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("กรุณากรอกชื่อผู้ใช้งาน")
        return cleaned_value

    @model_validator(mode="after")
    def passwords_must_match(self):
        if self.password != self.password_confirmation:
            raise ValueError("รหัสผ่านไม่ตรงกัน")
        return self

class UserUpdate(RequestModel):
    username: Optional[str] = None
    password: Optional[str] = Field(default=None, min_length=8)
    password_confirmation: Optional[str] = Field(default=None, min_length=8)
    license_plate: Optional[str] = None
    vehicles: Optional[List[VehicleInput]] = None

    @field_validator("username")
    @classmethod
    def optional_username_must_not_be_blank(
        cls,
        value: Optional[str],
    ) -> Optional[str]:
        if value is None:
            return None
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("กรุณากรอกชื่อผู้ใช้งาน")
        return cleaned_value

    @model_validator(mode="after")
    def password_confirmation_must_match(self):
        if self.password is None and self.password_confirmation is not None:
            raise ValueError(
                "ส่ง password_confirmation ได้เมื่อเปลี่ยนรหัสผ่านเท่านั้น"
            )
        if (
            self.password is not None
            and self.password != self.password_confirmation
        ):
            raise ValueError("รหัสผ่านไม่ตรงกัน")
        return self
    
class UserOut(OutputModel):
    id: int
    name: str
    username: Optional[str] = None
    username_changed_at: Optional[datetime] = None
    email: str
    role: str
    license_plate: Optional[str] = None
    vehicles: List[VehicleOut] = Field(default_factory=list)

class Token(OutputModel):
    access_token: str
    token_type: str

class AdminToken(Token):
    role: Literal["admin"]

class LoginForm(RequestModel):
    identifier: str = Field(
        validation_alias=AliasChoices(
            "identifier",
            "email",
            "username",
            "name",
        ),
    )
    password: str
    
    @field_validator("identifier")
    @classmethod
    def identifier_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("กรุณากรอกข้อมูลให้ครบถ้วน")
        return cleaned_value

class AdminLoginReq(LoginForm):
    pass

class AdminAddReq(RequestModel):
    first_name: str
    last_name: str
    username: str
    email: EmailStr
    password: str = Field(min_length=8)
    password_confirmation: str = Field(min_length=8)

    @field_validator("first_name", "last_name", "username")
    @classmethod
    def required_text_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("This field must not be blank")
        return cleaned_value

    @model_validator(mode="after")
    def passwords_must_match(self):
        if self.password != self.password_confirmation:
            raise ValueError("รหัสผ่านไม่ตรงกัน")
        return self

class AdminListOut(OutputModel):
    id: int
    name: str
    username: Optional[str] = None
    email: str
    created_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None
    last_logout_at: Optional[datetime] = None
    is_online: bool

class AdminDeactivateOut(OutputModel):
    admin_id: int
    status: Literal["disabled"]
    disabled_at: datetime

class MessageOut(OutputModel):
    message: str

class SlotOut(OutputModel):
    id: int
    name: str
    status: SlotStatus

PaymentMethod = Literal[
    "promptpay",
    "card",
    "mobile_banking_ธนาคารกสิกรไทย",
    "mobile_banking_ธนาคารไทยพาณิชย์",
    "mobile_banking_ธนาคารกรุงไทย",
    "mobile_banking_ธนาคารกรุงเทพ",
    "mobile_banking_ธนาคารกรุงศรีอยุธยา",
    "mobile_banking_ธนาคารทหารไทยธนชาต",
    "mobile_banking_ธนาคารออมสิน",
]

class PaymentOut(OutputModel):
    id: int
    booking_id: int
    amount: int
    base_amount: Optional[int] = None
    overstay_fee: Optional[int] = None
    payment_card_id: Optional[int] = None
    card_brand: Optional[str] = None
    card_last4: Optional[str] = None
    method: Optional[str] = None
    status: str
    paid_at: Optional[datetime] = None


class PaymentCreate(RequestModel):
    booking_id: int
    method: PaymentMethod
    payment_card_id: Optional[int] = None

    @model_validator(mode="after")
    def card_id_must_match_payment_method(self):
        if self.method == "card" and self.payment_card_id is None:
            raise ValueError("กรุณาเลือกบัตรที่ต้องการใช้ชำระเงิน")
        if self.method != "card" and self.payment_card_id is not None:
            raise ValueError("payment_card_id ใช้ได้เฉพาะเมื่อ method เป็น card")
        return self

class BookingCreate(RequestModel):
    slot_id: int
    start_time: datetime
    end_time: datetime
    payment_method: PaymentMethod  # For booking eligibility; the paid method may change.
    vehicle_id: Optional[int] = None
    license_plate: Optional[str] = None

    @field_validator("start_time", "end_time")
    @classmethod
    def booking_datetime_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("เวลาเริ่มและสิ้นสุดต้องมี timezone")
        return value

    @model_validator(mode="after")
    def booking_range_and_vehicle_must_be_valid(self):
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if self.vehicle_id is None and not (self.license_plate or "").strip():
            raise ValueError("กรุณาเลือกรถที่ลงทะเบียนไว้")
        return self

class PaymentCardCreate(RequestModel):
    card_number: str
    expiry_date: str
    cvv: str
    holder_name: str

    @field_validator("card_number")
    @classmethod
    def validate_card_number(cls, value: str) -> str:
        normalized = value.replace(" ", "").replace("-", "")
        if not normalized.isdigit() or len(normalized) != 16:
            raise ValueError("หมายเลขบัตรต้องเป็นตัวเลข 16 หลัก")
        return normalized

    @field_validator("expiry_date")
    @classmethod
    def validate_expiry_date(cls, value: str) -> str:
        normalized = value.strip()
        if not re.fullmatch(r"(0[1-9]|1[0-2])/\d{2}", normalized):
            raise ValueError("วันหมดอายุต้องอยู่ในรูปแบบ MM/YY")
        return normalized

    @field_validator("cvv")
    @classmethod
    def validate_cvv(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized.isdigit() or len(normalized) != 3:
            raise ValueError("CVV ต้องเป็นตัวเลข 3 หลัก")
        return normalized

    @field_validator("holder_name")
    @classmethod
    def validate_holder_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("กรุณากรอกชื่อเจ้าของบัตร")
        if not re.fullmatch(r"[A-Za-z .'-]+", normalized):
            raise ValueError("ชื่อเจ้าของบัตรต้องเป็นภาษาอังกฤษ")
        return normalized.upper()

class PaymentCardOut(BaseModel):
    id: int
    card_brand: str
    last4: str
    holder_name: str
    expiry_date: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

class BookingQrTokenOut(OutputModel):
    id: int
    booking_id: int
    booking_code: str
    qr_type: Literal["entry", "payment", "exit"]
    token: str
    qr_url: str
    issued_at: datetime
    expires_at: Optional[datetime] = None
    scanned_at: Optional[datetime] = None
    used_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None


class BookingOut(OutputModel):
    id: int
    slot_id: int
    vehicle_id: Optional[int] = None
    license_plate: Optional[str] = None
    vehicle_plate_number: Optional[str] = None
    vehicle_province: Optional[str] = None
    start_time: datetime
    end_time: datetime
    entered_at: Optional[datetime] = None
    exited_at: Optional[datetime] = None
    status: str
    total_amount: int
    booking_code: str
    created_at: datetime
    slot: SlotOut
    user: UserOut
    payment: Optional[PaymentOut] = None

class DeviceStatusOut(OutputModel):
    device_name: str
    status: str
    last_updated: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    last_processed_at: Optional[datetime] = None

class HardwareLogOut(OutputModel):
    id: int
    device_name: str
    status: str
    detail: Optional[str] = None
    command_id: Optional[str] = None
    source: Optional[str] = None
    created_at: Optional[datetime] = None

class GateStateOut(OutputModel):
    gate: str
    last_action: Optional[str] = None
    source: Optional[str] = None
    command_id: Optional[str] = None
    updated_at: Optional[datetime] = None

class HardwareCommandOut(OutputModel):
    command_id: str
    gate: str
    device_name: str
    target_device: Optional[str] = None
    action: str
    source: str
    status: str
    detail: Optional[str] = None
    payload: Optional[dict[str, Any]] = None
    result: Optional[dict[str, Any]] = None
    published_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    result_received_at: Optional[datetime] = None

AnalyticsPeriod = Literal["daily", "weekly", "monthly"]
class ChartData(OutputModel):
    label: str
    value: float

class AnalyticsOut(OutputModel):
    period: AnalyticsPeriod
    income_total: Optional[int] = None
    usage_percent: Optional[float] = None
    cars_in: Optional[int] = None
    cars_out: Optional[int] = None
    chart: List[ChartData] = Field(default_factory=list)

class DeviceErrorCount(OutputModel):
    device_name: str
    count: int

class HardwareErrorAnalyticsOut(OutputModel):
    period: AnalyticsPeriod
    total_errors: int
    by_device: List[DeviceErrorCount] = Field(default_factory=list)
    history: List[HardwareLogOut] = Field(default_factory=list)
    chart: List[ChartData] = Field(default_factory=list)

class AdminBookingHistoryOut(OutputModel):
    id: int
    slot_name: str
    user_name: str
    username: Optional[str] = None
    license_plate: Optional[str] = None
    vehicle_plate_number: Optional[str] = None
    vehicle_province: Optional[str] = None
    booking_code: str
    booking_status: str
    payment_status: Optional[str] = None
    payment_method: Optional[str] = None
    amount: Optional[int] = None
    booking_created_at: Optional[datetime] = None
    start_time: datetime
    end_time: datetime
    entry_time: Optional[datetime] = None
    exited_at: Optional[datetime] = None
    payment_paid_at: Optional[datetime] = None

class SupportMessageCreate(RequestModel):
    body: str

    @field_validator("body")
    @classmethod
    def body_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("ข้อความต้องไม่เว้นว่าง")
        return cleaned_value

class SupportMessageOut(OutputModel):
    id: int
    user_id: int
    sender_id: int
    sender_role: Literal["user", "admin"]
    body: str
    created_at: datetime
    read_at: Optional[datetime] = None

class SupportConversationOut(OutputModel):
    user_id: int
    user_name: str
    username: Optional[str] = None
    last_message: Optional[str] = None
    unread_count: int
    updated_at: Optional[datetime] = None

class AdminActivityOut(OutputModel):
    id: str
    actor_name: Optional[str] = None
    action: str
    source: str
    occurred_at: datetime

class GateEventOut(OutputModel):
    id: int
    event_id: str
    device_name: str
    gate: str
    sensor: str
    event: str
    command_id: Optional[str] = None
    booking_id: Optional[int] = None
    payload: Optional[dict[str, Any]] = None
    received_at: datetime

class SlotStatusLogOut(OutputModel):
    id: int
    slot_id: int
    status: SlotStatus
    source: str
    recorded_at: datetime

class ServoOverrideReq(RequestModel):
    action: Literal["open", "close"]

class RgbOverrideReq(RequestModel):
    slot_name: Literal["M", "N", "O"]
    status: SlotStatus

class BookingWebSocketEvent(OutputModel):
    type: Literal["booking_updated"] = "booking_updated"
    event: Literal[
        "booking_created",
        "booking_cancelled",
        "booking_time_out",
        "booking_entered",
        "payment_paid",
        "booking_completed",
    ]
    booking_id: int

class PaymentWebSocketEvent(BookingWebSocketEvent):
    event: Literal["payment_paid"] = "payment_paid"

class SlotWebSocketEvent(OutputModel):
    slot: str
    status: SlotStatus
