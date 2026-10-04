from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator
from datetime import datetime
from typing import List, Optional
from models import SlotStatus
import re

class VehicleInput(BaseModel):
    plate_number: str
    province: str

    @field_validator("plate_number", "province")
    @classmethod
    def value_must_not_be_blank(cls, value: str) -> str:
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError("Vehicle information must not be blank")
        return cleaned_value

class VehicleOut(VehicleInput):
    id: int
    model_config = ConfigDict(from_attributes=True)

class UserCreate(BaseModel):
    name: str
    email: EmailStr
    password: str
    license_plate: Optional[str] = None
    vehicles: Optional[List[VehicleInput]] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None
    password: Optional[str] = None
    license_plate: Optional[str] = None
    vehicles: Optional[List[VehicleInput]] = None

class UserOut(BaseModel):
    id: int
    name: str
    email: str
    license_plate: Optional[str] = None
    vehicles: List[VehicleOut] = Field(default_factory=list)
    model_config = ConfigDict(from_attributes=True)

class Token(BaseModel):
    access_token: str
    token_type: str

class LoginForm(BaseModel):
    email: str
    password: str

class SlotOut(BaseModel):
    id: int
    name: str
    status: SlotStatus
    model_config = ConfigDict(from_attributes=True)

class BookingCreate(BaseModel):
    slot_id: int
    start_time: datetime
    end_time: datetime
    vehicle_id: Optional[int] = None
    license_plate: Optional[str] = None
    
class PaymentOut(BaseModel):
    id: int
    amount: int
    method: str
    status: str
    paid_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

class PaymentCreate(BaseModel):
    booking_id: int
    method: str = "promptpay"

class PaymentCardCreate(BaseModel):
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

        if not re.fullmatch(r"\d{2}/\d{2}", normalized):
            raise ValueError("วันหมดอายุต้องอยู่ในรูปแบบ DD/YY")
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

class BookingQrTokenOut(BaseModel):
    id: int
    booking_id: int
    booking_code: str
    qr_type: str
    token: str
    issued_at: datetime
    expires_at: datetime
    scanned_at: Optional[datetime] = None
    used_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None

class BookingOut(BaseModel):
    id: int
    slot_id: int
    license_plate: Optional[str] = None
    start_time: datetime
    end_time: datetime
    status: str
    total_amount: int
    booking_code: str
    created_at: datetime
    slot: SlotOut
    user: UserOut
    payment: Optional[PaymentOut] = None

    model_config = ConfigDict(from_attributes=True)

class DeviceStatusOut(BaseModel):
    device_name: str
    status: str
    last_updated: datetime
    model_config = ConfigDict(from_attributes=True)

class HardwareLogOut(BaseModel):
    id: int
    device_name: str
    status: str
    detail: Optional[str] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)

class ChartData(BaseModel):
    label: str
    value: float

class AnalyticsOut(BaseModel):
    daily_income: int
    usage_percent: float
    in_out_count: str
    weekly_chart: list[ChartData]