from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator
from datetime import datetime
from typing import List, Optional
from models import SlotStatus

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
    license_plate: str
    
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

class BookingOut(BaseModel):
    id: int
    slot_id: int
    license_plate: Optional[str] = None
    start_time: datetime
    end_time: datetime
    status: str
    total_amount: int
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