from pydantic import BaseModel, EmailStr
from datetime import datetime
from typing import Optional
from models import SlotStatus

class UserCreate(BaseModel):
    name: str
    email: EmailStr
    password: str
    license_plate: Optional[str] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None
    license_plate: Optional[str] = None
    password: Optional[str] = None

class UserOut(BaseModel):
    id: int
    name: str
    email: str
    license_plate: Optional[str]

    class Config:
        from_attributes = True

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

    class Config:
        from_attributes = True

class BookingCreate(BaseModel):
    slot_id: int
    start_time: datetime
    end_time: datetime
    license_plate: str
    
class PaymentOut(BaseModel):
    id        : int
    amount    : int
    method    : str
    status    : str
    paid_at   : Optional[datetime]

    class Config:
        from_attributes = True

class PaymentCreate(BaseModel):
    booking_id : int
    method     : str = "promptpay"  

class BookingOut(BaseModel):
    id            : int
    slot_id       : int
    license_plate : Optional[str]
    start_time    : datetime
    end_time      : datetime
    status        : str
    total_amount  : int
    created_at    : datetime
    slot          : SlotOut
    user          : UserOut
    payment       : Optional[PaymentOut] 

    class Config:
        from_attributes = True