from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import json, os, asyncio

from database import get_db, engine
from models import Base, User, Slot, Booking, SlotStatus, Payment
from schemas import (
    UserCreate, UserOut, Token, LoginForm,
    SlotOut, BookingCreate, BookingOut, UserUpdate,
    PaymentCreate, PaymentOut
)

load_dotenv()
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Smart Parking API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM  = os.getenv("ALGORITHM", "HS256")
TOKEN_EXP  = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

connected_clients: list[WebSocket] = []

async def broadcast(message: dict):
    for ws in connected_clients.copy():
        try:
            await ws.send_json(message)
        except Exception:
            connected_clients.remove(ws)

# ---- MQTT setup ----
def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        slot_name = payload.get("slot")
        status    = payload.get("status")
        db = next(get_db())
        slot = db.query(Slot).filter(Slot.name == slot_name).first()
        if slot and status in SlotStatus.__members__:
            slot.status = SlotStatus[status]
            db.commit()
            asyncio.run(broadcast({"slot": slot_name, "status": status}))
    except Exception as e:
        print(f"MQTT error: {e}")

mqtt_client = mqtt.Client()
mqtt_client.on_message = on_mqtt_message
try:
    mqtt_client.connect(os.getenv("MQTT_BROKER", "localhost"),
                        int(os.getenv("MQTT_PORT", 1883)))
    mqtt_client.subscribe("parking/slot/#")
    mqtt_client.loop_start()
except Exception:
    print("MQTT broker not available — skipping")

# ---- Helpers ----
def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(minutes=TOKEN_EXP)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

from fastapi.security import HTTPBearer
bearer = HTTPBearer()

def get_current_user(credentials=Depends(bearer), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
    except (JWTError, TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user

# 🌟 ระบบสแกนยกเลิกการจองอัตโนมัติ (No-Show)
async def auto_cancel_no_shows():
    while True:
        await asyncio.sleep(60)  # ให้ระบบตื่นขึ้นมาเช็กทุกๆ 1 นาที
        try:
            db = next(get_db())
            # กำหนดเวลาปัจจุบันลบออก 30 นาที (เพื่อหาบิลที่เลยกำหนดมาแล้ว)
            cutoff_time = datetime.utcnow() - timedelta(minutes=30)
            
            # ค้นหา Booking ที่เลยเวลาเข้าจอดมาแล้ว 30 นาที และ Slot ยังคงค้างอยู่ที่สถานะ reserved
            expired_bookings = db.query(Booking).join(Slot).filter(
                Booking.start_time <= cutoff_time,
                Slot.status == SlotStatus.reserved
            ).all()
            
            for booking in expired_bookings:
                slot = booking.slot
                slot.status = SlotStatus.available # คืนสถานะช่องจอดเป็นว่าง
                
                # ลบการจองที่หมดเวลาออก (หรือเหนือจะเปลี่ยนสถานะเป็น 'cancelled' ก็ได้)
                db.delete(booking) 
                db.commit()
                
                # พ่นคำสั่งสั่งบอร์ด ESP32 ให้เปลี่ยนสถานะไฟเป็นว่าง
                mqtt_client.publish(
                    f"parking/slot/{slot.name}/command",
                    json.dumps({"slot": slot.name, "status": "available"})
                )
                # แจ้งเตือนแอปหน้าบ้านผ่าน WebSocket
                await broadcast({"slot": slot.name, "status": "available"})
                print(f"⏰ Auto-cancelled: Booking ID {booking.id} due to 30-mins no-show.")
                
        except Exception as e:
            print(f"Background task error: {e}")

# ==============================
# AUTH & USERS
# ==============================
@app.post("/auth/register", response_model=UserOut)
def register(body: UserCreate, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(400, "Email already registered")
    user = User(
        name=body.name, email=body.email,
        hashed_password=hash_password(body.password),
        license_plate=body.license_plate
    )
    db.add(user); db.commit(); db.refresh(user)
    return user

@app.post("/auth/login", response_model=Token)
def login(body: LoginForm, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(401, "Invalid email or password")
    return {"access_token": create_token({"sub": str(user.id)}), "token_type": "bearer"}

@app.get("/users/me", response_model=UserOut)
def get_me(current_user: User = Depends(get_current_user)):
    return current_user

@app.put("/users/me", response_model=UserOut)
def update_profile(body: UserUpdate,
                  db: Session = Depends(get_db),
                  current_user: User = Depends(get_current_user)):
    if body.name is not None:
        current_user.name = body.name
    if body.license_plate is not None:
        current_user.license_plate = body.license_plate
    if body.password is not None:
        if len(body.password) < 6:
            raise HTTPException(400, "Password must be at least 6 characters")
        current_user.hashed_password = hash_password(body.password)
    
    db.commit()
    db.refresh(current_user)
    return current_user

# ==============================
# SLOTS
# ==============================
@app.get("/slots", response_model=List[SlotOut])
def get_slots(db: Session = Depends(get_db)):
    return db.query(Slot).all()

@app.get("/slots/{slot_id}", response_model=SlotOut)
def get_slot(slot_id: int, db: Session = Depends(get_db)):
    slot = db.query(Slot).filter(Slot.id == slot_id).first()
    if not slot:
        raise HTTPException(404, "Slot not found")
    return slot

# ==============================
# BOOKINGS
# ==============================

# ---- ค่าจอดรถ ----
RATE_PER_HOUR = 45
MIN_CHARGE    = 45   # ขั้นต่ำ 45 บาท
DAILY_RATE    = 360  # เหมาจ่ายรายวันเมื่อจองตั้งแต่ 8 ชั่วโมงขึ้นไป

def calculate_amount(start: datetime, end: datetime) -> int:
    hours = (end - start).total_seconds() / 3600
    if hours >= 8:
        return DAILY_RATE
    amount = max(MIN_CHARGE, round(hours * RATE_PER_HOUR))
    return amount

# ---- แก้ create_booking ให้คำนวณราคาและสร้าง Payment อัตโนมัติ ----
@app.post("/bookings", response_model=BookingOut)
def create_booking(body: BookingCreate,
                   db: Session = Depends(get_db),
                   current_user: User = Depends(get_current_user)):
    has_pending = db.query(Payment).join(Booking, Payment.booking_id == Booking.id).filter(
        Booking.user_id == current_user.id,
        Payment.status != "paid"
    ).first()

    if has_pending:
        raise HTTPException(400, "รถทะเบียน $_selectedPlate มีการจองค้างอยู่ในระบบแล้ว!")

    slot = db.query(Slot).filter(Slot.id == body.slot_id).first()
    if not slot:
        raise HTTPException(404, "Slot not found")
    if slot.status != SlotStatus.available:
        raise HTTPException(400, f"Slot {slot.name} is not available")
    if body.end_time <= body.start_time:
        raise HTTPException(400, "end_time must be after start_time")

    fresh_user   = db.query(User).filter(User.id == current_user.id).first()
    total_amount = calculate_amount(body.start_time, body.end_time)

    slot.status  = SlotStatus.reserved
    booking = Booking(
        user_id       = fresh_user.id,
        slot_id       = body.slot_id,
        license_plate = fresh_user.license_plate,
        start_time    = body.start_time,
        end_time      = body.end_time,
        total_amount  = total_amount,
    )
    db.add(booking); db.commit(); db.refresh(booking)

    # สร้าง Payment record รอชำระ
    payment = Payment(booking_id=booking.id, amount=total_amount)
    db.add(payment); db.commit()

    mqtt_client.publish(
        f"parking/slot/{slot.name}/command",
        json.dumps({"slot": slot.name, "status": "reserved"})
    )
    db.refresh(booking)
    return booking


@app.get("/bookings/me", response_model=List[BookingOut])
def my_bookings(db: Session = Depends(get_db),
                current_user: User = Depends(get_current_user)):
    return (db.query(Booking)
              .filter(Booking.user_id == current_user.id)
              .order_by(Booking.created_at.desc())
              .all())

@app.delete("/bookings/{booking_id}")
def cancel_booking(booking_id: int,
                  db: Session = Depends(get_db),
                  current_user: User = Depends(get_current_user)):
    booking = db.query(Booking).filter(
        Booking.id == booking_id,
        Booking.user_id == current_user.id
    ).first()
    if not booking:
        raise HTTPException(404, "Booking not found")
    
    booking.slot.status = SlotStatus.available
    db.delete(booking)
    db.commit()
    
    mqtt_client.publish(
        f"parking/slot/{booking.slot.name}/command",
        json.dumps({"slot": booking.slot.name, "status": "available"})
    )
    return {"message": f"Booking {booking_id} cancelled"}


# ==============================
# PAYMENTS
# ==============================
# ---- ดูสถานะ Payment ของ Booking ----
@app.get("/payments/{booking_id}", response_model=PaymentOut)
def get_payment(booking_id: int,
                db: Session = Depends(get_db),
                current_user: User = Depends(get_current_user)):
    payment = db.query(Payment).filter(
        Payment.booking_id == booking_id).first()
    if not payment:
        raise HTTPException(404, "Payment not found")
    return payment


# ---- ยืนยันชำระเงิน ----
@app.post("/payments/confirm", response_model=PaymentOut)
def confirm_payment(body: PaymentCreate,
                    db: Session = Depends(get_db),
                    current_user: User = Depends(get_current_user)):
    
    booking = db.query(Booking).filter(
        Booking.id == body.booking_id,
        Booking.user_id == current_user.id
    ).first()
    if not booking:
        raise HTTPException(404, "Booking not found")

    payment = db.query(Payment).filter(
        Payment.booking_id == body.booking_id).first()
    if not payment:
        raise HTTPException(404, "Payment not found")
    if payment.status == "paid":
        raise HTTPException(400, "Already paid")

    payment.status  = "paid"
    payment.method  = body.method
    payment.paid_at = datetime.now(timezone.utc)

    booking.slot.status = SlotStatus.available
    
    mqtt_client.publish(
        f"parking/slot/{booking.slot.name}/command",
        json.dumps({"slot": booking.slot.name, "status": "available"})
    )

    db.commit()
    db.refresh(payment)

    asyncio.run(broadcast({"slot": booking.slot.name, "status": "available"}))

    return payment

# ==============================
# WEBSOCKET
# ==============================
@app.websocket("/ws/slots")
async def ws_slots(websocket: WebSocket, db: Session = Depends(get_db)):
    await websocket.accept()
    connected_clients.append(websocket)
    slots = db.query(Slot).all()
    await websocket.send_json([{"slot": s.name, "status": s.status} for s in slots])
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)

@app.on_event("startup")
async def startup_event():
    # สั่งให้ระบบยกเลิกอัตโนมัติทำงานเบื้องหลังทันทีที่เปิดเซิร์ฟเวอร์
    asyncio.create_task(auto_cancel_no_shows())