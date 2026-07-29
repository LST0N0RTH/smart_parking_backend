from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import List
from jose import jwt, JWTError
from passlib.context import CryptContext
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import paho.mqtt.client as mqtt
import json, os, asyncio

from database import get_db, engine, SessionLocal
from models import Base, User, Slot, Booking, SlotStatus, Payment
from schemas import (
    UserCreate, UserOut, Token, LoginForm,
    SlotOut, BookingCreate, BookingOut, UserUpdate,
    PaymentCreate, PaymentOut
)

load_dotenv()
Base.metadata.create_all(bind=engine)
# ==============================
# BACKGROUND TASKS & LIFESPAN
# ==============================
# 🌟 ระบบสแกนยกเลิกการจองอัตโนมัติ (No-Show)
async def auto_cancel_no_shows():
    while True:
        await asyncio.sleep(30) # ตื่นขึ้นมาเช็กทุกๆ 30 วินาที
        db = SessionLocal()
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=30) # กำหนดเข้าจอดช้าได้ไม่เกิน 30 นาที
            expired_bookings = db.query(Booking).join(Slot).filter( # หาการจองที่เลยเวลาเข้าจอดมาแล้ว 30 นาที และช่องจอดยังคงค้างอยู่ที่สถานะจองแล้ว
                Booking.start_time <= cutoff_time,
                Slot.status == SlotStatus.reserved
            ).all()
            
            for booking in expired_bookings:
                slot = booking.slot
                slot.status = SlotStatus.available # คืนสถานะช่องจอด
                
                db.delete(booking) 
                db.commit()
                
                mqtt_client.publish(
                    f"parking/slot/{slot.name}/command",
                    json.dumps({"slot": slot.name, "status": "available"})
                )
                await broadcast({"slot": slot.name, "status": "available"})
                print(f"⏰ Auto-cancelled: Booking ID {booking.id} due to 30-mins no-show.")
        except Exception as e:
            print(f"Background task error: {e}")
        finally:
            db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(auto_cancel_no_shows())
    yield
    task.cancel()

app = FastAPI(title="Smart Parking API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = os.getenv("SECRET_KEY", "fallback_secret_key")
ALGORITHM  = os.getenv("ALGORITHM", "HS256")
TOKEN_EXP  = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

connected_clients: list[WebSocket] = []

async def broadcast(message: dict):
    for ws in connected_clients.copy():
        try:
            await ws.send_json(message)
        except Exception:
            if ws in connected_clients:
                connected_clients.remove(ws)

# ==============================
# MQTT SETUP
# ==============================
def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        slot_name = payload.get("slot")
        status    = payload.get("status")
        
        db = SessionLocal()
        try:
            slot = db.query(Slot).filter(Slot.name == slot_name).first()
            if slot and status in SlotStatus.__members__:
                slot.status = SlotStatus[status]
                db.commit()
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(broadcast({"slot": slot_name, "status": status}), loop)
                else:
                    asyncio.run(broadcast({"slot": slot_name, "status": status}))
        finally:
            db.close()
    except Exception as e:
        print(f"MQTT error: {e}")

mqtt_client = mqtt.Client()
mqtt_client.on_message = on_mqtt_message
try:
    mqtt_client.connect(os.getenv("MQTT_BROKER", "localhost"), int(os.getenv("MQTT_PORT", 1883)))
    mqtt_client.subscribe("parking/slot/#")
    mqtt_client.loop_start()
except Exception:
    print("MQTT broker not available — skipping")

# ==============================
# HELPERS & AUTHENTICATION
# ==============================
def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(minutes=TOKEN_EXP)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

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

# ==============================
# USERS ROUTES
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
    db.add(user)
    db.commit()
    db.refresh(user)
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
            raise HTTPException(400, "รหัสผ่านต้องมีอย่างน้อย 6 ตัว")
        current_user.hashed_password = hash_password(body.password)
    
    db.commit()
    db.refresh(current_user)
    return current_user

# ==============================
# SLOTS ROUTES
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
# BOOKINGS ROUTES
# ==============================
RATE_PER_HOUR = 25
MIN_CHARGE    = 25  # ชั่วโมงละ 25 บาท
DAILY_RATE    = 250 # เหมาจ่ายรายวันเมื่อจองตั้งแต่ 8 ชั่วโมงขึ้นไปต่อวัน

def calculate_amount(start: datetime, end: datetime) -> int:
    total_hours = (end - start).total_seconds() / 3600 # หาจำนวนชั่วโมงรวมทั้งหมด
    if total_hours < 8:
        return max(MIN_CHARGE, round(total_hours * RATE_PER_HOUR))

    # ตัดแบ่งหาจำนวนวันเต็ม ๆ และเศษชั่วโมงที่เหลือ
    full_days = int(total_hours // 24)       
    remaining_hours = total_hours % 24

    # คำนวณเงินจากเศษชั่วโมงที่เกิน
    remaining_charge = 0
    if remaining_hours > 0:
        if remaining_hours >= 8:
            remaining_charge = DAILY_RATE    
        else:
            remaining_charge = max(MIN_CHARGE, round(remaining_hours * RATE_PER_HOUR)) 
            
    return (full_days * DAILY_RATE) + remaining_charge # คำนวณเงินค่าจอดทั้งหมด: (จำนวนวันx250)+(จำนวนชั่วโมงx25)

@app.post("/bookings", response_model=BookingOut)
def create_booking(body: BookingCreate,
                   db: Session = Depends(get_db),
                   current_user: User = Depends(get_current_user)):
    has_pending = db.query(Payment).join(Booking, Payment.booking_id == Booking.id).filter(
        Booking.user_id == current_user.id,
        Payment.status != "paid"
    ).first()

    if has_pending:
        plate_str = current_user.license_plate if current_user.license_plate else "ของคุณ"
        raise HTTPException(400, f"รถทะเบียน {plate_str} มีการจองค้างอยู่ในระบบแล้ว!")

    slot = db.query(Slot).filter(Slot.id == body.slot_id).first()
    if not slot:
        raise HTTPException(404, "Slot not found")
    if slot.status != SlotStatus.available:
        raise HTTPException(400, f"Slot {slot.name} is not available")
    if body.end_time <= body.start_time:
        raise HTTPException(400, "end_time must be after start_time")

    overlapping = db.query(Booking).filter(
        Booking.slot_id == body.slot_id,
        Booking.status == "active",
        Booking.start_time < body.end_time,
        Booking.end_time > body.start_time
    ).first()
    if overlapping:
        raise HTTPException(409, "ช่องจอดรถนี้ถูกจองแล้ว")
    
    total_amount = calculate_amount(body.start_time, body.end_time)
    slot.status  = SlotStatus.reserved

    booking = Booking(
        user_id       = current_user.id,
        slot_id       = body.slot_id,
        license_plate = current_user.license_plate,
        start_time    = body.start_time,
        end_time      = body.end_time,
        total_amount  = total_amount,
    )
    db.add(booking)
    db.commit()
    db.refresh(booking)

    # สร้าง Payment รอชำระ
    payment = Payment(booking_id=booking.id, amount=total_amount)
    db.add(payment)
    db.commit()

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
# PAYMENTS ROUTES
# ==============================
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
async def confirm_payment(body: PaymentCreate,
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
async def ws_slots(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    db = SessionLocal()
    try:
        slots = db.query(Slot).all()
        await websocket.send_json([{"slot": s.name, "status": s.status} for s in slots])
    finally:
        db.close()
        
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
