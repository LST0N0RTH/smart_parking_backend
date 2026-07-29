from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from pydantic import BaseModel
from datetime import datetime
from database import get_db
import models

from routers.auth import get_current_user 

router = APIRouter(prefix="/admin", tags=["admin"])
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Schemas สำหรับรับข้อมูล
class AdminLogin(BaseModel):
    username: str
    password: str

class AdminCreate(BaseModel):
    first_name: str
    last_name: str
    username: str
    password: str

class UserStatusUpdate(BaseModel):
    is_active: bool

# Admin Login
def get_current_admin(current_user: models.User = Depends(get_current_user)):
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="บัญชีนี้ไม่มีสิทธิ์เข้าถึงฟีเจอร์นี้"
        )
    return current_user\
    
@router.post("/login")
def admin_login(data: AdminLogin, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == data.username).first()
    if not user or not pwd_context.verify(data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้งานหรือรหัสผ่านไม่ถูกต้อง")
    
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="บัญชีนี้ไม่มีสิทธิ์เข้าถึงส่วนผู้ดูแลระบบ")

    if not user.is_active:
        raise HTTPException(status_code=403, detail="บัญชีนี้ถูกระงับการใช้งาน")
    
    return {
        "status": "success",
        "role": user.role,
        "username": user.username,
        "name": user.name
    }

# Add Admin
@router.post("/add")
def add_admin(data: AdminCreate, db: Session = Depends(get_db)):
    existing_user = db.query(models.User).filter(models.User.username == data.username).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="ชื่อผู้ใช้งานนี้มีอยู่ในระบบแล้ว"
        )
    full_name = f"{data.first_name} {data.last_name}".strip()
    
    new_admin = models.User(
        name=full_name,
        email=f"{data.username}@admin.coolkids.com",
        username=data.username,
        hashed_password=pwd_context.hash(data.password),
        role="admin",
        is_active=True
    )
    db.add(new_admin)
    db.commit()
    db.refresh(new_admin)
    return {"status": "success", "message": "เพิ่มผู้ดูแลระบบเรียบร้อยแล้ว"}

# ดึงรายชื่อผู้ดูแลระบบทั้งหมด (Admin List)
@router.get("/list")
def get_admin_list(db: Session = Depends(get_db), admin: models.User = Depends(get_current_admin)):
    admins = db.query(models.User).filter(models.User.role == "admin").all()
    return [{
        "id": adm.id,
        "name": adm.name,
        "username": adm.username,
        "is_active": adm.is_active,
        "created_at": adm.created_at.strftime("%Y-%m-%d %H:%M:%S") if adm.created_at else None
    } for adm in admins]

# เปลี่ยนสถานะ Active/Inactive ในระบบ Admin
@router.put("/users/{user_id}/status")
def update_user_status(
    user_id: int, 
    data: UserStatusUpdate, 
    db: Session = Depends(get_db),
    admin: models.User = Depends(get_current_admin)
):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="ไม่พบข้อมูลผู้ใช้งาน")
    
    user.is_active = data.is_active
    db.commit()
    return {"status": "success", "message": f"อัปเดตสถานะเรียบร้อยแล้ว"}

# การทำงานอุปกรณ์ (Hardware Logs)
@router.get("/hardware-logs")
def get_hardware_logs(
    db: Session = Depends(get_db),
    admin: models.User = Depends(get_current_admin)
):
    slots = db.query(models.Slot).all()
    logs = []
    for slot in slots:
        logs.append({
            "device": f"เซนเซอร์ช่องจอด {slot.name}",
            "status": "ว่าง" if slot.status == models.SlotStatus.available else "ไม่ว่าง",
            "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "detail": f"สถานะปัจจุบันของช่อง {slot.name}"
        })
    return logs