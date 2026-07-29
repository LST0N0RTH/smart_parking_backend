from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import models, schemas
from database import get_db
from routers.auth import get_current_user

router = APIRouter(prefix="/bookings", tags=["bookings"])

@router.post("/", response_model=schemas.BookingOut)
def create_booking(
    booking: schemas.BookingCreate, 
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # ตรวจสอบความถูกต้องของเวลา
    if booking.end_time <= booking.start_time:
        raise HTTPException(status_code=400, detail="เวลาออกต้องอยู่หลังเวลาเข้า")

    # 2. ตรวจสอบการจองซ้อนทับ
    overlapping = db.query(models.Booking).filter(
        models.Booking.slot_id == booking.slot_id,
        models.Booking.status == "active", 
        models.Booking.start_time < booking.end_time,
        models.Booking.end_time > booking.start_time
    ).first()

    if overlapping:
        raise HTTPException(status_code=409, detail="ช่องจอดรถนี้ถูกจองแล้ว")
    
    # 3. สร้างรายการจองและผูกกับ User id 
    new_booking = models.Booking(
        user_id=current_user.id, 
        slot_id=booking.slot_id,
        start_time=booking.start_time,
        end_time=booking.end_time,
        license_plate=booking.license_plate 
    )
    
    db.add(new_booking)
    db.commit()
    db.refresh(new_booking)
    
    return new_booking