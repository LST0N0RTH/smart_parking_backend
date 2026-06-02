from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
import models, schemas

router = APIRouter(prefix="/users", tags=["users"])

@router.put("/me")
def update_profile(
    profile_data: schemas.UserUpdate,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    user = db.query(models.User).filter(models.User.id == current_user.id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.name = profile_data.name
    if profile_data.license_plates:
        user.license_plate = profile_data.license_plates[0]

    db.add(user)
    db.commit() 
    db.refresh(user)
    return {"status": "success", "message": "Profile updated"}