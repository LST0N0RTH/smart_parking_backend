import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from jose import JWTError, jwt
from database import get_db
import models, schemas

router = APIRouter(prefix="/users", tags=["users"])

SECRET_KEY = os.getenv("SECRET_KEY", "your_fallback_secret_key")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.query(models.User).filter(models.User.email == email).first()
    if user is None:
        raise credentials_exception
    return user

@router.put("/me")
def update_profile(
    profile_data: schemas.UserUpdate,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if profile_data.name:
        current_user.name = profile_data.name
    if hasattr(profile_data, 'license_plates') and profile_data.license_plates:
        current_user.license_plate = ", ".join(profile_data.license_plates)
    elif hasattr(profile_data, 'license_plate') and profile_data.license_plate:
        current_user.license_plate = profile_data.license_plate

    db.add(current_user)
    db.commit() 
    db.refresh(current_user)
    
    return {"status": "success", "message": "Profile updated"}