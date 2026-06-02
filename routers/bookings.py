@router.post("/", response_model=schemas.BookingOut)
def create_booking(booking: schemas.BookingCreate, db: Session = Depends(get_db)):
    
    new_booking = models.Booking(
        slot_id=booking.slot_id,
        start_time=booking.start_time,
        end_time=booking.end_time,

        license_plate=booking.license_plate 
    )
    
    db.add(new_booking)
    db.commit()
    db.refresh(new_booking)
    return new_booking