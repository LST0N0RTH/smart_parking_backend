from sqlalchemy import text
from database import SessionLocal
from models import Slot

REQUIRED_SLOT_NAMES = ("M", "N", "O")

def init() -> None:
    from main import initialize_database

    initialize_database()
    db = SessionLocal()

    try:
        db.execute(text("SELECT pg_advisory_xact_lock(852004721)"))
        existing_names = {
            row[0]
            for row in (
                db.query(Slot.name)
                .filter(Slot.name.in_(REQUIRED_SLOT_NAMES))
                .all()
            )
        }
        missing_names = [
            name for name in REQUIRED_SLOT_NAMES if name not in existing_names
        ]
        for name in missing_names:
            db.add(Slot(name=name))
        db.commit()

        if missing_names:
            print(f"Created missing slots: {', '.join(missing_names)}")
        else:
            print("Database is ready; slots M, N, O already exist")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    init()