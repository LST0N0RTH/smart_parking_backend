from database import engine, SessionLocal
from models import Base, Slot

def init():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    
    try:
        if db.query(Slot).first() is None:
            for name in ["M", "N", "O"]:
                db.add(Slot(name=name))
            db.commit()
            print("Created slots: M, N, O")
        else:
            print("Slots already exist")
            
    except Exception as e:
        db.rollback()
        print(f"Error initializing database: {e}")
        
    finally:
        db.close()

if __name__ == "__main__":
    init()