from database import engine, SessionLocal
from models import Base, Slot

def init():
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    if db.query(Slot).count() == 0:
        for name in ["N1", "N2", "N3"]:
            db.add(Slot(name=name))
        db.commit()
        print("Created slots: N1, N2, N3")
    else:
        print("Slots already exist")
    db.close()

if __name__ == "__main__":
    init()