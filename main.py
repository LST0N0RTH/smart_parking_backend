from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import List
from jose import jwt, JWTError
from passlib.context import CryptContext

from contextlib import asynccontextmanager
import paho.mqtt.client as mqtt
import json, os, asyncio, logging, secrets
from pydantic import BaseModel
from database import get_db, engine, SessionLocal
from models import (
    Base, User, Vehicle, Slot, Booking, SlotStatus, 
    Payment, PaymentCard, BookingQrToken, HardwareCommand,
)
from schemas import (
    UserCreate, UserOut, Token, LoginForm,
    SlotOut, BookingCreate, BookingOut, UserUpdate,
    PaymentCreate, PaymentOut, VehicleInput, PaymentCardCreate,
    PaymentCardOut,
    BookingQrTokenOut,
)
from sqlalchemy import text

try:
    from models import DeviceStatus, HardwareLog
except ImportError:
    DeviceStatus = None
    HardwareLog = None

logger = logging.getLogger("smart_parking.auth")
USER_SCHEMA_MIGRATION_ID = "20260930_001_user_and_vehicle_schema"
BOOKING_VEHICLE_MIGRATION_ID = "20260930_002_booking_vehicle_reference"
LEGACY_VEHICLE_PROVINCE_MIGRATION_ID = ("20260930_003_normalize_legacy_vehicle_provinces")
EXPANDED_PROVINCE_ALIAS_MIGRATION_ID = ("20260930_004_expand_province_aliases")
PAYMENT_CARD_AND_QR_MIGRATION_ID = ("20261001_001_payment_cards_booking_code_and_qr_tokens")

CANONICAL_TABLE_NAMES = (
    ("users", "Users"),
    ("vehicles", "Vehicles"),
    ("slots", "Slots"),
    ("bookings", "Bookings"),
    ("payments", "Payments"),
    ("devices", "Devices"),
    ("hardwares", "Hardwares"),
    ("payment_cards", "PaymentCards"),
    ("booking_qr_tokens", "BookingQrTokens"),
    ("hardware_commands", "HardwareCommands"),
    ("schema_migrations", "SchemaMigrations"),
)

def migrate_table_names_to_pascal_case() -> None:
    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(852004721)")
        )

        for legacy_name, canonical_name in CANONICAL_TABLE_NAMES:
            legacy_table = connection.execute(
                text("SELECT to_regclass(:table_name)"),
                {"table_name": f"public.{legacy_name}"},
            ).scalar()

            canonical_table = connection.execute(
                text("SELECT to_regclass(:table_name)"),
                {"table_name": f'public."{canonical_name}"'},
            ).scalar()

            if legacy_table and canonical_table:
                raise RuntimeError(
                    f'พบทั้งตาราง "{legacy_name}" และ '
                    f'"{canonical_name}" พร้อมกัน '
                    "ระบบหยุดเพื่อป้องกันข้อมูลจริงแยกเป็นสองตาราง"
                )

            if legacy_table and not canonical_table:
                connection.execute(
                    text(
                        f'ALTER TABLE "{legacy_name}" '
                        f'RENAME TO "{canonical_name}"'
                    )
                )

def _province_lookup_key(value: str) -> str:
    return "".join(value.replace(".", "").split())

THAI_PROVINCES = (
    "กรุงเทพมหานคร",
    "กระบี่",
    "กาญจนบุรี",
    "กาฬสินธุ์",
    "กำแพงเพชร",
    "ขอนแก่น",
    "จันทบุรี",
    "ฉะเชิงเทรา",
    "ชลบุรี",
    "ชัยนาท",
    "ชัยภูมิ",
    "ชุมพร",
    "เชียงราย",
    "เชียงใหม่",
    "ตรัง",
    "ตราด",
    "ตาก",
    "นครนายก",
    "นครปฐม",
    "นครพนม",
    "นครราชสีมา",
    "นครศรีธรรมราช",
    "นครสวรรค์",
    "นนทบุรี",
    "นราธิวาส",
    "น่าน",
    "บึงกาฬ",
    "บุรีรัมย์",
    "ปทุมธานี",
    "ประจวบคีรีขันธ์",
    "ปราจีนบุรี",
    "ปัตตานี",
    "พระนครศรีอยุธยา",
    "พะเยา",
    "พังงา",
    "พัทลุง",
    "พิจิตร",
    "พิษณุโลก",
    "เพชรบุรี",
    "เพชรบูรณ์",
    "แพร่",
    "ภูเก็ต",
    "มหาสารคาม",
    "มุกดาหาร",
    "แม่ฮ่องสอน",
    "ยะลา",
    "ยโสธร",
    "ร้อยเอ็ด",
    "ระนอง",
    "ระยอง",
    "ราชบุรี",
    "ลพบุรี",
    "ลำปาง",
    "ลำพูน",
    "เลย",
    "ศรีสะเกษ",
    "สกลนคร",
    "สงขลา",
    "สตูล",
    "สมุทรปราการ",
    "สมุทรสงคราม",
    "สมุทรสาคร",
    "สระแก้ว",
    "สระบุรี",
    "สิงห์บุรี",
    "สุโขทัย",
    "สุพรรณบุรี",
    "สุราษฎร์ธานี",
    "สุรินทร์",
    "หนองคาย",
    "หนองบัวลำภู",
    "อ่างทอง",
    "อำนาจเจริญ",
    "อุดรธานี",
    "อุตรดิตถ์",
    "อุทัยธานี",
    "อุบลราชธานี",
)

PROVINCE_ALIASES = {
    **{
        province: province
        for province in THAI_PROVINCES
    },
    "กรุงเทพ": "กรุงเทพมหานคร",
    "กทม": "กรุงเทพมหานคร",
    "กาญ": "กาญจนบุรี",
    "กาฬ": "กาฬสินธุ์",
    "กำแพง": "กำแพงเพชร",
    "ขอน": "ขอนแก่น",
    "จันท์": "จันทบุรี",
    "ฉะเชิง": "ฉะเชิงเทรา",
    "ชล": "ชลบุรี",
    "นายก": "นครนายก",
    "ปฐม": "นครปฐม",
    "พนม": "นครพนม",
    "โคราช": "นครราชสีมา",
    "นครศรี": "นครศรีธรรมราช",
    "นครสวรรค์": "นครสวรรค์",
    "นนท์": "นนทบุรี",
    "นรา": "นราธิวาส",
    "บึง": "บึงกาฬ",
    "ปทุม": "ปทุมธานี",
    "ประจวบ": "ประจวบคีรีขันธ์",
    "ปราจีน": "ปราจีนบุรี",
    "อยุธยา": "พระนครศรีอยุธยา",
    "พิษณุ": "พิษณุโลก",
    "สารคาม": "มหาสารคาม",
    "มุกดา": "มุกดาหาร",
    "แม่ฮ่อง": "แม่ฮ่องสอน",
    "ยโส": "ยโสธร",
    "ราช": "ราชบุรี",
    "สกล": "สกลนคร",
    "สิงห์": "สิงห์บุรี",
    "สุพรรณ": "สุพรรณบุรี",
    "สุราษฎร์": "สุราษฎร์ธานี",
    "หนองคาย": "หนองคาย",
    "หนองบัว": "หนองบัวลำภู",
    "อำนาจ": "อำนาจเจริญ",
    "อุดร": "อุดรธานี",
    "อุทัย": "อุทัยธานี",
    "อุบล": "อุบลราชธานี",
}

THAI_PROVINCE_LOOKUP = {
    _province_lookup_key(province): province
    for province in THAI_PROVINCES
}

for province in THAI_PROVINCES:
    THAI_PROVINCE_LOOKUP[
        _province_lookup_key(f"{province}ฯ")
    ] = province

for alias, province in PROVINCE_ALIASES.items():
    for alias_variant in (
        alias,
        f"{alias}ฯ",
        f"{alias} ฯ",
    ):
        THAI_PROVINCE_LOOKUP[
            _province_lookup_key(alias_variant)
        ] = province

def _parse_legacy_vehicle_text(
    vehicle_text: str,
) -> tuple[str, str] | None:
    words = vehicle_text.strip().split()

    if not words:
        return None

    for province_start in range(1, len(words)):
        province_text = " ".join(words[province_start:])
        province = THAI_PROVINCE_LOOKUP.get(
            _province_lookup_key(province_text)
        )

        if province is not None:
            return (
                " ".join(words[:province_start]),
                province,
            )

    return (" ".join(words), "")

def _new_booking_code() -> str:
    return f"CKP-{secrets.token_hex(6).upper()}"

def migrate_payment_cards_and_booking_qr() -> None:
    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(852004721)")
        )

        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "SchemaMigrations" (
                    migration_id VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        migration_applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {"migration_id": PAYMENT_CARD_AND_QR_MIGRATION_ID},
        ).scalar()

        if migration_applied:
            return

        connection.execute(
            text(
                """
                ALTER TABLE "Bookings"
                ADD COLUMN IF NOT EXISTS booking_code VARCHAR
                """
            )
        )

        existing_codes = {
            row["booking_code"]
            for row in connection.execute(
                text(
                    """
                    SELECT booking_code
                    FROM "Bookings"
                    WHERE booking_code IS NOT NULL
                    """
                )
            ).mappings()
        }

        bookings_without_code = connection.execute(
            text(
                """
                SELECT id
                FROM "Bookings"
                WHERE booking_code IS NULL
                   OR BTRIM(booking_code) = ''
                """
            )
        ).mappings().all()

        for booking in bookings_without_code:
            booking_code = _new_booking_code()
            while booking_code in existing_codes:
                booking_code = _new_booking_code()
            existing_codes.add(booking_code)

            connection.execute(
                text(
                    """
                    UPDATE "Bookings"
                    SET booking_code = :booking_code
                    WHERE id = :booking_id
                    """
                ),
                {
                    "booking_id": booking["id"],
                    "booking_code": booking_code,
                },
            )

        connection.execute(
            text(
                """
                ALTER TABLE "Bookings"
                ALTER COLUMN booking_code SET NOT NULL
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS
                uq_bookings_booking_code
                ON "Bookings" (booking_code)
                """
            )
        )

        connection.execute(
            text(
                """
                INSERT INTO "SchemaMigrations" (migration_id)
                VALUES (:migration_id)
                """
            ),
            {"migration_id": PAYMENT_CARD_AND_QR_MIGRATION_ID},
        )

def migrate_existing_users_table() -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "SchemaMigrations" (
                    migration_id VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        migration_applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {"migration_id": USER_SCHEMA_MIGRATION_ID},
        ).scalar()

        if migration_applied:
            return

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ADD COLUMN IF NOT EXISTS username VARCHAR
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ADD COLUMN IF NOT EXISTS role VARCHAR
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ADD COLUMN IF NOT EXISTS is_active BOOLEAN
                """
            )
        )

        connection.execute(
            text(
                """
                UPDATE "Users"
                SET role = 'user'
                WHERE role IS NULL
                """
            )
        )

        connection.execute(
            text(
                """
                UPDATE "Users"
                SET is_active = TRUE
                WHERE is_active IS NULL
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ALTER COLUMN role SET DEFAULT 'user'
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ALTER COLUMN role SET NOT NULL
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ALTER COLUMN is_active SET DEFAULT TRUE
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE "Users"
                ALTER COLUMN is_active SET NOT NULL
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username
                ON "Users" (username)
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "Vehicles" (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES "Users"(id),
                    plate_number VARCHAR NOT NULL,
                    province VARCHAR NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_vehicles_user_plate_province
                        UNIQUE (user_id, plate_number, province)
                )
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_vehicles_user_id
                ON "Vehicles" (user_id)
                """
            )
        )

        legacy_users = connection.execute(
            text(
                """
                SELECT id, license_plate
                FROM "Users"
                WHERE license_plate IS NOT NULL
                  AND BTRIM(license_plate) <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM "Vehicles"
                      WHERE "Vehicles".user_id = "Users".id
                  )
                """
            )
        ).mappings().all()

        for legacy_user in legacy_users:
            legacy_plates = legacy_user["license_plate"].split(",")

            for legacy_plate in legacy_plates:
                vehicle_text = legacy_plate.strip()
                if not vehicle_text:
                    continue

                parsed_vehicle = _parse_legacy_vehicle_text(
                    vehicle_text
                )

                if parsed_vehicle is None:
                    continue

                plate_number, province = parsed_vehicle

                connection.execute(
                    text(
                        """
                        INSERT INTO "Vehicles" (
                            user_id,
                            plate_number,
                            province
                        )
                        VALUES (
                            :user_id,
                            :plate_number,
                            :province
                        )
                        ON CONFLICT (
                            user_id,
                            plate_number,
                            province
                        ) DO NOTHING
                        """
                    ),
                    {
                        "user_id": legacy_user["id"],
                        "plate_number": plate_number,
                        "province": province,
                    },
                )

        connection.execute(
            text(
                """
                INSERT INTO "SchemaMigrations" (migration_id)
                VALUES (:migration_id)
                """
            ),
            {"migration_id": USER_SCHEMA_MIGRATION_ID},
        )

def migrate_booking_vehicle_reference() -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))

        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "SchemaMigrations" (
                    migration_id VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        migration_applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {"migration_id": BOOKING_VEHICLE_MIGRATION_ID},
        ).scalar()

        if migration_applied:
            return

        connection.execute(
            text(
                """
                ALTER TABLE "Bookings"
                ADD COLUMN IF NOT EXISTS vehicle_id INTEGER
                """
            )
        )

        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'fk_bookings_vehicle_id'
                          AND conrelid = '"Bookings"'::regclass
                    ) THEN
                        ALTER TABLE "Bookings"
                        ADD CONSTRAINT fk_bookings_vehicle_id
                        FOREIGN KEY (vehicle_id)
                        REFERENCES "Vehicles"(id)
                        ON DELETE SET NULL;
                    END IF;
                END
                $$;
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_bookings_vehicle_id
                ON "Bookings" (vehicle_id)
                """
            )
        )

        connection.execute(
            text(
                """
                INSERT INTO "SchemaMigrations" (migration_id)
                VALUES (:migration_id)
                """
            ),
            {"migration_id": BOOKING_VEHICLE_MIGRATION_ID},
        )

def migrate_legacy_vehicle_provinces(migration_id: str,) -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))

        migration_applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {
                "migration_id": (migration_id)
            },
        ).scalar()

        if migration_applied:
            return

        legacy_users = connection.execute(
            text(
                """
                SELECT id, license_plate
                FROM "Users"
                WHERE license_plate IS NOT NULL
                  AND BTRIM(license_plate) <> ''
                """
            )
        ).mappings().all()

        for legacy_user in legacy_users:
            current_vehicles = connection.execute(
                text(
                    """
                    SELECT id, plate_number, province
                    FROM "Vehicles"
                    WHERE user_id = :user_id
                    """
                ),
                {"user_id": legacy_user["id"]},
            ).mappings().all()

            for raw_vehicle in legacy_user["license_plate"].split(","):
                vehicle_text = raw_vehicle.strip()
                parsed_vehicle = _parse_legacy_vehicle_text(
                    vehicle_text
                )

                if not vehicle_text or parsed_vehicle is None:
                    continue

                expected_plate, expected_province = parsed_vehicle

                old_parts = vehicle_text.rsplit(maxsplit=1)
                old_plate = old_parts[0].strip()
                old_province = (
                    old_parts[1].strip()
                    if len(old_parts) == 2
                    else ""
                )

                if (
                    old_plate == expected_plate
                    and old_province == expected_province
                ):
                    continue

                incorrect_vehicle = next(
                    (
                        vehicle
                        for vehicle in current_vehicles
                        if (
                            vehicle["plate_number"] == old_plate
                            and vehicle["province"] == old_province
                        )
                    ),
                    None,
                )

                corrected_vehicle_exists = any(
                    (
                        vehicle["plate_number"] == expected_plate
                        and vehicle["province"] == expected_province
                    )
                    for vehicle in current_vehicles
                )

                if (
                    incorrect_vehicle is None
                    or corrected_vehicle_exists
                ):
                    continue

                connection.execute(
                    text(
                        """
                        UPDATE "Vehicles"
                        SET plate_number = :plate_number,
                            province = :province
                        WHERE id = :vehicle_id
                        """
                    ),
                    {
                        "vehicle_id": incorrect_vehicle["id"],
                        "plate_number": expected_plate,
                        "province": expected_province,
                    },
                )

        connection.execute(
            text(
                """
                INSERT INTO "SchemaMigrations" (migration_id)
                VALUES (:migration_id)
                """
            ),
            {
                "migration_id": (migration_id)
            },
        )

migrate_table_names_to_pascal_case()
migrate_existing_users_table()
migrate_booking_vehicle_reference()
migrate_legacy_vehicle_provinces(LEGACY_VEHICLE_PROVINCE_MIGRATION_ID)
migrate_legacy_vehicle_provinces(EXPANDED_PROVINCE_ALIAS_MIGRATION_ID)
Base.metadata.create_all(bind=engine)
migrate_payment_cards_and_booking_qr()

# ==============================
# BACKGROUND TASKS & LIFESPAN
# ==============================
# 🌟 ระบบสแกนยกเลิกการจองอัตโนมัติ
async def auto_cancel_no_shows():
    while True:
        await asyncio.sleep(30)
        db = SessionLocal()
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=30) 
            expired_bookings = db.query(Booking).join(Slot).filter( 
                Booking.start_time <= cutoff_time,
                Slot.status == SlotStatus.reserved
            ).all()
            
            for booking in expired_bookings:
                slot = booking.slot
                slot.status = SlotStatus.available
                
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

SECRET_KEY = os.getenv("SECRET_KEY")

if not SECRET_KEY or len(SECRET_KEY) < 32:
    raise RuntimeError(
        "SECRET_KEY must be configured with at least 32 characters."
    )

ALGORITHM = os.getenv("ALGORITHM", "HS256")
TOKEN_EXP = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

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

def get_current_admin(current_user: User = Depends(get_current_user)):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="ไม่มีสิทธิ์เข้าถึงส่วนผู้ดูแลระบบ")
    return current_user

MAX_VEHICLES_PER_USER = 5
MAX_PAYMENT_CARDS_PER_USER = 3
QR_VALIDITY = timedelta(minutes=15)
QR_TYPES = {"entry", "exit"}

def _new_qr_token() -> str:
    return secrets.token_urlsafe(32)

def _generate_unique_booking_code(db: Session) -> str:
    booking_code = _new_booking_code()

    while db.query(Booking).filter(
        Booking.booking_code == booking_code
    ).first():
        booking_code = _new_booking_code()
    return booking_code

def _create_qr_token(
    db: Session,
    booking: Booking,
    qr_type: str,
) -> BookingQrToken:
    now = datetime.now(timezone.utc)

    qr_token = BookingQrToken(
        booking_id=booking.id,
        qr_type=qr_type,
        token=_new_qr_token(),
        issued_at=now,
        expires_at=now + QR_VALIDITY,
    )
    db.add(qr_token)
    return qr_token


def _active_qr_token(
    db: Session,
    booking_id: int,
    qr_type: str,
) -> BookingQrToken | None:
    now = datetime.now(timezone.utc)
    return (
        db.query(BookingQrToken)
        .filter(
            BookingQrToken.booking_id == booking_id,
            BookingQrToken.qr_type == qr_type,
            BookingQrToken.revoked_at.is_(None),
            BookingQrToken.used_at.is_(None),
            BookingQrToken.expires_at > now,
        )
        .order_by(BookingQrToken.issued_at.desc())
        .first()
    )

def _vehicle_display(plate_number: str, province: str) -> str:
    return f"{plate_number} {province}".strip()

def _validate_vehicle_specs(
    vehicle_specs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    if len(vehicle_specs) > MAX_VEHICLES_PER_USER:
        raise HTTPException(
            status_code=422,
            detail="A user may register up to 5 vehicles",
        )
    
    normalized_specs: list[tuple[str, str]] = []
    registered_vehicles: set[tuple[str, str]] = set()

    for plate_number, province in vehicle_specs:
        cleaned_plate_number = plate_number.strip()
        cleaned_province = province.strip()
        if not cleaned_plate_number:
            raise HTTPException(
                status_code=422,
                detail="Vehicle plate number is required",
            )
        vehicle_key = (
            cleaned_plate_number.casefold(),
            cleaned_province.casefold(),
        )
        if vehicle_key in registered_vehicles:
            raise HTTPException(
                status_code=422,
                detail="Duplicate vehicle information",
            )
        registered_vehicles.add(vehicle_key)
        normalized_specs.append(
            (cleaned_plate_number, cleaned_province)
        )
    return normalized_specs

def _vehicle_specs_from_payload(
    vehicles: list[VehicleInput],
) -> list[tuple[str, str]]:
    return _validate_vehicle_specs(
        [
            (vehicle.plate_number, vehicle.province)
            for vehicle in vehicles
        ]
    )

def _vehicle_specs_from_legacy(
    license_plate: str | None,
) -> list[tuple[str, str]]:
    if not license_plate or not license_plate.strip():
        return []
    vehicle_specs: list[tuple[str, str]] = []
    for raw_vehicle in license_plate.split(","):
        parsed_vehicle = _parse_legacy_vehicle_text(
            raw_vehicle
        )
        if parsed_vehicle is not None:
            vehicle_specs.append(parsed_vehicle)
    return _validate_vehicle_specs(vehicle_specs)

def _legacy_plate_value(
    vehicle_specs: list[tuple[str, str]],
) -> str | None:
    if not vehicle_specs:
        return None
    return ",".join(
        _vehicle_display(plate_number, province)
        for plate_number, province in vehicle_specs
    )

def _replace_user_vehicles(
    db: Session,
    user_id: int,
    vehicle_specs: list[tuple[str, str]],
) -> None:
    db.query(Vehicle).filter(Vehicle.user_id == user_id).delete(
        synchronize_session=False
    )
    db.add_all(
        [
            Vehicle(
                user_id=user_id,
                plate_number=plate_number,
                province=province,
            )
            for plate_number, province in vehicle_specs
        ]
    )

def _get_selected_vehicle(
    db: Session,
    user_id: int,
    vehicle_id: int | None,
    license_plate: str | None,
) -> Vehicle:
    if vehicle_id is not None:
        vehicle = (
            db.query(Vehicle)
            .filter(
                Vehicle.id == vehicle_id,
                Vehicle.user_id == user_id,
            )
            .first()
        )
    else:
        requested_plate = (license_plate or "").strip()
        vehicle = None

        if requested_plate:
            user_vehicles = (
                db.query(Vehicle)
                .filter(Vehicle.user_id == user_id)
                .all()
            )

            vehicle = next(
                (
                    item
                    for item in user_vehicles
                    if _vehicle_display(
                        item.plate_number,
                        item.province,
                    ) == requested_plate
                ),
                None,
            )

    if vehicle is None:
        raise HTTPException(
            status_code=422,
            detail="Selected vehicle was not found in this account",
        )

    return vehicle

# ==============================
# USERS ROUTES
# ==============================
@app.post("/auth/register", response_model=UserOut)
def register(body: UserCreate, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(400, "Email already registered")
    if body.vehicles is None:
        vehicle_specs = _vehicle_specs_from_legacy(body.license_plate)
    else:
        vehicle_specs = _vehicle_specs_from_payload(body.vehicles)
    user = User(
        name=body.name,
        email=body.email,
        hashed_password=hash_password(body.password),
        license_plate=_legacy_plate_value(vehicle_specs),
    )
    db.add(user)
    db.flush()
    _replace_user_vehicles(db, user.id, vehicle_specs)
    db.commit()
    db.refresh(user)
    return user

@app.post("/auth/login", response_model=Token)
def login(body: LoginForm, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.email == body.email).first()

        if not user or not verify_password(body.password, user.hashed_password):
            raise HTTPException(
                status_code=401,
                detail="Invalid email or password",
            )
        return {
            "access_token": create_token({"sub": str(user.id)}),
            "token_type": "bearer",
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error while processing a login request")
        raise HTTPException(
            status_code=500,
            detail="Unable to complete sign-in",
        )

@app.get("/users/me", response_model=UserOut)
def get_me(current_user: User = Depends(get_current_user)):
    return current_user

@app.put("/users/me", response_model=UserOut)
def update_profile(
    body: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if body.name is not None:
        current_user.name = body.name.strip()
    if body.vehicles is not None:
        vehicle_specs = _vehicle_specs_from_payload(body.vehicles)
        _replace_user_vehicles(db, current_user.id, vehicle_specs)
        current_user.license_plate = _legacy_plate_value(vehicle_specs)
    elif body.license_plate is not None:
        vehicle_specs = _vehicle_specs_from_legacy(body.license_plate)
        _replace_user_vehicles(db, current_user.id, vehicle_specs)
        current_user.license_plate = _legacy_plate_value(vehicle_specs)
    if body.password is not None:
        if len(body.password) < 8:
            raise HTTPException(400, "รหัสผ่านต้องมีอย่างน้อย 8 ตัว")
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
MIN_CHARGE    = 25  
DAILY_RATE    = 250 

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
    selected_vehicle = _get_selected_vehicle(
        db=db,
        user_id=current_user.id,
        vehicle_id=body.vehicle_id,
        license_plate=body.license_plate,
    )
    selected_license_plate = _vehicle_display(
        selected_vehicle.plate_number,
        selected_vehicle.province,
    )

    same_vehicle_booking = (
        db.query(Booking)
        .filter(
            Booking.user_id == current_user.id,
            Booking.status == "active",
            Booking.start_time < body.end_time,
            Booking.end_time > body.start_time,
            (
                (Booking.vehicle_id == selected_vehicle.id)
                | (
                    (Booking.vehicle_id.is_(None))
                    & (Booking.license_plate == selected_license_plate)
                )
            ),
        )
        .first()
    )

    if same_vehicle_booking:
        plate_str = selected_license_plate
        raise HTTPException(
            status_code=409,
            detail=f"รถทะเบียน {plate_str} มีการจองค้างอยู่ในระบบแล้ว!",
        )

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
        vehicle_id    = selected_vehicle.id,
        license_plate = selected_license_plate,
        start_time    = body.start_time,
        end_time      = body.end_time,
        total_amount  = total_amount,
        booking_code  = _generate_unique_booking_code(db),
    )
    db.add(booking)
    db.commit()
    db.refresh(booking)

    # สร้าง Payment รอชำระ
    payment = Payment(booking_id=booking.id, amount=total_amount)
    db.add(payment)

    _create_qr_token(
        db=db,
        booking=booking,
        qr_type="entry",
    )

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

# ==============================
# ADMIN ROUTES
# ==============================
class AdminLoginReq(BaseModel):
    username: str
    password: str

class AdminAddReq(BaseModel):
    first_name: str
    last_name: str
    username: str
    password: str

class ServoOverrideReq(BaseModel):
    action: str

@app.post("/admin/login")
def admin_login(body: AdminLoginReq, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == body.username, User.role == "admin").first()
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(401, "Invalid username or password")
    
    # อัปเดตสถานะ
    user.is_active = True
    db.commit()

    token = create_token({"sub": str(user.id)})
    return {"role": "admin", "access_token": token}

@app.get("/admin/list")
def get_admin_list(current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    admins = db.query(User).filter(User.role == "admin").all()
    return [
        {
            "is_active": admin.is_active,
            "name": admin.name,
            "username": admin.username,
            "created_at": admin.created_at.strftime("%Y-%m-%d %H:%M") if admin.created_at else "-"
        }
        for admin in admins
    ]

@app.post("/admin/add")
def add_admin(body: AdminAddReq, current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.username == body.username).first()
    if existing:
        raise HTTPException(400, "Username already exists")
    
    new_admin = User(
        name=f"{body.first_name} {body.last_name}",
        username=body.username,
        email=f"{body.username}@admin.local", 
        hashed_password=hash_password(body.password),
        role="admin",
        is_active=False
    )
    db.add(new_admin)
    db.commit()
    return {"message": "Admin added successfully"}

@app.get("/admin/analytics")
def get_analytics(current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    today = datetime.utcnow().date()
    
    # คำนวณรายได้รายวัน
    payments = db.query(Payment).filter(Payment.status == "paid").all()
    daily_income = sum(p.amount for p in payments if p.paid_at and p.paid_at.date() == today)

    # คำนวณ % การเข้าใช้งานพื้นที่
    total_slots = db.query(Slot).count()
    occupied_slots = db.query(Slot).filter(Slot.status != SlotStatus.available).count()
    usage_percent = round((occupied_slots / total_slots) * 100, 1) if total_slots > 0 else 0.0

    # นับจำนวนรถเข้า-ออก
    all_bookings = db.query(Booking).all()
    cars_in = sum(1 for b in all_bookings if b.start_time and b.start_time.date() == today)
    cars_out = sum(1 for b in all_bookings if b.status == "completed" and b.end_time.date() == today)
    in_out_count = f"{cars_in} / {cars_out} คัน"

    # สร้างกราฟสถิติ 7 วันย้อนหลัง
    weekly_chart = []
    for i in range(6, -1, -1):
        target_date = today - timedelta(days=i)
        day_income = sum(p.amount for p in payments if p.paid_at and p.paid_at.date() == target_date)
        weekly_chart.append({"label": target_date.strftime("%a"), "value": float(day_income)})

    return {
        "daily_income": daily_income,
        "usage_percent": usage_percent,
        "in_out_count": in_out_count,
        "weekly_chart": weekly_chart
    }

@app.get("/admin/bookings")
def get_all_bookings_admin(current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    # ดึงประวัติจอดรถทั้งหมด (Admin Only)
    return db.query(Booking).order_by(Booking.created_at.desc()).all()

@app.get("/admin/hardware-logs")
def get_hardware_logs(current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    if HardwareLog is None:
        return []
    logs = db.query(HardwareLog).order_by(HardwareLog.created_at.desc()).limit(50).all()
    return [
        {
            "device": log.device_name,
            "status": log.status,
            "time": log.created_at.strftime("%H:%M:%S") if log.created_at else "-",
            "detail": log.detail
        } for log in logs
    ]

@app.post("/admin/override/servo")
def manual_override_servo(body: ServoOverrideReq, current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    if body.action not in ["open", "close"]:
        raise HTTPException(400, "Invalid action")
    mqtt_client.publish("parking/servo/command", json.dumps({"device": "servo_motor", "action": body.action}))

    if HardwareLog and DeviceStatus:
        servo_status = db.query(DeviceStatus).filter(DeviceStatus.device_name == "servo_motor").first()
        if not servo_status:
            servo_status = DeviceStatus(device_name="servo_motor", status=body.action)
            db.add(servo_status)
        else:
            servo_status.status = body.action
        
        log = HardwareLog(device_name="servo_motor", status=body.action, detail=f"Manual override by {current_admin.username}")
        db.add(log)
        db.commit()

    return {"message": f"Servo motor set to {body.action}"}