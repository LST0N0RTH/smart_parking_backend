from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from jose import jwt, JWTError
from passlib.context import CryptContext
from contextlib import asynccontextmanager
import asyncio
import json
import logging
import os
import secrets
import socket
import ssl
from pathlib import Path
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from html import escape
from database import get_db, engine, SessionLocal
from models import (
    Base, User, Vehicle, Slot, Booking, SlotStatus,
    Payment, PaymentCard, BookingQrToken, HardwareCommand, GateState,
    DeviceStatus, HardwareLog, AuthSession, GateEvent, SlotStatusLog,
    SupportMessage,
)
from schemas import (
    UserCreate, UserOut, Token, LoginForm, MessageOut,
    SlotOut, BookingCreate, BookingOut, UserUpdate,
    PaymentCreate, PaymentOut, VehicleInput, PaymentCardCreate,
    PaymentCardOut, BookingQrTokenOut, GateStateOut, HardwareCommandOut,
    DeviceStatusOut, AdminLoginReq, AdminAddReq, AdminListOut,
    AdminDeactivateOut, AdminBookingHistoryOut, AnalyticsOut,
    AnalyticsPeriod, ChartData, HardwareErrorAnalyticsOut,
    DeviceErrorCount, HardwareLogOut, RgbOverrideReq, ServoOverrideReq,
    SupportMessageCreate, SupportMessageOut, SupportConversationOut,
    AdminActivityOut, GateEventOut, SlotStatusLogOut,
)
from sqlalchemy import text, or_, and_, func
from sqlalchemy.exc import IntegrityError
load_dotenv(Path(__file__).with_name(".env"))
logger = logging.getLogger("smart_parking.auth")

USER_SCHEMA_MIGRATION_ID = "20260930_001_user_and_vehicle_schema"
BOOKING_VEHICLE_MIGRATION_ID = "20260930_002_booking_vehicle_reference"
LEGACY_VEHICLE_PROVINCE_MIGRATION_ID = ("20260930_003_normalize_legacy_vehicle_provinces")
EXPANDED_PROVINCE_ALIAS_MIGRATION_ID = ("20260930_004_expand_province_aliases")
PAYMENT_CARD_AND_QR_MIGRATION_ID = ("20261001_001_payment_cards_booking_code_and_qr_tokens")
QR_TOKEN_LIFECYCLE_MIGRATION_ID = "20261004_002_qr_token_lifecycle"
HARDWARE_COMMAND_PUBLISH_TIME_MIGRATION_ID = ("20261009_001_hardware_command_published_at")
GATE_STATE_MIGRATION_ID = "20261009_002_gate_states"
RUNTIME_SCHEMA_MIGRATION_ID = "20261013_001_runtime_schema_fields"
ENTRY_TIME_AND_PAYMENT_METHOD_MIGRATION_ID = "20261013_002_entry_time_payment_method"
PAYMENT_BOOKING_UNIQUE_MIGRATION_ID = "20261014_001_payment_booking_unique"

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
    ("auth_sessions", "AuthSessions"),
    ("gate_events", "GateEvents"),
    ("slot_status_logs", "SlotStatusLogs"),
    ("support_messages", "SupportMessages"),
    ("gate_states", "GateStates"),
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
                    f'ตรวจพบทั้งตาราง "{legacy_name}" และ '
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

def migrate_qr_token_lifecycle() -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))

        applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {"migration_id": QR_TOKEN_LIFECYCLE_MIGRATION_ID},
        ).scalar()

        if applied:
            return

        connection.execute(
            text(
                """
                ALTER TABLE "BookingQrTokens"
                ALTER COLUMN expires_at DROP NOT NULL
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
            {"migration_id": QR_TOKEN_LIFECYCLE_MIGRATION_ID},
        )

def migrate_hardware_command_publish_time() -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))

        applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {
                "migration_id": (
                    HARDWARE_COMMAND_PUBLISH_TIME_MIGRATION_ID
                )
            },
        ).scalar()

        if applied:
            return

        connection.execute(
            text(
                """
                ALTER TABLE "HardwareCommands"
                ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ
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
            {
                "migration_id": (
                    HARDWARE_COMMAND_PUBLISH_TIME_MIGRATION_ID
                )
            },
        )

def migrate_gate_states() -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))

        applied = connection.execute(
            text(
                """
                SELECT 1
                FROM "SchemaMigrations"
                WHERE migration_id = :migration_id
                """
            ),
            {"migration_id": GATE_STATE_MIGRATION_ID},
        ).scalar()

        if applied:
            return

        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "GateStates" (
                    id SERIAL PRIMARY KEY,
                    gate VARCHAR NOT NULL UNIQUE,
                    last_action VARCHAR,
                    source VARCHAR,
                    command_id VARCHAR,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
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
            {"migration_id": GATE_STATE_MIGRATION_ID},
        )

def migrate_runtime_schema_fields() -> None:
    """Additive columns used by the current API without replacing data."""
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
        if connection.execute(
            text(
                'SELECT 1 FROM "SchemaMigrations" '
                'WHERE migration_id = :migration_id'
            ),
            {"migration_id": RUNTIME_SCHEMA_MIGRATION_ID},
        ).scalar():
            return

        additions = (
            ('"Users"', "username_changed_at TIMESTAMPTZ"),
            ('"Users"', "last_login_at TIMESTAMPTZ"),
            ('"Users"', "last_logout_at TIMESTAMPTZ"),
            ('"Users"', "deleted_at TIMESTAMPTZ"),
            ('"Bookings"', "vehicle_plate_number VARCHAR"),
            ('"Bookings"', "vehicle_province VARCHAR"),
            ('"Bookings"', "exited_at TIMESTAMPTZ"),
            ('"Payments"', "base_amount INTEGER"),
            ('"Payments"', "overstay_fee INTEGER"),
            (
                '"Payments"',
                'payment_card_id INTEGER REFERENCES "PaymentCards"(id) '
                "ON DELETE SET NULL",
            ),
            ('"Payments"', "card_brand VARCHAR"),
            ('"Payments"', "card_last4 VARCHAR"),
            ('"Devices"', "last_seen_at TIMESTAMPTZ"),
            ('"Devices"', "last_processed_at TIMESTAMPTZ"),
            ('"Hardwares"', "command_id VARCHAR"),
            ('"Hardwares"', "source VARCHAR"),
            ('"HardwareCommands"', "target_device VARCHAR"),
            ('"HardwareCommands"', "payload JSON"),
            ('"HardwareCommands"', "result JSON"),
            ('"HardwareCommands"', "result_received_at TIMESTAMPTZ"),
        )
        for table_name, column_definition in additions:
            connection.execute(
                text(
                    f"ALTER TABLE {table_name} "
                    f"ADD COLUMN IF NOT EXISTS {column_definition}"
                )
            )
        connection.execute(
            text(
                'CREATE INDEX IF NOT EXISTS ix_hardwares_command_id '
                'ON "Hardwares" (command_id)'
            )
        )
        connection.execute(
            text(
                'CREATE INDEX IF NOT EXISTS ix_hardwares_source '
                'ON "Hardwares" (source)'
            )
        )
        connection.execute(
            text(
                'CREATE INDEX IF NOT EXISTS ix_hardware_commands_target_device '
                'ON "HardwareCommands" (target_device)'
            )
        )
        connection.execute(
            text(
                'CREATE INDEX IF NOT EXISTS ix_devices_last_seen_at '
                'ON "Devices" (last_seen_at)'
            )
        )
        connection.execute(
            text(
                'CREATE INDEX IF NOT EXISTS ix_users_deleted_at '
                'ON "Users" (deleted_at)'
            )
        )
        connection.execute(
            text(
                'INSERT INTO "SchemaMigrations" (migration_id) '
                'VALUES (:migration_id)'
            ),
            {"migration_id": RUNTIME_SCHEMA_MIGRATION_ID},
        )

def initialize_database() -> None:
    migrate_table_names_to_pascal_case()
    Base.metadata.create_all(bind=engine)
    migrate_existing_users_table()
    migrate_booking_vehicle_reference()
    migrate_legacy_vehicle_provinces(LEGACY_VEHICLE_PROVINCE_MIGRATION_ID)
    migrate_legacy_vehicle_provinces(EXPANDED_PROVINCE_ALIAS_MIGRATION_ID)
    migrate_payment_cards_and_booking_qr()
    migrate_qr_token_lifecycle()
    migrate_hardware_command_publish_time()
    migrate_gate_states()
    migrate_runtime_schema_fields()
    migrate_entry_time_and_payment_method()
    migrate_payment_booking_unique()

def migrate_payment_booking_unique() -> None:
    """Reject ambiguous old data before enforcing one payment per booking."""
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))
        applied = connection.execute(
            text('SELECT 1 FROM "SchemaMigrations" WHERE migration_id = :id'),
            {"id": PAYMENT_BOOKING_UNIQUE_MIGRATION_ID},
        ).scalar()
        if applied:
            return
        duplicate = connection.execute(
            text(
                'SELECT booking_id, COUNT(*) AS total FROM "Payments" '
                'GROUP BY booking_id HAVING COUNT(*) > 1 LIMIT 1'
            )
        ).mappings().first()
        if duplicate:
            raise RuntimeError(
                f'Payments has {duplicate["total"]} rows for booking '
                f'{duplicate["booking_id"]}; resolve the old data before startup'
            )
        connection.execute(
            text(
                'CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_booking_id '
                'ON "Payments" (booking_id)'
            )
        )
        connection.execute(
            text('INSERT INTO "SchemaMigrations" (migration_id) VALUES (:id)'),
            {"id": PAYMENT_BOOKING_UNIQUE_MIGRATION_ID},
        )

def migrate_entry_time_and_payment_method() -> None:
    """Keep existing bookings and payment history while updating their schema."""
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(852004721)"))
        if connection.execute(
            text(
                'SELECT 1 FROM "SchemaMigrations" '
                'WHERE migration_id = :migration_id'
            ),
            {"migration_id": ENTRY_TIME_AND_PAYMENT_METHOD_MIGRATION_ID},
        ).scalar():
            return
        connection.execute(
            text('ALTER TABLE "Bookings" ADD COLUMN IF NOT EXISTS entered_at TIMESTAMPTZ')
        )
        connection.execute(
            text('ALTER TABLE "Payments" ALTER COLUMN method DROP DEFAULT')
        )
        connection.execute(
            text('ALTER TABLE "Payments" ALTER COLUMN method DROP NOT NULL')
        )
        connection.execute(
            text('UPDATE "Payments" SET method = NULL WHERE status <> \'paid\'')
        )
        connection.execute(
            text(
                'INSERT INTO "SchemaMigrations" (migration_id) '
                'VALUES (:migration_id)'
            ),
            {"migration_id": ENTRY_TIME_AND_PAYMENT_METHOD_MIGRATION_ID},
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

# ==============================
# BACKGROUND TASKS & LIFESPAN
# ==============================
# 🌟 ระบบสแกนยกเลิกการจองอัตโนมัติ
async def auto_cancel_no_shows():
    while True:
        await asyncio.sleep(1)
        db = SessionLocal()

        try:
            now = datetime.now(timezone.utc)
            cutoff_time = now - timedelta(minutes=30)

            expired_bookings = (
                db.query(Booking)
                .filter(
                    Booking.status == "active",
                    Booking.entered_at.is_(None),
                    Booking.start_time <= cutoff_time,
                )
                .with_for_update()
                .all()
            )

            for booking in expired_bookings:
                slot_name = _mark_booking_time_out(
                    db=db,
                    booking=booking,
                    now=now,
                )
                db.commit()

                await _publish_slot_state(
                    slot_name=slot_name,
                    status="available",
                    source="booking_time_out",
                )
                await broadcast_booking_update(
                    user_id=booking.user_id,
                    booking_id=booking.id,
                    event="booking_time_out",
                )
                logger.info(
                    "Booking %s changed to time_out after the 30-minute "
                    "entry window",
                    booking.id,
                )

        except Exception as error:
            db.rollback()
            print(f"Background task error: {error}")
        finally:
            db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global slot_broadcast_loop

    await asyncio.to_thread(initialize_database)
    slot_broadcast_loop = asyncio.get_running_loop()

    try:
        mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
        mqtt_client.connect_async(
            MQTT_BROKER,
            MQTT_PORT,
            keepalive=30,
        )
        mqtt_client.loop_start()
    except Exception:
        logger.exception("Unable to start MQTT connection; reconnect will be retried")

    tasks = [
        asyncio.create_task(auto_cancel_no_shows()),
        asyncio.create_task(monitor_hardware_command_timeouts()),
        asyncio.create_task(retry_unpublished_hardware_commands()),
        asyncio.create_task(monitor_device_liveness()),
        asyncio.create_task(expire_admin_rgb_overrides()),
    ]

    try:
        yield
    finally:
        for task in tasks:
            task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        try:
            mqtt_client.disconnect()
            mqtt_client.loop_stop()
        except Exception:
            logger.exception("Unable to stop MQTT client cleanly")

        slot_broadcast_loop = None

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
slot_clients_lock = asyncio.Lock()
slot_broadcast_loop: asyncio.AbstractEventLoop | None = None
booking_clients: dict[int, list[WebSocket]] = {}
booking_clients_lock = asyncio.Lock()

async def broadcast_slot_update(message: dict) -> None:
    async with slot_clients_lock:
        for websocket in connected_clients.copy():
            try:
                await websocket.send_json(message)
            except Exception:
                if websocket in connected_clients:
                    connected_clients.remove(websocket)

async def broadcast_booking_update(
    user_id: int,
    booking_id: int,
    event: str,
) -> None:
    message = {
        "type": "booking_updated",
        "event": event,
        "booking_id": booking_id,
    }

    async with booking_clients_lock:
        clients = booking_clients.get(user_id, [])

        for websocket in clients.copy():
            try:
                await websocket.send_json(message)
            except Exception:
                if websocket in clients:
                    clients.remove(websocket)

        if not clients:
            booking_clients.pop(user_id, None)

def schedule_booking_broadcast(
    user_id: int,
    booking_id: int,
    event: str,
) -> None:
    loop = slot_broadcast_loop

    if loop is None or loop.is_closed():
        logger.warning(
            "Booking changed but no WebSocket event loop is available: "
            "booking_id=%s event=%s",
            booking_id,
            event,
        )
        return

    try:
        future = asyncio.run_coroutine_threadsafe(
            broadcast_booking_update(
                user_id=user_id,
                booking_id=booking_id,
                event=event,
            ),
            loop,
        )
    except RuntimeError:
        logger.exception(
            "Unable to schedule booking WebSocket update: "
            "booking_id=%s event=%s",
            booking_id,
            event,
        )
        return

    def log_broadcast_error(result) -> None:
        try:
            result.result()
        except Exception:
            logger.exception(
                "Unable to broadcast booking WebSocket update: "
                "booking_id=%s event=%s",
                booking_id,
                event,
            )

    future.add_done_callback(log_broadcast_error)

def schedule_slot_broadcast(
    slot_name: str,
    status: str,
) -> None:
    db = SessionLocal()
    try:
        slot = db.query(Slot).filter(Slot.name == slot_name).first()
        if slot is not None:
            status = slot.status.value
    finally:
        db.close()
    loop = slot_broadcast_loop

    if loop is None or loop.is_closed():
        logger.warning(
            "Slot update was committed but no WebSocket event loop is available: "
            "slot=%s status=%s",
            slot_name,
            status,
        )
        return

    try:
        future = asyncio.run_coroutine_threadsafe(
            broadcast_slot_update(
                {
                    "slot": slot_name,
                    "status": status,
                }
            ),
            loop,
        )
    except RuntimeError:
        logger.exception(
            "Unable to schedule WebSocket update for slot %s",
            slot_name,
        )
        return

    def log_broadcast_error(result) -> None:
        try:
            result.result()
        except Exception:
            logger.exception(
                "Unable to broadcast WebSocket update for slot %s",
                slot_name,
            )
    future.add_done_callback(log_broadcast_error)

GATE_EVENT_TOPIC = "parking/gate/event"
HARDWARE_ACK_TOPIC = "parking/hardware/ack"
HARDWARE_HEARTBEAT_TOPIC = "parking/hardware/heartbeat"
SLOT_VISION_TOPIC = "parking/slot/vision"
GATE_DISPLAY_TOPIC = "parking/gate/display"

MQTT_INBOUND_TOPICS = (
    GATE_EVENT_TOPIC,
    HARDWARE_ACK_TOPIC,
    HARDWARE_HEARTBEAT_TOPIC,
    SLOT_VISION_TOPIC,
)

HARDWARE_ACK_TIMEOUT_SECONDS = 5
HARDWARE_ACK_POLL_SECONDS = 1
DEVICE_OFFLINE_SECONDS = 30

KNOWN_HARDWARE_DEVICES = {
    "esp32_cam",
    "esp32_cam_exit",
    "esp32_wroom",
    "slot_webcam",
    "ir1",
    "ir2",
    "ir3",
    "ir4",
    "ir5",
    "ir6",
    "servo",
    "oled",
    "rgb_m",
    "rgb_n",
    "rgb_o",
}

REPORTED_DEVICE_STATUSES = {
    "online",
    "degraded",
    "failed",
    "offline",
}

# ==============================
# MQTT SETUP
# ==============================
def _mqtt_env_flag(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)

    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()

    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be true or false")

MQTT_BROKER = os.getenv("MQTT_BROKER", "").strip()
if not MQTT_BROKER:
    raise RuntimeError("MQTT_BROKER must be configured for Render WSS")
try:
    MQTT_PORT = int(os.getenv("MQTT_PORT", "443"))
except ValueError as error:
    raise RuntimeError("MQTT_PORT must be a number") from error
if not 1 <= MQTT_PORT <= 65535:
    raise RuntimeError("MQTT_PORT must be between 1 and 65535")

MQTT_TRANSPORT = os.getenv(
    "MQTT_TRANSPORT",
    "websockets",
).strip().lower()

MQTT_TLS = _mqtt_env_flag("MQTT_TLS", default=True)

MQTT_USERNAME = os.getenv(
    "MQTT_USERNAME",
    "",
).strip()
MQTT_PASSWORD = os.getenv(
    "MQTT_PASSWORD",
    "",
)
MQTT_WEBSOCKET_PATH = os.getenv(
    "MQTT_WEBSOCKET_PATH",
    "/mqtt",
).strip() or "/mqtt"
MQTT_CLIENT_ID = os.getenv(
    "MQTT_CLIENT_ID",
    "",
).strip() or (
    f"coolkids-backend-{socket.gethostname()}-{os.getpid()}"
)
if MQTT_TRANSPORT != "websockets":
    raise RuntimeError("Coolkids Smart Parking requires MQTT_TRANSPORT=websockets")
if not MQTT_TLS:
    raise RuntimeError("Coolkids Smart Parking requires MQTT_TLS=true")
if bool(MQTT_USERNAME) != bool(MQTT_PASSWORD):
    raise RuntimeError(
        "MQTT_USERNAME and MQTT_PASSWORD must be configured together"
    )
if not MQTT_WEBSOCKET_PATH.startswith("/"):
    raise RuntimeError(
        "MQTT_WEBSOCKET_PATH must start with /"
    )

def _subscribe_to_mqtt_topics(
    client: mqtt.Client,
) -> None:
    for topic in MQTT_INBOUND_TOPICS:
        result, _ = client.subscribe(
            topic,
            qos=1,
        )

        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "Unable to subscribe to MQTT topic %s: result=%s",
                topic,
                result,
            )

def on_mqtt_connect(
    client: mqtt.Client,
    userdata: object,
    connect_flags: mqtt.ConnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: mqtt.Properties | None,
) -> None:
    del userdata, connect_flags, properties

    if reason_code.is_failure:
        logger.error(
            "MQTT connection failed: result=%s",
            reason_code,
        )
        return

    _subscribe_to_mqtt_topics(client)
    logger.info(
        "MQTT connected and subscribed to inbound topics"
    )
    _publish_gate_display_state()

def on_mqtt_disconnect(
    client: mqtt.Client,
    userdata: object,
    disconnect_flags: mqtt.DisconnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: mqtt.Properties | None,
) -> None:
    del client, userdata, disconnect_flags, properties

    logger.warning(
        "MQTT disconnected: %s",
        reason_code,
    )

def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning(
            "Ignoring invalid MQTT JSON payload from topic %s",
            msg.topic,
        )
        return
    try:
        if msg.topic == HARDWARE_ACK_TOPIC:
            _handle_hardware_ack(payload)
            return

        if msg.topic == HARDWARE_HEARTBEAT_TOPIC:
            _handle_hardware_heartbeat(payload)
            return

        if msg.topic == SLOT_VISION_TOPIC:
            _handle_slot_vision_update(payload)
            return

        if msg.topic == GATE_EVENT_TOPIC:
            _handle_gate_event(payload)
            return

        logger.warning(
            "Ignoring unsupported MQTT topic: %s",
            msg.topic,
        )
    except Exception:
        logger.exception("MQTT message processing error")

def _build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        clean_session=True,
        protocol=mqtt.MQTTv311,
        transport=MQTT_TRANSPORT,
        reconnect_on_failure=True,
    )
    if MQTT_USERNAME:
        client.username_pw_set(
            MQTT_USERNAME,
            MQTT_PASSWORD,
        )
    if MQTT_TRANSPORT == "websockets":
        client.ws_set_options(
            path=MQTT_WEBSOCKET_PATH,
        )
    if MQTT_TLS:
        client.tls_set_context(
            ssl.create_default_context(),
        )
    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_message = on_mqtt_message
    return client

mqtt_client = _build_mqtt_client()

# ==============================
# HELPERS & AUTHENTICATION
# ==============================
def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

def create_token(data: dict, db: Session | None = None) -> str:
    payload = data.copy()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=TOKEN_EXP)
    token_id = secrets.token_urlsafe(32)
    payload["jti"] = token_id
    payload["exp"] = expires_at
    if db is not None:
        user_id = payload.get("sub")
        if user_id is None:
            raise ValueError("A session token requires a user id")
        db.add(
            AuthSession(
                token_id=token_id,
                user_id=int(user_id),
                created_at=now,
                expires_at=expires_at,
            )
        )
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

bearer = HTTPBearer()

def get_current_user(credentials=Depends(bearer), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
        token_id = payload.get("jti")
    except (JWTError, TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid token")

    if not isinstance(token_id, str) or not token_id:
        raise HTTPException(status_code=401, detail="Session is not active")

    now = datetime.now(timezone.utc)
    user = (
        db.query(User)
        .filter(
            User.id == user_id,
            User.is_active.is_(True),
            User.deleted_at.is_(None),
        )
        .first()
    )
    session = (
        db.query(AuthSession)
        .filter(
            AuthSession.token_id == token_id,
            AuthSession.user_id == user_id,
            AuthSession.revoked_at.is_(None),
            AuthSession.logged_out_at.is_(None),
            AuthSession.expires_at > now,
        )
        .first()
    )
    if not user or not session:
        raise HTTPException(status_code=401, detail="Session is not active")
    return user

def get_current_admin(current_user: User = Depends(get_current_user)):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="ไม่มีสิทธิ์เข้าถึงส่วนผู้ดูแลระบบ")
    return current_user

MAX_VEHICLES_PER_USER = 5
MAX_PAYMENT_CARDS_PER_USER = 3
MAX_BOOKING_ADVANCE_DAYS = 30
ENTRY_QR_EARLY_ACCESS = timedelta(minutes=10)
ENTRY_QR_VALIDITY = timedelta(minutes=30)
GATE_PAIR_ACK_VALIDITY = timedelta(minutes=3)
PAYMENT_QR_VALIDITY = timedelta(minutes=15)
QR_TYPES = {"entry", "payment", "exit"}
PUBLIC_API_BASE_URL = os.getenv(
    "PUBLIC_API_BASE_URL",
    "https://coolkids-backend.onrender.com",
).rstrip("/")
SUPPORT_CONTACT_URL = os.getenv("SUPPORT_CONTACT_URL", "").strip()

def _support_contact_target() -> str:
    if not SUPPORT_CONTACT_URL:
        raise RuntimeError(
            "SUPPORT_CONTACT_URL must be configured with the public Web app URL"
        )
    parsed_url = urlsplit(SUPPORT_CONTACT_URL)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise RuntimeError(
            "SUPPORT_CONTACT_URL must be an absolute http(s) URL"
        )
    query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))
    query["openSupport"] = "true"

    return urlunsplit(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path or "/",
            urlencode(query),
            parsed_url.fragment,
        )
    )

SUPPORT_CONTACT_TARGET = _support_contact_target()
def _new_qr_token() -> str:
    return secrets.token_urlsafe(32)

def _generate_unique_booking_code(db: Session) -> str:
    booking_code = _new_booking_code()

    while db.query(Booking).filter(
        Booking.booking_code == booking_code
    ).first():
        booking_code = _new_booking_code()
    return booking_code

def _qr_expiry_for(
    booking: Booking,
    qr_type: str,
    now: datetime,
) -> datetime | None:
    if qr_type == "entry":
        return booking.start_time + ENTRY_QR_VALIDITY
    if qr_type == "payment":
        return now + PAYMENT_QR_VALIDITY
    if qr_type == "exit":
        return None
    raise ValueError(f"Unsupported QR type: {qr_type}")

def _create_qr_token(
    db: Session,
    booking: Booking,
    qr_type: str,
) -> BookingQrToken:
    if qr_type not in QR_TYPES:
        raise ValueError(f"Unsupported QR type: {qr_type}")

    now = datetime.now(timezone.utc)
    qr_token = BookingQrToken(
        booking_id=booking.id,
        qr_type=qr_type,
        token=_new_qr_token(),
        issued_at=now,
        expires_at=_qr_expiry_for(booking, qr_type, now),
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
            or_(
                BookingQrToken.expires_at.is_(None),
                BookingQrToken.expires_at > now,
            ),
        )
        .order_by(BookingQrToken.issued_at.desc())
        .first()
    )

def _qr_payload(
    booking: Booking,
    qr_token: BookingQrToken,
) -> dict:
    return {
        "id": qr_token.id,
        "booking_id": booking.id,
        "booking_code": booking.booking_code,
        "qr_type": qr_token.qr_type,
        "token": qr_token.token,
        "qr_url": f"{PUBLIC_API_BASE_URL}/scan/{qr_token.token}",
        "issued_at": qr_token.issued_at,
        "expires_at": qr_token.expires_at,
        "scanned_at": qr_token.scanned_at,
        "used_at": qr_token.used_at,
        "revoked_at": qr_token.revoked_at,
    }

def _get_owned_booking(
    db: Session,
    booking_id: int,
    user_id: int,
) -> Booking:
    booking = (
        db.query(Booking)
        .filter(
            Booking.id == booking_id,
            Booking.user_id == user_id,
        )
        .first()
    )

    if not booking:
        raise HTTPException(404, "Booking not found")

    return booking

def _revoke_unused_qr_tokens(
    db: Session,
    booking_id: int,
    now: datetime,
    qr_type: str | None = None,
) -> None:
    query = db.query(BookingQrToken).filter(
        BookingQrToken.booking_id == booking_id,
        BookingQrToken.revoked_at.is_(None),
        BookingQrToken.used_at.is_(None),
    )

    if qr_type is not None:
        query = query.filter(BookingQrToken.qr_type == qr_type)

    query.update(
        {BookingQrToken.revoked_at: now},
        synchronize_session=False,
    )

def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

def _booking_is_past_no_show_deadline(
    booking: Booking,
    now: datetime,
) -> bool:
    return (
        booking.entered_at is None
        and now >= _utc_datetime(booking.start_time) + ENTRY_QR_VALIDITY
    )

def _entry_qr_available_from(booking: Booking) -> datetime:
    return _utc_datetime(booking.start_time) - ENTRY_QR_EARLY_ACCESS

def _format_bangkok_datetime(value: datetime) -> str:
    return _utc_datetime(value).astimezone(
        ZoneInfo("Asia/Bangkok")
    ).strftime("%d/%m/%Y เวลา %H:%M น.")

def _set_slot_status(
    db: Session,
    slot: Slot,
    status: SlotStatus,
    source: str,
    now: datetime,
) -> bool:
    if (
        not source.startswith("admin:")
        and _latest_admin_rgb_command(db, slot.name, now) is not None
    ):
        return False
    if slot.status == status:
        return False
    slot.status = status
    db.add(
        SlotStatusLog(
            slot_id=slot.id,
            status=status,
            source=source,
            recorded_at=now,
        )
    )
    return True

def _mark_booking_time_out(
    db: Session,
    booking: Booking,
    now: datetime,
) -> str:
    if booking.status != "active" or not _booking_is_past_no_show_deadline(booking, now):
        return booking.slot.name
    booking.status = "time_out"
    _set_slot_status(
        db=db,
        slot=booking.slot,
        status=SlotStatus.available,
        source="booking_time_out",
        now=now,
    )
    payment = (
        db.query(Payment)
        .filter(Payment.booking_id == booking.id)
        .first()
    )
    if payment and payment.status == "pending":
        payment.status = "cancelled"
        payment.method = None
    _revoke_unused_qr_tokens(
        db=db,
        booking_id=booking.id,
        now=now,
    )
    return booking.slot.name

def _complete_payment(
    db: Session,
    booking: Booking,
    payment: Payment,
    method: str,
    now: datetime,
    card: PaymentCard | None = None,
) -> BookingQrToken:
    if payment.status == "paid":
        raise HTTPException(409, "Payment already completed")
    if booking.entered_at is None:
        raise HTTPException(409, "ต้องมีการยืนยันรถเข้าจอดก่อนชำระเงิน")

    base_amount = booking.total_amount
    overstay_fee = 50 if now > _utc_datetime(booking.end_time) else 0
    payment.base_amount = base_amount
    payment.overstay_fee = overstay_fee
    payment.amount = base_amount + overstay_fee
    payment.payment_card_id = card.id if card is not None else None
    payment.card_brand = card.card_brand if card is not None else None
    payment.card_last4 = card.card_number[-4:] if card is not None else None
    payment.status = "paid"
    payment.method = method
    payment.paid_at = now
    booking.status = "ready_to_exit"

    _revoke_unused_qr_tokens(
        db=db,
        booking_id=booking.id,
        now=now,
        qr_type="payment",
    )

    exit_qr = _active_qr_token(
        db=db,
        booking_id=booking.id,
        qr_type="exit",
    )

    if exit_qr is None:
        exit_qr = _create_qr_token(
            db=db,
            booking=booking,
            qr_type="exit",
        )
    return exit_qr

def _record_hardware_log(
    db: Session,
    device_name: str,
    status: str,
    detail: str | None = None,
    command_id: str | None = None,
    source: str | None = None,
) -> None:
    db.add(
        HardwareLog(
            device_name=device_name,
            status=status,
            detail=detail,
            command_id=command_id,
            source=source,
        )
    )

def _set_device_status(
    db: Session,
    device_name: str,
    status: str,
    now: datetime | None = None,
    seen: bool = True,
) -> None:
    observed_at = now or datetime.now(timezone.utc)

    device = (
        db.query(DeviceStatus)
        .filter(DeviceStatus.device_name == device_name)
        .first()
    )

    if device is None:
        if not seen:
            return
        db.add(
            DeviceStatus(
                device_name=device_name,
                status=status,
                last_updated=observed_at,
                last_seen_at=observed_at if seen else None,
            )
        )
        return

    device.status = status
    device.last_updated = observed_at
    if seen:
        device.last_seen_at = observed_at

def _record_device_observation(
    db: Session,
    device_name: str,
    status: str,
    detail: str,
    now: datetime,
    processed: bool = False,
) -> None:
    existing = (
        db.query(DeviceStatus)
        .filter(DeviceStatus.device_name == device_name)
        .first()
    )
    previous_status = existing.status if existing is not None else None
    if existing is None:
        db.add(
            DeviceStatus(
                device_name=device_name,
                status=status,
                last_updated=now,
                last_seen_at=now,
                last_processed_at=now if processed else None,
            )
        )
    else:
        existing.status = status
        existing.last_updated = now
        existing.last_seen_at = now
        if processed:
            existing.last_processed_at = now

    if previous_status != status:
        _record_hardware_log(
            db=db,
            device_name=device_name,
            status=status,
            detail=detail,
            source=device_name,
        )

async def monitor_device_liveness() -> None:
    while True:
        await asyncio.sleep(1)
        db = SessionLocal()
        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(seconds=DEVICE_OFFLINE_SECONDS)
            stale = (
                db.query(DeviceStatus)
                .filter(
                    DeviceStatus.status != "offline",
                    or_(
                        and_(
                            DeviceStatus.last_seen_at.is_not(None),
                            DeviceStatus.last_seen_at <= cutoff,
                        ),
                        and_(
                            DeviceStatus.last_seen_at.is_(None),
                            DeviceStatus.last_updated <= cutoff,
                        ),
                    ),
                )
                .with_for_update()
                .all()
            )
            for device in stale:
                device.status = "offline"
                device.last_updated = now
                _record_hardware_log(
                    db,
                    device.device_name,
                    "offline",
                    f"No signal for more than {DEVICE_OFFLINE_SECONDS} seconds",
                    source="liveness_monitor",
                )
            if stale:
                db.commit()
        except Exception:
            db.rollback()
            logger.exception("Unable to update device liveness")
        finally:
            db.close()

async def expire_admin_rgb_overrides() -> None:
    while True:
        await asyncio.sleep(15)
        db = SessionLocal()
        restored: list[tuple[str, str]] = []
        try:
            now = datetime.now(timezone.utc)
            day_start = _bangkok_day_start(now)
            for slot in db.query(Slot).with_for_update().all():
                if _latest_admin_rgb_command(db, slot.name, now) is not None:
                    continue
                last = (
                    db.query(SlotStatusLog)
                    .filter(SlotStatusLog.slot_id == slot.id)
                    .order_by(
                        SlotStatusLog.recorded_at.desc(),
                        SlotStatusLog.id.desc(),
                    )
                    .first()
                )
                if (
                    last is None
                    or not last.source.startswith("admin:")
                    or _utc_datetime(last.recorded_at) >= day_start
                ):
                    continue
                live = (
                    db.query(Booking)
                    .filter(
                        Booking.slot_id == slot.id,
                        Booking.status.in_(("active", "ready_to_exit")),
                    )
                    .first()
                )
                target = (
                    SlotStatus.occupied if live and live.entered_at is not None
                    else SlotStatus.reserved if live else SlotStatus.available
                )
                slot.status = target
                db.add(SlotStatusLog(
                    slot_id=slot.id,
                    status=target,
                    source="admin_expired",
                    recorded_at=now,
                ))
                restored.append((slot.name, target.value))
            if restored:
                db.commit()
        except Exception:
            db.rollback()
            logger.exception("Unable to expire Admin RGB overrides")
            restored = []
        finally:
            db.close()
        for slot_name, status in restored:
            _publish_slot_command(slot_name, status, source="admin_expired")
            schedule_slot_broadcast(slot_name, status)

def _record_slot_status(
    db: Session,
    slot: Slot,
    status: SlotStatus,
    source: str,
    now: datetime,
) -> bool:
    return _set_slot_status(db, slot, status, source, now)

def _record_gate_event(payload: dict) -> str | None:
    device_name = payload.get("device")
    gate = payload.get("gate")
    sensor = payload.get("sensor")
    event = payload.get("event")
    if (
        device_name not in {"esp32_cam", "esp32_cam_exit"}
        or not isinstance(gate, str)
        or not gate.strip()
        or not isinstance(sensor, str)
        or not sensor.strip()
        or not isinstance(event, str)
        or not event.strip()
    ):
        logger.warning("Ignoring Gate event with invalid source fields: %s", payload)
        return None
    db = SessionLocal()
    try:
        supplied_id = payload.get("event_id")
        event_id = (
            supplied_id.strip()
            if isinstance(supplied_id, str) and supplied_id.strip()
            else f"backend-{secrets.token_urlsafe(18)}"
        )
        if db.query(GateEvent.id).filter(GateEvent.event_id == event_id).first():
            return None
        raw_command_id = payload.get("command_id")
        command_id = (
            raw_command_id.strip()
            if isinstance(raw_command_id, str) and raw_command_id.strip()
            else None
        )
        raw_code = payload.get("booking_code")
        booking_code = (
            raw_code.strip()
            if isinstance(raw_code, str) and raw_code.strip()
            else None
        )
        command = None
        booking = None
        if command_id is not None:
            command = (
                db.query(HardwareCommand)
                .filter(HardwareCommand.command_id == command_id)
                .first()
            )
            if (
                command is None
                or command.device_name != "esp32_cam"
                or (command.gate != gate and command.gate != "admin")
            ):
                logger.warning("Gate event has unknown or mismatched command_id")
                return None
            if command.booking_id is not None:
                booking = (
                    db.query(Booking)
                    .filter(Booking.id == command.booking_id)
                    .first()
                )
            if booking_code is not None and (
                booking is None or booking.booking_code != booking_code
            ):
                logger.warning("Gate event booking_code and command_id disagree")
                return None
        elif booking_code is not None:
            booking = (
                db.query(Booking)
                .filter(Booking.booking_code == booking_code)
                .first()
            )
        db.add(GateEvent(
            event_id=event_id,
            device_name=device_name,
            gate=gate,
            sensor=sensor,
            event=event,
            command_id=command_id,
            booking_id=booking.id if booking is not None else None,
            payload=payload,
            received_at=datetime.now(timezone.utc),
        ))
        db.commit()
        return event_id
    except Exception:
        db.rollback()
        logger.exception("Unable to persist Gate event")
        return None
    finally:
        db.close()

def _publish_gate_display_state() -> None:
    db = SessionLocal()
    try:
        counts = {status.value: 0 for status in SlotStatus}
        for status, count in (
            db.query(Slot.status, func.count(Slot.id))
            .group_by(Slot.status)
            .all()
        ):
            counts[status.value] = int(count)
        payload = {
            "available_count": counts[SlotStatus.available.value],
            "occupied_count": counts[SlotStatus.occupied.value],
            "reserved_count": counts[SlotStatus.reserved.value],
            "total_count": sum(counts.values()),
        }
    except Exception:
        logger.exception("Unable to read actual slot counts for Gate OLED")
        return
    finally:
        db.close()
    try:
        result = mqtt_client.publish(
            GATE_DISPLAY_TOPIC,
            json.dumps(payload),
            qos=1,
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "Gate OLED state publish failed: result=%s",
                result.rc,
            )
    except Exception:
        logger.exception("Unable to publish Gate OLED state")

def _set_gate_state(
    db: Session,
    action: str,
    source: str,
    command_id: str | None,
    now: datetime,
) -> None:
    state = (
        db.query(GateState)
        .filter(GateState.gate == "main")
        .with_for_update()
        .first()
    )
    if state is None:
        db.add(
            GateState(
                gate="main",
                last_action=action,
                source=source,
                command_id=command_id,
                updated_at=now,
            )
        )
        return
    state.last_action = action
    state.source = source
    state.command_id = command_id
    state.updated_at = now

def _set_gate_state_from_hardware_command(
    db: Session,
    command: HardwareCommand,
    now: datetime,
) -> None:
    if command.device_name != "esp32_cam":
        return
    if command.gate not in {"entry", "exit", "admin"}:
        return
    if command.action not in {"open", "close"}:
        return
    if command.source == "qr":
        source = f"qr_{command.gate}"
    else:
        source = command.source
    _set_gate_state(
        db=db,
        action=command.action,
        source=source,
        command_id=command.command_id,
        now=now,
    )
def _record_gate_event_action(
    action: str,
    source: str,
    sensor: str | None = None,
    sensor_device: str | None = None,
) -> None:
    db = SessionLocal()
    reassert_command: HardwareCommand | None = None
    try:
        now = datetime.now(timezone.utc)
        _set_gate_state(
            db=db,
            action=action,
            source=source,
            command_id=None,
            now=now,
        )
        _record_device_observation(
            db=db,
            device_name="esp32_cam",
            status="online",
            detail=f"Gate action executed by Entry controller: {source}",
            now=now,
            processed=True,
        )
        _record_device_observation(
            db=db,
            device_name="servo",
            status="online",
            detail=f"Servo gate action observed: {action}",
            now=now,
            processed=True,
        )
        if sensor is not None:
            _record_device_observation(
                db=db,
                device_name=sensor.lower(),
                status="online",
                detail=f"Sensor event received: {sensor}",
                now=now,
                processed=True,
            )
        if sensor_device is not None:
            _record_device_observation(
                db=db,
                device_name=sensor_device,
                status="online",
                detail=f"Sensor event relayed by Entry controller: {sensor}",
                now=now,
                processed=True,
            )
        _record_hardware_log(
            db=db,
            device_name="esp32_cam",
            status="gate_event",
            detail=(
                f"action={action} | source={source}"
                f"{f' | sensor={sensor}' if sensor else ''}"
                f"{f' | sensor_device={sensor_device}' if sensor_device else ''}"
            ),
            source="esp32_cam",
        )
        admin_command = _latest_admin_servo_command(db, now)
        if (
            admin_command is not None
            and admin_command.status == "acknowledged"
            and admin_command.action != action
        ):
            _record_hardware_log(
                db, "servo", "admin_conflict",
                f"Observed {action} while Admin requested {admin_command.action}",
                admin_command.command_id, "esp32_cam",
            )
            reassert_command = _queue_servo_command(
                db, None, None, "admin", admin_command.action, source="admin"
            )
            _stamp_admin_override(reassert_command, now)
        db.commit()

        if reassert_command is not None:
            db.refresh(reassert_command)
    except Exception:
        db.rollback()
        reassert_command = None
        logger.exception(
            "Unable to record gate event action %s from %s",
            action,
            source,
        )
    finally:
        db.close()
    if reassert_command is not None:
        _publish_servo_command(reassert_command)

def _finish_hardware_command(
    db: Session,
    command: HardwareCommand,
    status: str,
    detail: str,
    now: datetime,
    result: dict | None = None,
) -> None:
    command.status = status

    if status == "acknowledged":
        command.acknowledged_at = now
        _set_gate_state_from_hardware_command(
            db=db,
            command=command,
            now=now,
        )
    if result is not None:
        command.result = result
        command.result_received_at = now
    if status == "acknowledged" and command.gate == "slot" and command.action == "set_status":
        command_payload = command.payload or {}
        slot_name = command_payload.get("slot")
        requested_status = command_payload.get("status")
        if isinstance(slot_name, str) and requested_status in {
            item.value for item in SlotStatus
        }:
            slot = (
                db.query(Slot)
                .filter(Slot.name == slot_name)
                .with_for_update()
                .first()
            )
            if slot is not None:
                _record_slot_status(
                    db=db,
                    slot=slot,
                    status=SlotStatus(requested_status),
                    source=command.source,
                    now=now,
                )
    command.detail = (
        f"{command.detail} | {detail}"
        if command.detail
        else detail
    )

    device_status = "online" if status == "acknowledged" else status
    _set_device_status(
        db,
        command.device_name,
        device_status,
        now=now,
        seen=result is not None,
    )
    _record_hardware_log(
        db=db,
        device_name=command.device_name,
        status=status,
        detail=f"command_id={command.command_id} | {detail}",
        command_id=command.command_id,
        source=command.source,
    )
    component_name = None
    if command.device_name == "esp32_cam" and command.action in {"open", "close"}:
        component_name = "servo"
    elif command.gate == "slot":
        slot_name = (command.payload or {}).get("slot")
        if slot_name in {"M", "N", "O"}:
            component_name = f"rgb_{slot_name.lower()}"

    if component_name is not None:
        component_status = "online" if status == "acknowledged" else "failed"
        _set_device_status(
            db, component_name, component_status, now=now,
            seen=result is not None,
        )
        if status != "acknowledged":
            _record_hardware_log(
                db, component_name, status,
                f"command_id={command.command_id} | {detail}",
                command.command_id, command.source,
            )

def _mark_hardware_command_failed(
    command_id: str,
    detail: str,
) -> None:
    db = SessionLocal()

    try:
        command = (
            db.query(HardwareCommand)
            .filter(HardwareCommand.command_id == command_id)
            .with_for_update()
            .first()
        )

        if command and command.status == "queued":
            _finish_hardware_command(
                db=db,
                command=command,
                status="failed",
                detail=detail,
                now=datetime.now(timezone.utc),
            )
            db.commit()
    except Exception:
        db.rollback()
        logger.exception(
            "Unable to mark hardware command %s as failed",
            command_id,
        )
    finally:
        db.close()

def _mark_hardware_command_published(command_id: str) -> None:
    db = SessionLocal()

    try:
        command = (
            db.query(HardwareCommand)
            .filter(HardwareCommand.command_id == command_id)
            .with_for_update()
            .first()
        )
        if command is None:
            logger.warning(
                "Published hardware command was not found: %s",
                command_id,
            )
            return

        if command.published_at is None:
            command.published_at = datetime.now(timezone.utc)
            db.commit()
    except Exception:
        db.rollback()
        logger.exception(
            "Unable to record publish time for hardware command %s",
            command_id,
        )
    finally:
        db.close()

def _handle_hardware_ack(payload: dict) -> None:
    command_id = payload.get("command_id")
    device_name = payload.get("device")
    ack_status = payload.get("status")
    detail = payload.get("detail") or "Hardware response received"

    if (
        not isinstance(command_id, str)
        or not command_id.strip()
        or not isinstance(device_name, str)
        or not device_name.strip()
        or ack_status not in {"acknowledged", "failed"}
    ):
        logger.warning("Ignoring invalid hardware ACK payload: %s", payload)
        return
    slot_to_broadcast: str | None = None
    db = SessionLocal()

    try:
        command = (
            db.query(HardwareCommand)
            .filter(HardwareCommand.command_id == command_id.strip())
            .with_for_update()
            .first()
        )
        if command is None or command.device_name != device_name.strip():
            logger.warning("Unknown command or ACK device mismatch: %s", command_id)
            return
        if command.status in {"acknowledged", "superseded"}:
            return
        if command.status not in {"queued", "failed", "no_response"}:
            return
        now = datetime.now(timezone.utc)

        if (
            (command.source == "admin" or command.source.startswith("admin:"))
            and command.created_at is not None
            and _utc_datetime(command.created_at) < _bangkok_day_start(now)
        ):
            command.status = "superseded"
            db.commit()
            return
        
        auto_servo = (
            command.device_name == "esp32_cam"
            and command.source != "admin"
            and command.action in {"open", "close"}
            and _latest_admin_servo_command(db, now) is not None
        )
        slot_name = (
            (command.payload or {}).get("slot")
            if command.gate == "slot"
            else None
        )
        auto_rgb = (
            isinstance(slot_name, str)
            and not command.source.startswith("admin:")
            and _latest_admin_rgb_command(db, slot_name, now) is not None
        )
        if auto_servo or auto_rgb:
            command.status = "superseded"
            command.result = payload
            command.result_received_at = now

            _record_hardware_log(
                db,
                command.device_name,
                "superseded",
                f"ACK arrived after Admin override: {command.command_id}",
                command.command_id,
                command.source,
            )
            db.commit()
            return

        _finish_hardware_command(
            db=db,
            command=command,
            status=ack_status,
            detail=detail,
            now=now,
            result=payload,
        )
        if (
            ack_status == "acknowledged"
            and command.gate == "slot"
            and isinstance(slot_name, str)
        ):
            slot_to_broadcast = slot_name
        db.commit()

    except Exception:
        db.rollback()
        logger.exception("Unable to process hardware ACK for %s", command_id)
        return
    finally:
        db.close()
    if slot_to_broadcast is not None:
        schedule_slot_broadcast(slot_to_broadcast, "")
        _publish_gate_display_state()

async def monitor_hardware_command_timeouts() -> None:
    while True:
        await asyncio.sleep(HARDWARE_ACK_POLL_SECONDS)

        db = SessionLocal()

        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(
                seconds=HARDWARE_ACK_TIMEOUT_SECONDS
            )

            commands = (
                db.query(HardwareCommand)
                .filter(
                    HardwareCommand.status == "queued",
                    HardwareCommand.published_at.is_not(None),
                    HardwareCommand.published_at <= cutoff,
                )
                .with_for_update()
                .all()
            )

            for command in commands:
                _finish_hardware_command(
                    db=db,
                    command=command,
                    status="no_response",
                    detail=(
                        "No hardware ACK received within "
                        f"{HARDWARE_ACK_TIMEOUT_SECONDS} seconds"
                    ),
                    now=now,
                )

            if commands:
                db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                "Unable to monitor hardware command timeouts"
            )
        finally:
            db.close()

def _publish_hardware_command(
    command: HardwareCommand | str,
    topic: str,
    payload: dict,
) -> bool:
    command_id = (
        command.command_id
        if isinstance(command, HardwareCommand)
        else command
    )
    db = SessionLocal()
    try:
        persisted = (
            db.query(HardwareCommand)
            .filter(HardwareCommand.command_id == command_id)
            .first()
        )
        if (
            persisted is None
            or persisted.status != "queued"
            or _command_retry_topic(
                db, persisted, datetime.now(timezone.utc)
            ) != topic
        ):
            if persisted is not None and persisted.status == "queued":
                persisted.status = "superseded"
                db.commit()
            return False
    finally:
        db.close()
    if not mqtt_client.is_connected():
        _mark_hardware_command_failed(
            command_id,
            "MQTT broker is not connected; hardware command was not sent",
        )
        return False
    
    try:
        result = mqtt_client.publish(
            topic,
            json.dumps(payload),
            qos=1,
        )
    except Exception as error:
        _mark_hardware_command_failed(
            command_id,
            f"MQTT publish raised an error: {error}",
        )
        return False
    
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        _mark_hardware_command_failed(
            command_id,
            f"MQTT publish failed with result code {result.rc}",
        )
        return False
    _mark_hardware_command_published(command_id)
    return True

def _command_retry_topic(
    db: Session, command: HardwareCommand, now: datetime,
) -> str | None:
    payload = command.payload
    if not isinstance(payload, dict):
        return None
    if command.gate == "slot":
        slot_name = payload.get("slot")
        if not isinstance(slot_name, str):
            return None
        if command.source.startswith("admin:"):
            latest = _latest_admin_rgb_command(db, slot_name, now)
            if latest is None or latest.id != command.id:
                return None
        else:
            slot = db.query(Slot).filter(Slot.name == slot_name).first()
            if (
                slot is None
                or slot.status.value != payload.get("status")
                or _latest_admin_rgb_command(db, slot_name, now) is not None
            ):
                return None
        return f"parking/slot/{slot_name}/command"
    if command.device_name != "esp32_cam":
        return None
    if command.source == "admin":
        latest = _latest_admin_servo_command(db, now)
        if latest is None or latest.id != command.id:
            return None
        return "parking/servo/command"
    if command.gate not in {"entry", "exit"}:
        return None
    if _latest_admin_servo_command(db, now) is not None:
        return None
    booking = db.query(Booking).filter(Booking.id == command.booking_id).first()
    qr_token = db.query(BookingQrToken).filter(
        BookingQrToken.id == command.qr_token_id
    ).first()
    if (
        booking is None or qr_token is None
        or qr_token.booking_id != booking.id
        or qr_token.qr_type != command.gate
        or qr_token.used_at is None
        or qr_token.revoked_at is not None
    ):
        return None
    
    if command.gate == "entry":
        if (
            booking.status != "active"
            or booking.entered_at is not None
            or _booking_is_past_no_show_deadline(booking, now)
        ):
            return None
        if (
            command.action == "open"
            and now < _entry_qr_available_from(booking)
        ):
            return None
    else:
        payment = (
            db.query(Payment)
            .filter(
                Payment.booking_id == booking.id,
                Payment.status == "paid",
            )
            .first()
        )
        if (
            booking.status != "ready_to_exit"
            or booking.entered_at is None
            or payment is None
        ):
            return None
    if (
        command.action == "open"
        and command.source == f"{command.gate}_ir_pair"
    ):
        return "parking/servo/command"
    return None

async def retry_unpublished_hardware_commands() -> None:
    while True:
        await asyncio.sleep(3)
        if not mqtt_client.is_connected():
            continue
        db = SessionLocal()
        to_publish: list[tuple[str, str, dict]] = []
        try:
            now = datetime.now(timezone.utc)
            commands = (
                db.query(HardwareCommand)
                .filter(
                    or_(
                        and_(
                            HardwareCommand.status.in_(("queued", "failed")),
                            HardwareCommand.published_at.is_(None),
                        ),
                        HardwareCommand.status == "no_response",
                    )
                )
                .order_by(HardwareCommand.created_at.asc())
                .limit(50)
                .with_for_update(skip_locked=True)
                .all()
            )
            for command in commands:
                topic = _command_retry_topic(db, command, now)
                if topic is None:
                    command.status = "superseded"
                    _record_hardware_log(
                        db, command.device_name, "superseded",
                        f"Command no longer eligible: {command.command_id}",
                        command.command_id, command.source,
                    )
                    continue
                command.status = "queued"
                command.published_at = None
                command.detail = (
                    f"{command.detail} | continuing after reconnect"
                    if command.detail else "Continuing after reconnect"
                )
                to_publish.append(
                    (command.command_id, topic, command.payload)
                )
            if commands:
                db.commit()
        except Exception:
            db.rollback()
            logger.exception("Unable to retry hardware commands")
            to_publish = []
        finally:
            db.close()
        for command_id, topic, payload in to_publish:
            _publish_hardware_command(command_id, topic, payload)

def _queue_servo_command(
    db: Session,
    booking: Booking | None,
    qr_token: BookingQrToken | None,
    gate: str,
    action: str,
    source: str = "qr",
) -> HardwareCommand:
    if source == "entry_ir_pair":
        detail = "IR1 and IR2 confirmed a vehicle; entry QR was verified"
    elif source == "exit_ir_pair":
        detail = "IR4 and IR5 confirmed a vehicle; exit QR was verified"
    elif qr_token is not None:
        detail = f"{gate} gate command from {qr_token.qr_type} QR"
    else:
        detail = "Admin servo override requested"

    command = HardwareCommand(
        command_id=f"cmd-{secrets.token_urlsafe(18)}",
        qr_token_id=qr_token.id if qr_token is not None else None,
        booking_id=booking.id if booking is not None else None,
        gate=gate,
        device_name="esp32_cam",
        action=action,
        source=source,
        status="queued",
        target_device="esp32_cam",
        detail=detail,
    )
    command_payload = {
        "command_id": command.command_id,
        "device": command.device_name,
        "gate": command.gate,
        "action": command.action,
        "source": command.source,
    }
    if booking is not None:
        command_payload["booking_code"] = booking.booking_code
    if gate == "entry":
        command_payload["close_sensor"] = "IR3"
        command_payload["sensor_device"] = "esp32_cam"
    elif gate == "exit":
        command_payload["close_sensor"] = "IR6"
        command_payload["sensor_device"] = "esp32_cam_exit"

    command.payload = command_payload
    db.add(command)
    return command

def _queue_slot_command(
    db: Session,
    slot_name: str,
    status: str,
    source: str,
) -> HardwareCommand:
    command = HardwareCommand(
        command_id=f"cmd-{secrets.token_urlsafe(18)}",
        qr_token_id=None,
        booking_id=None,
        gate="slot",
        device_name="esp32_wroom",
        action="set_status",
        source=source,
        status="queued",
        target_device="esp32_wroom",
        detail=f"slot={slot_name} | requested_status={status}",
    )
    command.payload = {
        "command_id": command.command_id,
        "device": command.device_name,
        "action": command.action,
        "source": command.source,
        "slot": slot_name,
        "status": status,
    }
    db.add(command)
    return command

def _publish_servo_command(
    command: HardwareCommand,
    booking: Booking | None = None,
) -> bool:
    payload = command.payload or {
        "command_id": command.command_id,
        "device": command.device_name,
        "gate": command.gate,
        "action": command.action,
        "source": command.source,
    }
    if booking is not None:
        payload["booking_code"] = booking.booking_code
    if command.gate == "entry":
        payload["close_sensor"] = "IR3"
        payload["sensor_device"] = "esp32_cam"
    elif command.gate == "exit":
        payload["close_sensor"] = "IR6"
        payload["sensor_device"] = "esp32_cam_exit"
    return _publish_hardware_command(
        command=command,
        topic="parking/servo/command",
        payload=payload,
    )

async def _publish_slot_state(
    slot_name: str,
    status: str,
    source: str = "backend",
) -> None:
    db = SessionLocal()
    try:
        slot = db.query(Slot).filter(Slot.name == slot_name).first()
        if slot is not None:
            status = slot.status.value
    finally:
        db.close()
    _publish_slot_command(
        slot_name=slot_name,
        status=status,
        source=source,
    )

    await broadcast_slot_update(
        {
            "slot": slot_name,
            "status": status,
        }
    )

def _publish_slot_command(
    slot_name: str,
    status: str,
    source: str = "backend",
) -> None:
    db = SessionLocal()

    try:
        if (
            not source.startswith("admin:")
            and _latest_admin_rgb_command(
                db, slot_name, datetime.now(timezone.utc)
            ) is not None
        ):
            return
        command = _queue_slot_command(
            db=db,
            slot_name=slot_name,
            status=status,
            source=source,
        )
        db.commit()
        db.refresh(command)
    except Exception:
        db.rollback()
        logger.exception(
            "Unable to queue RGB command for slot %s",
            slot_name,
        )
        return
    finally:
        db.close()

    _publish_hardware_command(
        command=command,
        topic=f"parking/slot/{slot_name}/command",
        payload=command.payload or {
            "command_id": command.command_id,
            "device": command.device_name,
            "action": command.action,
            "source": command.source,
            "slot": slot_name,
            "status": status,
        },
    )
    _publish_gate_display_state()

def _handle_slot_vision_update(payload: dict) -> None:
    device_name = payload.get("device")
    status = payload.get("status", "online")
    observations = payload.get("slots")
    detail = payload.get("detail")

    if (
        device_name != "slot_webcam"
        or status not in REPORTED_DEVICE_STATUSES
        or not isinstance(observations, list)
    ):
        logger.warning(
            "Ignoring invalid Slot Webcam payload: %s",
            payload,
        )
        return

    if not isinstance(detail, str) or not detail.strip():
        detail = "Slot Webcam submitted a parking-space analysis"

    normalized_observations: list[tuple[str, bool]] = []
    observed_names: set[str] = set()

    for observation in observations:
        if not isinstance(observation, dict):
            logger.warning(
                "Ignoring invalid Slot Webcam observation: %s",
                observation,
            )
            return

        slot_name = observation.get("name")
        occupied = observation.get("occupied")

        if (
            slot_name not in {"M", "N", "O"}
            or not isinstance(occupied, bool)
            or slot_name in observed_names
        ):
            logger.warning(
                "Ignoring invalid Slot Webcam observation: %s",
                observation,
            )
            return

        observed_names.add(slot_name)
        normalized_observations.append((slot_name, occupied))
        required_slot_names = {"M", "N", "O"}
    if observed_names != required_slot_names:
        logger.warning(
            "Ignoring incomplete Slot Webcam snapshot: expected=%s received=%s",
            sorted(required_slot_names),
            sorted(observed_names),
        )
        return

    changed_slots: list[tuple[str, str]] = []
    db = SessionLocal()

    try:
        now = datetime.now(timezone.utc)

        _record_device_observation(
            db=db,
            device_name="slot_webcam",
            status=status,
            detail=detail,
            now=now,
            processed=True,
        )

        for slot_name, occupied in normalized_observations:
            slot = (
                db.query(Slot)
                .filter(Slot.name == slot_name)
                .with_for_update()
                .first()
            )

            if slot is None:
                logger.warning(
                    "Slot Webcam reported a slot absent from the database: %s",
                    slot_name,
                )
                continue

            if _latest_admin_rgb_command(db, slot_name, now) is not None:
                continue

            live_booking = (
                db.query(Booking)
                .filter(
                    Booking.slot_id == slot.id,
                    Booking.status.in_(("active", "ready_to_exit")),
                )
                .first()
            )
            if occupied or (live_booking and live_booking.entered_at is not None):
                target_status = SlotStatus.occupied
            elif live_booking:
                target_status = SlotStatus.reserved
            else:
                target_status = SlotStatus.available

            if slot.status == target_status:
                continue

            changed = _record_slot_status(
                db=db,
                slot=slot,
                status=target_status,
                source="slot_webcam",
                now=now,
            )
            if not changed:
                continue
            changed_slots.append(
                (slot.name, target_status.value)
            )

            _record_hardware_log(
                db=db,
                device_name="slot_webcam",
                status="slot_status_confirmed",
                detail=(
                    f"slot={slot.name} | "
                    f"status={target_status.value}"
                ),
                source="slot_webcam",
            )

        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Unable to process Slot Webcam analysis")
        return
    finally:
        db.close()

    for slot_name, slot_status in changed_slots:
        _publish_slot_command(
            slot_name=slot_name,
            status=slot_status,
            source="slot_webcam",
        )
        schedule_slot_broadcast(
            slot_name=slot_name,
            status=slot_status,
        )
    if changed_slots:
        _publish_gate_display_state()

def _replay_slot_states_to_wroom() -> None:
    slot_states: list[tuple[str, str, str]] = []
    db = SessionLocal()

    try:
        now = datetime.now(timezone.utc)
        valid_statuses = {status.value for status in SlotStatus}

        slots = (
            db.query(Slot)
            .filter(Slot.name.in_(("M", "N", "O")))
            .order_by(Slot.name)
            .all()
        )

        for slot in slots:
            status = slot.status.value
            source = "backend_sync"

            admin_command = _latest_admin_rgb_command(
                db,
                slot.name,
                now,
            )

            if admin_command is not None:
                payload = admin_command.payload or {}
                admin_status = (
                    payload.get("status")
                    if isinstance(payload, dict)
                    else None
                )

                if admin_status in valid_statuses:
                    status = admin_status

                source = admin_command.source

            slot_states.append((slot.name, status, source))

    except Exception:
        logger.exception(
            "Unable to prepare RGB state replay for ESP32-WROOM"
        )
        return
    finally:
        db.close()

    for slot_name, status, source in slot_states:
        _publish_slot_command(slot_name, status, source=source)


def _handle_hardware_heartbeat(payload: dict) -> None:
    device_name = payload.get("device")
    status = payload.get("status")
    detail = payload.get("detail")
    components = payload.get("components", [])
    sync_requested = payload.get("sync_requested", False)

    if (
        device_name not in KNOWN_HARDWARE_DEVICES
        or status not in REPORTED_DEVICE_STATUSES
        or not isinstance(components, list)
        or not isinstance(sync_requested, bool)
    ):
        logger.warning(
            "Ignoring invalid hardware heartbeat: %s",
            payload,
        )
        return
    
    if not isinstance(detail, str) or not detail.strip():
        detail = f"Heartbeat received from {device_name}"
    normalized_components: list[tuple[str, str, str]] = []

    for component in components:
        if not isinstance(component, dict):
            logger.warning(
                "Ignoring invalid heartbeat component: %s",
                component,
            )
            return

        component_name = component.get("device")
        component_status = component.get("status")
        component_detail = component.get("detail")

        if (
            component_name not in KNOWN_HARDWARE_DEVICES
            or component_status not in REPORTED_DEVICE_STATUSES
        ):
            logger.warning(
                "Ignoring invalid heartbeat component: %s",
                component,
            )
            return
        
        if not isinstance(component_detail, str) or not component_detail.strip():
            component_detail = (
                f"Heartbeat received through {device_name}"
            )
        normalized_components.append(
            (
                component_name,
                component_status,
                component_detail,
            )
        )
    replay_slot_states = (
        device_name == "esp32_wroom"
        and status == "online"
        and sync_requested
    )
    db = SessionLocal()

    try:
        now = datetime.now(timezone.utc)
        _record_device_observation(
            db=db,
            device_name=device_name,
            status=status,
            detail=detail,
            now=now,
        )
        for component_name, component_status, component_detail in normalized_components:
            _record_device_observation(
                db=db,
                device_name=component_name,
                status=component_status,
                detail=component_detail,
                now=now,
            )
        db.commit()

    except Exception:
        db.rollback()
        logger.exception(
            "Unable to process heartbeat from %s",
            device_name,
        )
        return
    
    finally:
        db.close()
    if replay_slot_states:
        _replay_slot_states_to_wroom()

def _bangkok_day_start(now: datetime) -> datetime:
    local_date = now.astimezone(ZoneInfo("Asia/Bangkok")).date()
    return datetime.combine(
        local_date, datetime.min.time(), tzinfo=ZoneInfo("Asia/Bangkok")
    ).astimezone(timezone.utc)

def _stamp_admin_override(command: HardwareCommand, now: datetime) -> None:
    payload = dict(command.payload or {})
    payload["priority"] = "admin"
    payload["override_until"] = _bangkok_day_start(
        now + timedelta(days=1)
    ).isoformat()
    command.payload = payload

def _latest_admin_servo_command(
    db: Session, now: datetime,
) -> HardwareCommand | None:
    return (
        db.query(HardwareCommand)
        .filter(
            HardwareCommand.device_name == "esp32_cam",
            HardwareCommand.source == "admin",
            HardwareCommand.action.in_(("open", "close")),
            HardwareCommand.created_at >= _bangkok_day_start(now),
            HardwareCommand.status != "superseded",
        )
        .order_by(HardwareCommand.created_at.desc(), HardwareCommand.id.desc())
        .first()
    )

def _latest_admin_rgb_command(
    db: Session, slot_name: str, now: datetime,
) -> HardwareCommand | None:
    commands = (
        db.query(HardwareCommand)
        .filter(
            HardwareCommand.device_name == "esp32_wroom",
            HardwareCommand.gate == "slot",
            HardwareCommand.action == "set_status",
            HardwareCommand.source.like("admin:%"),
            HardwareCommand.created_at >= _bangkok_day_start(now),
            HardwareCommand.status != "superseded",
        )
        .order_by(HardwareCommand.created_at.desc(), HardwareCommand.id.desc())
        .all()
    )
    return next(
        (command for command in commands
         if isinstance(command.payload, dict)
         and command.payload.get("slot") == slot_name),
        None,
    )

def _gate_pair_confirmed(gate: str, payload: dict) -> bool:
    if gate not in {"entry", "exit"} or payload.get("gate") != gate:
        return False
    pair = ("IR1", "IR2") if gate == "entry" else ("IR4", "IR5")
    active = payload.get("active_sensors")
    states = payload.get("sensor_states")
    both_active = (
        isinstance(active, list)
        and all(isinstance(item, str) for item in active)
        and set(pair).issubset(set(active))
    ) or (
        isinstance(states, dict)
        and all(states.get(item) is True for item in pair)
    )
    return both_active and payload.get("vehicle_detected") is True

def _queue_normal_gate_open_after_verified_qr(
    db: Session,
    booking: Booking,
    qr_token: BookingQrToken,
    now: datetime,
) -> tuple[str, HardwareCommand | None]:
    gate = qr_token.qr_type

    if gate not in {"entry", "exit"}:
        raise ValueError("Normal gate opening requires an entry or exit QR token")
    if _latest_admin_servo_command(db, now) is not None:
        return "admin_override", None

    expected_sensors = (
        ("IR1", "IR2")
        if gate == "entry"
        else ("IR4", "IR5")
    )

    pending_events = (
        db.query(GateEvent)
        .filter(
            GateEvent.gate == gate,
            GateEvent.sensor.in_(expected_sensors),
            GateEvent.event.in_(("vehicle_detected", "sensor_triggered")),
            GateEvent.command_id.is_(None),
            GateEvent.booking_id.is_(None),
            GateEvent.received_at >= now - GATE_PAIR_ACK_VALIDITY,
        )
        .order_by(
            GateEvent.received_at.desc(),
            GateEvent.id.desc(),
        )
        .with_for_update(skip_locked=True)
        .all()
    )
    matched_event = next(
        (
            event
            for event in pending_events
            if isinstance(event.payload, dict)
            and _gate_pair_confirmed(gate, event.payload)
        ),
        None,
    )
    if matched_event is None:
        return "waiting_for_vehicle", None

    command = _queue_servo_command(
        db=db,
        booking=booking,
        qr_token=qr_token,
        gate=gate,
        action="open",
        source=f"{gate}_ir_pair",
    )
    db.flush()

    matched_event.booking_id = booking.id
    matched_event.command_id = command.command_id

    return "queued", command

def _complete_passage_after_ir(
    gate: str,
    booking_code: str | None,
    command_id: str | None,
    event_id: str,
) -> None:
    if gate not in {"entry", "exit"} or not command_id:
        logger.warning("Ignoring %s passage without an exact open command", gate)
        return
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        command = (
            db.query(HardwareCommand)
            .filter(
                HardwareCommand.command_id == command_id,
                HardwareCommand.gate == gate,
                HardwareCommand.action == "open",
                HardwareCommand.source == f"{gate}_ir_pair",
                HardwareCommand.device_name == "esp32_cam",
                HardwareCommand.status.in_((
                    "queued", "acknowledged", "no_response", "superseded"
                )),
                HardwareCommand.booking_id.is_not(None),
                HardwareCommand.qr_token_id.is_not(None),
            )
            .first()
        )
        if command is None:
            logger.warning("IR passage has no matching normal-open command: %s", command_id)
            return
        if command.status == "superseded" and command.published_at is None:
            return
        booking = (
            db.query(Booking)
            .filter(Booking.id == command.booking_id)
            .with_for_update()
            .first()
        )
        qr_token = (
            db.query(BookingQrToken)
            .filter(BookingQrToken.id == command.qr_token_id)
            .first()
        )
        if (
            booking is None or qr_token is None
            or qr_token.booking_id != booking.id
            or qr_token.qr_type != gate
            or qr_token.used_at is None
            or qr_token.revoked_at is not None
            or (booking_code is not None and booking_code != booking.booking_code)
        ):
            logger.warning("IR passage and scanned QR do not match")
            return
        if gate == "entry":
            if (
                booking.status != "active"
                or booking.entered_at is not None
                or _booking_is_past_no_show_deadline(booking, now)
            ):
                return
            booking.entered_at = now
            event_name = "booking_entered"
        else:
            payment = (
                db.query(Payment)
                .filter(Payment.booking_id == booking.id)
                .first()
            )
            if (
                booking.status != "ready_to_exit"
                or payment is None
                or payment.status != "paid"
            ):
                return
            booking.status = "completed"
            booking.exited_at = now
            _record_slot_status(
                db, booking.slot, SlotStatus.available, "exit_ir6", now
            )
            event_name = "booking_completed"
        db.query(GateEvent).filter(GateEvent.event_id == event_id).update(
            {GateEvent.booking_id: booking.id}, synchronize_session=False
        )
        db.commit()
        if gate == "exit":
            _publish_slot_command(booking.slot.name, "available", source="exit_ir6")
            schedule_slot_broadcast(booking.slot.name, "available")
        schedule_booking_broadcast(booking.user_id, booking.id, event_name)
    except Exception:
        db.rollback()
        logger.exception("Unable to record %s passage for %s", gate, command_id)
    finally:
        db.close()

def _handle_gate_pair(gate: str, sensor: str, payload: dict) -> None:
    pair = ("IR1", "IR2") if gate == "entry" else ("IR4", "IR5")
    expected_device = "esp32_cam" if gate == "entry" else "esp32_cam_exit"
    device_name = payload.get("device")
    if device_name != expected_device:
        logger.warning(
            "Ignoring %s pair event from %s; expected %s",
            gate,
            device_name,
            expected_device,
        )
        return

    simultaneous_pair = _gate_pair_confirmed(gate, payload)
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        _record_device_observation(
            db,
            device_name,
            "online",
            f"{device_name} triggered {sensor}",
            now,
            processed=True,
        )
        _record_device_observation(
            db, sensor.lower(), "online", f"Gate sensor triggered: {sensor}", now,
            processed=True,
        )
        if simultaneous_pair:
            other_sensor = pair[1] if sensor == pair[0] else pair[0]
            _record_device_observation(
                db, other_sensor.lower(), "online",
                f"Gate pair confirmed with {sensor}", now,
                processed=True,
            )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Unable to pair gate sensors at %s", gate)
        return
    finally:
        db.close()
    if not simultaneous_pair:
        return
    logger.info(
        "%s IR pair confirmed; waiting for the user's QR scan",
        gate,
    )

def _handle_gate_event(payload: dict) -> None:
    event = payload.get("event")
    gate = payload.get("gate")
    sensor = payload.get("sensor")
    device_name = payload.get("device")
    sensor_device = payload.get("sensor_device")
    entry_boot = (
        device_name == "esp32_cam"
        and event == "gate_closed"
        and sensor == "boot"
        and gate in {"main", "entry", "exit"}
    )
    entry_pair = (
        device_name == "esp32_cam"
        and event in {"sensor_triggered", "vehicle_detected"}
        and gate == "entry"
        and sensor in {"IR1", "IR2"}
    )
    exit_pair = (
        device_name == "esp32_cam_exit"
        and event in {"sensor_triggered", "vehicle_detected"}
        and gate == "exit"
        and sensor in {"IR4", "IR5"}
    )
    entry_passage = (
        device_name == "esp32_cam"
        and event == "gate_closed"
        and (gate, sensor) == ("entry", "IR3")
    )
    exit_passage = (
        device_name == "esp32_cam"
        and sensor_device == "esp32_cam_exit"
        and event == "gate_closed"
        and (gate, sensor) == ("exit", "IR6")
    )
    if not (entry_boot or entry_pair or exit_pair or entry_passage or exit_passage):
        logger.warning("Ignoring unsupported gate event: %s", payload)
        return
    
    event_id = _record_gate_event(payload)
    if event_id is None:
        return
    if entry_boot:
        _record_gate_event_action(action="close", source="boot")
        return
    if entry_pair or exit_pair:
        _handle_gate_pair(gate, sensor, payload)
        return
    booking_code = payload.get("booking_code")
    command_id = payload.get("command_id")
    booking_code = booking_code.strip() if isinstance(booking_code, str) and booking_code.strip() else None
    command_id = command_id.strip() if isinstance(command_id, str) and command_id.strip() else None
    _record_gate_event_action(
        action="close",
        source=f"{gate}_{sensor.lower()}",
        sensor=sensor,
        sensor_device="esp32_cam_exit" if exit_passage else None,
    )
    _complete_passage_after_ir(gate, booking_code, command_id, event_id)

def _scan_page(
    title: str,
    message: str,
    status_code: int,
    *,
    booking_code: str | None = None,
    details: tuple[str, ...] = (),
    show_support: bool = False,
) -> HTMLResponse:
    safe_title = escape(title)
    safe_message = escape(message)
    safe_booking_code = escape(booking_code) if booking_code else ""
    detail_html = "".join(
        f'<p class="detail">{escape(detail)}</p>'
        for detail in details
    )
    booking_html = (
        f"""
        <div class="booking-code">
          <span>รหัสการจอง</span>
          <strong>{safe_booking_code}</strong>
        </div>
        """
        if booking_code
        else ""
    )

    support_html = ""
    if show_support:
        support_html = f"""
        <a class="support-button"
        href="{escape(SUPPORT_CONTACT_TARGET, quote=True)}">
        <span aria-hidden="true">&#9993;</span>
        ติดต่อผู้ดูแลพื้นที่
        </a>
        """

    if 200 <= status_code < 300:
        accent = "#0000CD"
        surface = "#F0F2FF"
        icon = "&#10003;"
    elif status_code == 425:
        accent = "#D97706"
        surface = "#FFF7E6"
        icon = "!"
    else:
        accent = "#CD2626"
        surface = "#FFF1F1"
        icon = "!"

    return HTMLResponse(
        content=f"""
        <!DOCTYPE html>
        <html lang="th">
          <head>
            <meta charset="utf-8">
            <meta name="viewport"
                  content="width=device-width, initial-scale=1">
            <title>{safe_title}</title>
            <style>
              * {{ box-sizing: border-box; }}

              body {{
                margin: 0;
                min-height: 100vh;
                display: grid;
                place-items: center;
                padding: 24px;
                font-family: Arial, "Noto Sans Thai", sans-serif;
                background: #F5F5F5;
                color: #202124;
              }}

              main {{
                width: min(100%, 460px);
                padding: 28px 24px;
                border: 1px solid #E5E7EB;
                border-radius: 20px;
                background: #FFFFFF;
                box-shadow: 0 16px 40px rgba(31, 41, 55, .12);
                text-align: center;
              }}

              .status-icon {{
                width: 58px;
                height: 58px;
                display: grid;
                place-items: center;
                margin: 0 auto 18px;
                border: 2px solid {accent};
                border-radius: 50%;
                background: {surface};
                color: {accent};
                font-size: 30px;
                font-weight: 700;
              }}

              h1 {{
                margin: 0;
                color: {accent};
                font-size: 24px;
                line-height: 1.35;
              }}

              .message {{
                margin: 18px 0 0;
                font-size: 17px;
                line-height: 1.65;
              }}

              .booking-code {{
                display: grid;
                gap: 6px;
                margin: 22px 0;
                padding: 16px;
                border-radius: 14px;
                background: #F0F2FF;
                color: #0000CD;
              }}

              .booking-code span {{
                font-size: 14px;
              }}

              .booking-code strong {{
                font-size: 22px;
                letter-spacing: .04em;
                overflow-wrap: anywhere;
              }}

              .detail {{
                margin: 12px 0 0;
                color: #4B5563;
                font-size: 15px;
                line-height: 1.65;
              }}

              .support-button {{
                width: 100%;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 10px;
                margin-top: 24px;
                padding: 14px 18px;
                border-radius: 12px;
                background: #0000CD;
                color: #FFFFFF;
                font-size: 16px;
                font-weight: 700;
                text-decoration: none;
              }}

              .support-button:focus-visible {{
                outline: 3px solid rgba(0, 0, 205, .3);
                outline-offset: 3px;
              }}

              .support-note {{
                margin: 20px 0 0;
                color: #6B7280;
                font-size: 14px;
                line-height: 1.6;
              }}
            </style>
          </head>
          <body>
            <main>
              <div class="status-icon"
                   aria-hidden="true">{icon}</div>
              <h1>{safe_title}</h1>
              {booking_html}
              <p class="message">{safe_message}</p>
              {detail_html}
              {support_html}
            </main>
          </body>
        </html>
        """,
        status_code=status_code,
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
    normalized_name = body.name.strip()
    normalized_username = body.username.strip()
    normalized_email = str(body.email).strip().lower()
    duplicate = db.query(User.id).filter(
        or_(
            func.lower(User.name) == normalized_name.lower(),
            func.lower(User.username) == normalized_username.lower(),
            func.lower(User.email) == normalized_email,
        )
    ).first()
    if duplicate:
        raise HTTPException(
            409,
            "ชื่อ ชื่อผู้ใช้ หรืออีเมลนี้มีอยู่ในระบบแล้ว",
        )
    if body.vehicles is None:
        vehicle_specs = _vehicle_specs_from_legacy(body.license_plate)
    else:
        vehicle_specs = _vehicle_specs_from_payload(body.vehicles)
    user = User(
        name=normalized_name,
        username=normalized_username,
        email=normalized_email,
        hashed_password=hash_password(body.password),
        license_plate=_legacy_plate_value(vehicle_specs),
        role="user",
        is_active=True,
    )
    try:
        db.add(user)
        db.flush()
        _replace_user_vehicles(db, user.id, vehicle_specs)
        db.commit()
        db.refresh(user)
        return user
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(409, "ข้อมูลบัญชีซ้ำกับบัญชีที่มีอยู่แล้ว") from error

def _lookup_login_user(
    db: Session,
    identifier: str,
    role: str | None = None,
) -> User | None:
    normalized = identifier.strip().lower()
    base_query = db.query(User)
    if role is not None:
        base_query = base_query.filter(User.role == role)

    for column in (User.email, User.username):
        user = base_query.filter(func.lower(column) == normalized).first()
        if user is not None:
            return user

    matches = (
        base_query
        .filter(func.lower(User.name) == normalized)
        .limit(2)
        .all()
    )
    return matches[0] if len(matches) == 1 else None

def _issue_access_token(db: Session, user: User) -> str:
    now = datetime.now(timezone.utc)
    user.last_login_at = now
    return create_token({"sub": str(user.id)}, db=db)

@app.post("/auth/login", response_model=Token)
def login(body: LoginForm, db: Session = Depends(get_db)):
    try:
        user = _lookup_login_user(db, body.identifier)

        if (
            not user
            or not user.is_active
            or user.deleted_at is not None
            or not verify_password(body.password, user.hashed_password)
        ):
            raise HTTPException(
                status_code=401,
                detail="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง หรือบัญชีไม่พร้อมใช้งาน",
            )
        token = _issue_access_token(db, user)
        db.commit()
        return {
            "access_token": token,
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

@app.post("/auth/logout", response_model=MessageOut)
def logout(
    credentials=Depends(bearer),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        payload = jwt.decode(
            credentials.credentials,
            SECRET_KEY,
            algorithms=[ALGORITHM],
        )
    except JWTError as error:
        raise HTTPException(401, "Invalid token") from error

    session = (
        db.query(AuthSession)
        .filter(
            AuthSession.token_id == payload.get("jti"),
            AuthSession.user_id == current_user.id,
            AuthSession.revoked_at.is_(None),
        )
        .with_for_update()
        .first()
    )
    if session is not None:
        now = datetime.now(timezone.utc)
        session.logged_out_at = now
        session.revoked_at = now
        current_user.last_logout_at = now
        db.commit()
    return {"message": "ออกจากระบบแล้ว"}

@app.get("/users/me", response_model=UserOut)
def get_me(current_user: User = Depends(get_current_user)):
    return current_user

@app.put("/users/me", response_model=UserOut)
def update_profile(
    body: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if body.username is not None and body.username != current_user.username:
        now = datetime.now(timezone.utc)
        if current_user.username_changed_at is not None:
            changed_at = _utc_datetime(current_user.username_changed_at)
            if now < changed_at + timedelta(days=7):
                eligible_at = changed_at + timedelta(days=7)
                raise HTTPException(
                    409,
                    detail=(
                        "เปลี่ยน username ได้ทุก 7 วัน; "
                        f"เปลี่ยนได้อีกครั้งหลัง {eligible_at.isoformat()}"
                    ),
                )
        duplicate_username = (
            db.query(User.id)
            .filter(
                User.id != current_user.id,
                func.lower(User.username) == body.username.strip().lower(),
            )
            .first()
        )
        if duplicate_username:
            raise HTTPException(409, "ชื่อผู้ใช้งานนี้มีผู้ใช้งานแล้ว")
        current_user.username = body.username.strip()
        current_user.username_changed_at = now
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
        current_user.hashed_password = hash_password(body.password)
    try:
        db.commit()
        db.refresh(current_user)
        return current_user
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(409, "ข้อมูลบัญชีซ้ำกับบัญชีที่มีอยู่แล้ว") from error

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
def create_booking(
    body: BookingCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    start_time = _utc_datetime(body.start_time)
    end_time = _utc_datetime(body.end_time)
    now = datetime.now(timezone.utc)
    bangkok_timezone = ZoneInfo("Asia/Bangkok")
    current_bangkok_minute = now.astimezone(
        bangkok_timezone
    ).replace(
        second=0,
        microsecond=0,
    )
    start_time_bangkok = start_time.astimezone(
        bangkok_timezone
    )
    end_time_bangkok = end_time.astimezone(
        bangkok_timezone
    )
    latest_booking_date = (
        current_bangkok_minute.date()
        + timedelta(days=MAX_BOOKING_ADVANCE_DAYS)
    )
    if end_time <= start_time:
        raise HTTPException(
            400,
            "end_time must be after start_time",
        )
    if start_time_bangkok < current_bangkok_minute:
        raise HTTPException(
            400,
            "รูปแบบเวลาไม่ถูกต้อง",
        )
    if (
        start_time_bangkok.date() > latest_booking_date
        or end_time_bangkok.date() > latest_booking_date
    ):
        raise HTTPException(
            400,
            (
                "จองล่วงหน้าได้ไม่เกิน "
                f"{MAX_BOOKING_ADVANCE_DAYS} วัน"
            ),
        )
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
    if body.payment_method == "card":
        saved_card_exists = (
            db.query(PaymentCard.id)
            .filter(
                PaymentCard.user_id == current_user.id
            )
            .first()
        )
        if saved_card_exists is None:
            raise HTTPException(
                409,
                "ไม่สามารถจองพื้นที่จอดรถได้ กรุณาเพิ่มข้อมูลบัตรเครดิต/เดบิต",
            )

    (
        db.query(User.id)
        .filter(User.id == current_user.id)
        .with_for_update()
        .first()
    )
    same_vehicle_filter = (
        (Booking.vehicle_id == selected_vehicle.id)
        | (
            Booking.vehicle_id.is_(None)
            & (
                func.lower(Booking.license_plate)
                == selected_license_plate.lower()
            )
        )
    )
    unpaid_vehicle_booking = (
        db.query(Booking)
        .outerjoin(
            Payment,
            Payment.booking_id == Booking.id,
        )
        .filter(
            Booking.user_id == current_user.id,
            Booking.status == "active",
            or_(
                Payment.status == "pending",
                Payment.id.is_(None),
            ),
            same_vehicle_filter,
        )
        .first()
    )
    if unpaid_vehicle_booking:
        raise HTTPException(
            status_code=409,
            detail=(
                f"รถทะเบียน {selected_license_plate} "
                "มีรายการจองที่ยังค้างชำระ "
                "จึงไม่สามารถจองซ้ำได้"
            ),
        )
    same_vehicle_booking = (
        db.query(Booking)
        .filter(
            Booking.user_id == current_user.id,
            Booking.status.in_(
                ("active", "ready_to_exit")
            ),
            Booking.start_time < end_time,
            Booking.end_time > start_time,
            same_vehicle_filter,
        )
        .first()
    )
    if same_vehicle_booking:
        raise HTTPException(
            status_code=409,
            detail=(
                f"รถทะเบียน {selected_license_plate} "
                "มีการจองค้างอยู่ในระบบแล้ว!"
            ),
        )
    slot = (
        db.query(Slot)
        .filter(Slot.id == body.slot_id)
        .with_for_update()
        .first()
    )

    if not slot:
        raise HTTPException(404, "Slot not found")

    if slot.status != SlotStatus.available:
        raise HTTPException(
            400,
            f"Slot {slot.name} is not available",
        )

    live_booking_for_slot = (
        db.query(Booking)
        .filter(
            Booking.slot_id == body.slot_id,
            Booking.status.in_(
                ("active", "ready_to_exit")
            ),
        )
        .first()
    )
    if live_booking_for_slot:
        raise HTTPException(
            409,
            "ช่องจอดรถนี้ถูกจองแล้ว กรุณาเลือกช่องจอดอื่น",
        )
    total_amount = calculate_amount(
        start_time,
        end_time,
    )
    _record_slot_status(
        db=db,
        slot=slot,
        status=SlotStatus.reserved,
        source="booking",
        now=now,
    )
    booking = Booking(
        user_id=current_user.id,
        slot_id=body.slot_id,
        vehicle_id=selected_vehicle.id,
        license_plate=selected_license_plate,
        vehicle_plate_number=(
            selected_vehicle.plate_number
        ),
        vehicle_province=selected_vehicle.province,
        start_time=start_time,
        end_time=end_time,
        total_amount=total_amount,
        booking_code=_generate_unique_booking_code(db),
    )
    try:
        db.add(booking)
        db.flush()
        db.add(
            Payment(
                booking_id=booking.id,
                amount=total_amount,
                base_amount=total_amount,
                overstay_fee=0,
                status="pending",
            )
        )
        _create_qr_token(
            db=db,
            booking=booking,
            qr_type="entry",
        )
        db.commit()
        db.refresh(booking)
    except Exception:
        db.rollback()
        raise
    _publish_slot_command(
        slot_name=slot.name,
        status="reserved",
        source="booking",
    )
    schedule_slot_broadcast(
        slot_name=slot.name,
        status="reserved",
    )
    schedule_booking_broadcast(
        user_id=booking.user_id,
        booking_id=booking.id,
        event="booking_created",
    )
    return booking

@app.get("/bookings/me", response_model=List[BookingOut])
def my_bookings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(Booking)
        .filter(Booking.user_id == current_user.id)
        .order_by(Booking.created_at.desc())
        .all()
    )

@app.delete("/bookings/{booking_id}")
def cancel_booking(
    booking_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    booking = (
        db.query(Booking)
        .filter(
            Booking.id == booking_id,
            Booking.user_id == current_user.id,
        )
        .with_for_update()
        .first()
    )
    if not booking:
        raise HTTPException(404, "Booking not found")

    if booking.status != "active":
        raise HTTPException(
            409,
            "รายการจองนี้ไม่สามารถยกเลิกได้",
        )
    if booking.entered_at is not None:
        raise HTTPException(409, "รถเข้าจอดแล้ว ไม่สามารถยกเลิกการจองได้")

    now = datetime.now(timezone.utc)
    slot = booking.slot
    slot_name = slot.name
    payment = (
        db.query(Payment)
        .filter(Payment.booking_id == booking.id)
        .first()
    )
    if payment:
        if payment.status == "paid":
            raise HTTPException(409, "รายการนี้ชำระเงินแล้ว ไม่สามารถยกเลิกได้")
        payment.status = "cancelled"
        payment.method = None

    _revoke_unused_qr_tokens(
        db=db,
        booking_id=booking.id,
        now=now,
    )
    booking.status = "cancelled"

    _record_slot_status(
        db=db,
        slot=slot,
        status=SlotStatus.available,
        source="booking_cancelled",
        now=now,
    )
    db.commit()

    _publish_slot_command(
        slot_name=slot_name,
        status="available",
        source="booking_cancelled",
    )
    schedule_slot_broadcast(
        slot_name=slot_name,
        status="available",
    )
    schedule_booking_broadcast(
        user_id=booking.user_id,
        booking_id=booking.id,
        event="booking_cancelled",
    )
    return {"message": f"Booking {booking_id} cancelled"}

# ==============================
# PAYMENTS ROUTES
# ==============================
@app.get("/payments/{booking_id}", response_model=PaymentOut)
def get_payment(booking_id: int,
                db: Session = Depends(get_db),
                current_user: User = Depends(get_current_user)):
    booking = _get_owned_booking(db, booking_id, current_user.id)
    payment = db.query(Payment).filter(Payment.booking_id == booking.id).first()
    if not payment:
        raise HTTPException(404, "Payment not found")
    return payment

@app.get("/payment-cards", response_model=List[PaymentCardOut])
def get_payment_cards(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(PaymentCard)
        .filter(PaymentCard.user_id == current_user.id)
        .order_by(PaymentCard.created_at.desc(), PaymentCard.id.desc())
        .all()
    )

@app.post(
    "/payment-cards",
    response_model=PaymentCardOut,
    status_code=201,
)
def create_payment_card(
    body: PaymentCardCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Serialize card creation so concurrent requests cannot exceed the limit.
    (
        db.query(User.id)
        .filter(User.id == current_user.id)
        .with_for_update()
        .first()
    )
    card_count = (
        db.query(func.count(PaymentCard.id))
        .filter(PaymentCard.user_id == current_user.id)
        .scalar()
        or 0
    )
    if card_count >= MAX_PAYMENT_CARDS_PER_USER:
        raise HTTPException(
            409,
            f"บันทึกบัตรได้ไม่เกิน {MAX_PAYMENT_CARDS_PER_USER} ใบ",
        )

    existing_card = (
        db.query(PaymentCard)
        .filter(
            PaymentCard.user_id == current_user.id,
            PaymentCard.card_number == body.card_number,
        )
        .first()
    )

    if existing_card:
        raise HTTPException(409, "บัตรใบนี้ถูกบันทึกแล้ว")
    card_number = body.card_number
    if card_number.startswith("4"):
        card_brand = "Visa"
    elif (
        card_number[:2] in {"51", "52", "53", "54", "55"}
        or 2221 <= int(card_number[:4]) <= 2720
    ):
        card_brand = "Mastercard"
    elif card_number[:2] in {"34", "37"}:
        card_brand = "American Express"
    elif card_number.startswith(("6011", "65")) or card_number[:3] in {
        "644", "645", "646", "647", "648", "649",
    }:
        card_brand = "Discover"
    elif card_number.startswith("35"):
        card_brand = "JCB"
    else:
        card_brand = "Other"

    card = PaymentCard(
        user_id=current_user.id,
        card_number=body.card_number,
        expiry_date=body.expiry_date,
        cvv=body.cvv,
        holder_name=body.holder_name,
        card_brand=card_brand,
    )
    try:
        db.add(card)
        db.commit()
        db.refresh(card)
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(409, "บัตรใบนี้ถูกบันทึกแล้ว") from error
    return card

# ---- ยืนยันชำระเงิน ----
@app.post("/payments/confirm", response_model=PaymentOut)
async def confirm_payment(
    body: PaymentCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    booking = _get_owned_booking(
        db=db,
        booking_id=body.booking_id,
        user_id=current_user.id,
    )
    booking = (
        db.query(Booking)
        .filter(Booking.id == booking.id)
        .with_for_update()
        .first()
    )
    if booking.status != "active":
        raise HTTPException(409, "Booking is not waiting for payment")
    if booking.entered_at is None:
        raise HTTPException(409, "ต้องมีการยืนยันรถเข้าจอดก่อนชำระเงิน")
    payment = (
        db.query(Payment)
        .filter(Payment.booking_id == booking.id)
        .with_for_update()
        .first()
    )
    if not payment:
        raise HTTPException(404, "Payment not found")
    if payment.status != "pending":
        raise HTTPException(409, "Payment is not waiting for confirmation")
    method = body.method.strip()
    selected_card = None
    if method == "promptpay":
        raise HTTPException(
            409,
            "PromptPay ต้องยืนยันจากการสแกน QR Code",
        )
    if method == "card":
        if body.payment_card_id is None:
            raise HTTPException(422, "กรุณาเลือกบัตรที่ต้องการชำระเงิน")
        selected_card = (
            db.query(PaymentCard)
            .filter(
                PaymentCard.id == body.payment_card_id,
                PaymentCard.user_id == current_user.id,
            )
            .first()
        )
        if not selected_card:
            raise HTTPException(404, "ไม่พบบัตรที่เลือก")
    elif body.payment_card_id is not None:
        raise HTTPException(
            422,
            "payment_card_id ใช้ได้เฉพาะเมื่อ method เป็น card",
        )
    elif method.startswith("card:"):
        raise HTTPException(
            422,
            "ระบบกำหนดรหัสหลังบัตรตรวจสอบความเป็นเจ้าของเท่านั้น",
        )
    _complete_payment(
        db=db,
        booking=booking,
        payment=payment,
        method=method,
        now=datetime.now(timezone.utc),
        card=selected_card,
    )
    db.commit()
    db.refresh(payment)
    await broadcast_booking_update(
        user_id=booking.user_id,
        booking_id=booking.id,
        event="payment_paid",
    )
    return payment

@app.get(
    "/bookings/{booking_id}/qr/entry",
    response_model=BookingQrTokenOut,
)
async def get_entry_qr(
    booking_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    booking = _get_owned_booking(db, booking_id, current_user.id)
    booking = (
        db.query(Booking)
        .filter(Booking.id == booking.id)
        .with_for_update()
        .first()
    )
    now = datetime.now(timezone.utc)

    if booking.status == "time_out":
        raise HTTPException(
            410,
            "รายการจองถูกยกเลิก ไม่พบการเข้าใช้งานพื้นที่จอดรถภายในระยะเวลาที่กำหนด",
        )

    if booking.status != "active":
        raise HTTPException(
            409,
            "Booking is not eligible for an Entry QR",
        )

    if booking.entered_at is not None:
        raise HTTPException(
            409,
            "ตรวจพบรถในพื้นที่จอดแล้ว QR ทางเข้าไม่สามารถใช้งานได้อีก",
        )

    if _booking_is_past_no_show_deadline(booking, now):
        slot_name = _mark_booking_time_out(
            db,
            booking,
            now,
        )
        db.commit()

        await _publish_slot_state(
            slot_name,
            "available",
            source="booking_time_out",
        )
        await broadcast_booking_update(
            user_id=booking.user_id,
            booking_id=booking.id,
            event="booking_time_out",
        )
        raise HTTPException(
            410,
            "เกินเวลาเข้าพื้นที่จอดรถ 30 นาทีแล้ว QR ทางเข้าถูกยกเลิก",
        )
    available_from = _entry_qr_available_from(booking)

    if now < available_from:
        raise HTTPException(
            status_code=425,
            detail=(
                "QR ทางเข้า จะพร้อมใช้งานตั้งแต่ "
                f"{_format_bangkok_datetime(available_from)} ถึง "
                f"{_format_bangkok_datetime(_utc_datetime(booking.start_time) + ENTRY_QR_VALIDITY)}"
            ),
        )
    entry_qr = _active_qr_token(
        db,
        booking.id,
        "entry",
    )
    if entry_qr:
        return _qr_payload(booking, entry_qr)
    latest_entry_qr = (
        db.query(BookingQrToken)
        .filter(
            BookingQrToken.booking_id == booking.id,
            BookingQrToken.qr_type == "entry",
        )
        .order_by(BookingQrToken.issued_at.desc())
        .first()
    )
    if (
        latest_entry_qr
        and latest_entry_qr.expires_at
        and latest_entry_qr.expires_at <= now
    ):
        slot_name = _mark_booking_time_out(
            db,
            booking,
            now,
        )
        db.commit()

        await _publish_slot_state(
            slot_name,
            "available",
            source="booking_time_out",
        )
        await broadcast_booking_update(
            user_id=booking.user_id,
            booking_id=booking.id,
            event="booking_time_out",
        )
        raise HTTPException(
            410,
            "QR Code หมดอายุ",
        )
    raise HTTPException(
        404,
        "Entry QR not available",
    )

@app.post(
    "/bookings/{booking_id}/qr/payment",
    response_model=BookingQrTokenOut,
)
def create_payment_qr(
    booking_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    booking = _get_owned_booking(db, booking_id, current_user.id)
    booking = (
        db.query(Booking)
        .filter(Booking.id == booking.id)
        .with_for_update()
        .first()
    )

    if booking.status != "active":
        raise HTTPException(409, "Booking is not waiting for payment")
    if booking.entered_at is None:
        raise HTTPException(409, "ต้องมีการยืนยันรถเข้าจอดก่อนชำระเงิน")

    payment = (
        db.query(Payment)
        .filter(Payment.booking_id == booking.id)
        .first()
    )
    if not payment or payment.status != "pending":
        raise HTTPException(409, "Payment is not waiting for confirmation")
    active_qr = _active_qr_token(db, booking.id, "payment")
    if active_qr:
        return _qr_payload(booking, active_qr)
    latest_qr = (
        db.query(BookingQrToken)
        .filter(
            BookingQrToken.booking_id == booking.id,
            BookingQrToken.qr_type == "payment",
            BookingQrToken.revoked_at.is_(None),
            BookingQrToken.used_at.is_(None),
        )
        .order_by(BookingQrToken.issued_at.desc())
        .first()
    )
    if latest_qr:
        raise HTTPException(
            410,
            "PromptPay QR หมดอายุ กรุณาลองใหม่อีกครั้ง",
        )

    payment_qr = _create_qr_token(
        db=db,
        booking=booking,
        qr_type="payment",
    )
    db.commit()
    db.refresh(payment_qr)

    return _qr_payload(booking, payment_qr)

@app.post(
    "/bookings/{booking_id}/qr/payment/reissue",
    response_model=BookingQrTokenOut,
)
def reissue_payment_qr(
    booking_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    booking = _get_owned_booking(db, booking_id, current_user.id)
    booking = (
        db.query(Booking)
        .filter(Booking.id == booking.id)
        .with_for_update()
        .first()
    )
    if booking.status != "active":
        raise HTTPException(409, "Booking is not waiting for payment")
    if booking.entered_at is None:
        raise HTTPException(409, "ต้องมีการยืนยันรถเข้าจอดก่อนชำระเงิน")
    payment = (
        db.query(Payment)
        .filter(Payment.booking_id == booking.id)
        .first()
    )
    if not payment or payment.status != "pending":
        raise HTTPException(409, "Payment is not waiting for confirmation")
    latest_payment_qr = (
        db.query(BookingQrToken)
        .filter(
            BookingQrToken.booking_id == booking.id,
            BookingQrToken.qr_type == "payment",
        )
        .order_by(BookingQrToken.issued_at.desc())
        .first()
    )
    if latest_payment_qr is None:
        raise HTTPException(
            409,
            "ไม่พบ PromptPay QR ที่รอดำเนินการซ้ำ",
        )
    active_payment_qr = _active_qr_token(
        db=db,
        booking_id=booking.id,
        qr_type="payment",
    )
    if active_payment_qr:
        raise HTTPException(
            409,
            "PromptPay QR ยังไม่หมดอายุ",
        )
    now = datetime.now(timezone.utc)

    _revoke_unused_qr_tokens(
        db=db,
        booking_id=booking.id,
        now=now,
        qr_type="payment",
    )
    payment_qr = _create_qr_token(
        db=db,
        booking=booking,
        qr_type="payment",
    )
    db.commit()
    db.refresh(payment_qr)

    return _qr_payload(booking, payment_qr)

@app.get(
    "/bookings/{booking_id}/qr/exit",
    response_model=BookingQrTokenOut,
)
def get_exit_qr(
    booking_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    booking = _get_owned_booking(db, booking_id, current_user.id)

    payment = (
        db.query(Payment)
        .filter(Payment.booking_id == booking.id)
        .first()
    )
    if booking.status != "ready_to_exit" or not payment or payment.status != "paid":
        raise HTTPException(409, "Exit QR is available after payment only")
    exit_qr = _active_qr_token(db, booking.id, "exit")

    if not exit_qr:
        raise HTTPException(404, "Exit QR not found")
    return _qr_payload(booking, exit_qr)

@app.get("/scan/{token}", response_class=HTMLResponse)
async def scan_qr_token(
    token: str,
    db: Session = Depends(get_db),
):
    initial_qr_token = (
        db.query(BookingQrToken)
        .filter(BookingQrToken.token == token)
        .first()
    )
    if not initial_qr_token:
        return _scan_page(
            "ไม่พบ QR Code",
            "QR Code นี้ไม่มีอยู่ในระบบ",
            404,
        )
    booking = (
        db.query(Booking)
        .filter(Booking.id == initial_qr_token.booking_id)
        .with_for_update()
        .first()
    )
    qr_token = (
        db.query(BookingQrToken)
        .filter(BookingQrToken.id == initial_qr_token.id)
        .with_for_update()
        .first()
    )
    if qr_token is None:
        return _scan_page(
            "QR Code ถูกยกเลิก",
            "QR Code หมดอายุ",
            410,
        )
    now = datetime.now(timezone.utc)

    if booking is None:
        return _scan_page(
            "ไม่พบรายการจอง",
            "รายการจองนี้ถูกยกเลิก",
            404,
        )
    entry_window_elapsed = (
        qr_token.qr_type == "entry"
        and (
            booking.status == "time_out"
            or _booking_is_past_no_show_deadline(
                booking,
                now,
            )
        )
    )
    if entry_window_elapsed:
        if booking.status == "active":
            slot_name = _mark_booking_time_out(
                db,
                booking,
                now,
            )
            db.commit()

            await _publish_slot_state(
                slot_name,
                "available",
                source="booking_time_out",
            )
            await broadcast_booking_update(
                user_id=booking.user_id,
                booking_id=booking.id,
                event="booking_time_out",
            )
        return _scan_page(
            "รายการจองถูกยกเลิก",
            "ไม่พบการเข้าใช้งานพื้นที่จอดรถภายในระยะเวลาที่กำหนด",
            410,
            booking_code=booking.booking_code,
            details=(
                "หากต้องการเข้าใช้บริการพื้นที่จอดรถ กรุณาทำรายการจองใหม่",
            ),
        )
    if qr_token.revoked_at is not None:
        return _scan_page(
            "QR Code ถูกยกเลิก",
            "ไม่พบสิทธิ์การเข้าถึงพื้นที่จอด",
            410,
        )
    if qr_token.used_at is not None:
        return _scan_page(
            "QR Code ถูกใช้งานแล้ว",
            "QR Code ถูกจำกัดสิทธิ์การใช้งานได้เพียงครั้งเดียว",
            410,
        )
    if (
        qr_token.qr_type == "entry"
        and now < _entry_qr_available_from(booking)
    ):
        available_from = _entry_qr_available_from(booking)

        return _scan_page(
            "ขออภัยในความไม่สะดวก ขณะนี้ยังไม่สามารถเข้าใช้งานพื้นที่จอดรถได้",
            "กรุณาเข้าใช้บริการพื้นที่จอดรถในช่วงระยะที่ได้ทำการจองไว้เท่านั้น",
            425,
            booking_code=booking.booking_code,
            details=(
                "QR ทางเข้า จะพร้อมใช้งานตั้งแต่ "
                f"{_format_bangkok_datetime(available_from)} ถึง "
                f"{_format_bangkok_datetime(_utc_datetime(booking.start_time) + ENTRY_QR_VALIDITY)}",
            ),
        )
    if (
        qr_token.expires_at
        and qr_token.expires_at <= now
    ):
        return _scan_page(
            "QR Code หมดอายุ",
            "กรุณาทำรายการใหม่อีกครั้ง",
            410,
        )
    if qr_token.qr_type == "entry":
        if (
            booking.status != "active"
            or booking.entered_at is not None
        ):
            return _scan_page(
                "ไม่พบสิทธิ์การเข้าถึงพื้นที่จอด",
                "QR ถูกใช้งานแล้ว",
                409,
            )
        outcome, open_command = _queue_normal_gate_open_after_verified_qr(
            db=db,
            booking=booking,
            qr_token=qr_token,
            now=now,
        )
        if outcome == "waiting_for_vehicle":
            return _scan_page(
                "เกิดข้อผิดพลาดในการเปิดไม้กั้น ระบบไม่พบรถยนต์",
                "กรุณานำรถเข้าจอดบริเวณจุดตรวจทางเข้า แล้วสแกน QR Code อีกครั้ง",
                409,
                booking_code=booking.booking_code,
                details=(
                    "หากพบปัญหากรุณาติดต่อผู้ดูแลพื้นที่จอดรถ ขออภัยในความไม่สะดวก",
                ),
                show_support=True,
            )
        if outcome == "admin_override":
            return _scan_page(
                "พบปัญหาในการเข้าใช้บริการพื้นที่จอดรถใช่ไหม?",
                "กรุณาติดต่อผู้ดูแลพื้นที่จอดรถเพื่อขอความช่วยเหลือ ขออภัยในความไม่สะดวก",
                409,
                booking_code=booking.booking_code,
                show_support=True,
            )
        if open_command is None:
            return _scan_page(
                "ไม่สามารถเปิดไม้กั้นได้",
                "กรุณาติดต่อผู้ดูแลพื้นที่จอดรถเพื่อขอความช่วยเหลือ ขออภัยในความไม่สะดวก",
                500,
                booking_code=booking.booking_code,
                show_support=True,
            )
        qr_token.scanned_at = now
        qr_token.used_at = now

        db.commit()

        _publish_servo_command(
            open_command,
            booking,
        )
        return _scan_page(
            "ยินดีต้อนรับสู่พื้นที่จอดรถ!",
            "กรุณานำรถเข้าจอดบริเวณจุดตรวจทางเข้า",
            202,
            booking_code=booking.booking_code,
            details=(
                "กำลังเปิดไม้กั้น กรุณารอสักครู่...",
                "หากพบปัญหากรุณาติดต่อผู้ดูแลพื้นที่จอดรถ ขออภัยในความไม่สะดวก",
            ),
            show_support=True,
        )
    if qr_token.qr_type == "payment":
        if (
            booking.status != "active"
            or booking.entered_at is None
        ):
            return _scan_page(
                "ไม่พบรายการชำระเงิน",
                "รายการจองนี้ถูกยกเลิกแล้ว",
                409,
            )
        payment = (
            db.query(Payment)
            .filter(Payment.booking_id == booking.id)
            .with_for_update()
            .first()
        )
        if (
            not payment
            or payment.status != "pending"
        ):
            return _scan_page(
                "ไม่พบรายการชำระเงิน กรุณาลองใหม่อีกครั้ง",
                "รายการจองนี้ถูกยกเลิกแล้ว",
                409,
            )
        qr_token.scanned_at = now
        qr_token.used_at = now

        _complete_payment(
            db=db,
            booking=booking,
            payment=payment,
            method="promptpay",
            now=now,
        )
        db.commit()
        await broadcast_booking_update(
            user_id=booking.user_id,
            booking_id=booking.id,
            event="payment_paid",
        )
        return _scan_page(
            "ชำระเงินสำเร็จ!",
            "QR ทางออก อยู่ท้ายใบเสร็จ",
            200,
        )
    if qr_token.qr_type == "exit":
        payment = (
            db.query(Payment)
            .filter(Payment.booking_id == booking.id)
            .with_for_update()
            .first()
        )
        if (
            booking.status != "ready_to_exit"
            or not payment
            or payment.status != "paid"
        ):
            return _scan_page(
                "เปิดไม้กั้นไม่สำเร็จ",
                "กรุณาติดต่อผู้ดูแลพื้นที่จอดรถ ขออภัยในความไม่สะดวก",
                409,
            )
        outcome, open_command = _queue_normal_gate_open_after_verified_qr(
            db=db,
            booking=booking,
            qr_token=qr_token,
            now=now,
        )
        if outcome == "waiting_for_vehicle":
            return _scan_page(
                "เกิดข้อผิดพลาดในการเปิดไม้กั้น",
                "กรุณานำรถเข้าจอดบริเวณจุดตรวจทางออก แล้วสแกน QR Code อีกครั้ง",
                409,
                booking_code=booking.booking_code,
                details=(
                    "หากพบปัญหากรุณาติดต่อผู้ดูแลพื้นที่จอดรถ ขออภัยในความไม่สะดวก",
                ),
                show_support=True,
            )
        if outcome == "admin_override":
            return _scan_page(
                "พบปัญหาในการเข้าใช้บริการพื้นที่จอดรถใช่ไหม?",
                "กรุณาติดต่อผู้ดูแลพื้นที่จอดรถเพื่อขอความช่วยเหลือ ขออภัยในความไม่สะดวก",
                409,
                booking_code=booking.booking_code,
                show_support=True,
            )
        if open_command is None:
            return _scan_page(
                "ไม่สามารถเปิดไม้กั้นได้",
                "กรุณาติดต่อผู้ดูแลพื้นที่จอดรถเพื่อขอความช่วยเหลือ ขออภัยในความไม่สะดวก",
                500,
                booking_code=booking.booking_code,
                show_support=True,
            )
        qr_token.scanned_at = now
        qr_token.used_at = now

        db.commit()

        _publish_servo_command(
            open_command,
            booking,
        )
        return _scan_page(
            "ขอบคุณที่ใช้บริการพื้นที่จอดรถ Coolkids!",
            "กรุณานำรถไปยังจุดตรวจทางออก",
            202,
            booking_code=booking.booking_code,
            details=(
                "กำลังเปิดไม้กั้น กรุณารอสักครู่...",
                "หากพบปัญหากรุณาติดต่อผู้ดูแลพื้นที่จอดรถ ขออภัยในความไม่สะดวก",
            ),
            show_support=True,
        )

# ==============================
# WEBSOCKET
# ==============================
@app.websocket("/ws/slots")
async def ws_slots(websocket: WebSocket):
    await websocket.accept()

    async with slot_clients_lock:
        connected_clients.append(websocket)

    db = SessionLocal()
    try:
        slots = db.query(Slot).all()
        await websocket.send_json(
            [
                {
                    "slot": slot.name,
                    "status": slot.status.value,
                }
                for slot in slots
            ]
        )
    finally:
        db.close()

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with slot_clients_lock:
            if websocket in connected_clients:
                connected_clients.remove(websocket)

@app.websocket("/ws/bookings")
async def ws_bookings(websocket: WebSocket):
    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=1008)
        return

    try:
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
        )
        user_id = int(payload.get("sub"))
        token_id = payload.get("jti")
    except (JWTError, TypeError, ValueError):
        await websocket.close(code=1008)
        return

    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        active_session = (
            db.query(AuthSession.token_id)
            .filter(
                AuthSession.token_id == token_id,
                AuthSession.user_id == user_id,
                AuthSession.revoked_at.is_(None),
                AuthSession.logged_out_at.is_(None),
                AuthSession.expires_at > now,
                User.id == user_id,
                User.is_active.is_(True),
                User.deleted_at.is_(None),
            )
            .join(User, User.id == AuthSession.user_id)
            .first()
        )
    finally:
        db.close()

    if not active_session:
        await websocket.close(code=1008)
        return

    await websocket.accept()

    async with booking_clients_lock:
        booking_clients.setdefault(user_id, []).append(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with booking_clients_lock:
            clients = booking_clients.get(user_id, [])

            if websocket in clients:
                clients.remove(websocket)

            if not clients:
                booking_clients.pop(user_id, None)

# ==============================
# ADMIN ROUTES
# ==============================
@app.post("/admin/login")
def admin_login(body: AdminLoginReq, db: Session = Depends(get_db)):
    user = _lookup_login_user(db, body.identifier, role="admin")
    if (
        not user
        or not user.is_active
        or user.deleted_at is not None
        or not verify_password(body.password, user.hashed_password)
    ):
        raise HTTPException(401, "ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")

    token = _issue_access_token(db, user)
    db.commit()
    return {
        "role": "admin",
        "access_token": token,
        "token_type": "bearer",
    }

@app.get("/admin/list", response_model=List[AdminListOut])
def get_admin_list(current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    now = datetime.now(timezone.utc)
    admins = (
        db.query(User)
        .filter(
            User.role == "admin",
            User.is_active.is_(True),
            User.deleted_at.is_(None),
        )
        .order_by(User.created_at.asc(), User.id.asc())
        .all()
    )
    active_session_user_ids = {
        row[0]
        for row in (
            db.query(AuthSession.user_id)
            .filter(
                AuthSession.user_id.in_([admin.id for admin in admins] or [-1]),
                AuthSession.revoked_at.is_(None),
                AuthSession.logged_out_at.is_(None),
                AuthSession.expires_at > now,
            )
            .distinct()
            .all()
        )
    }
    return [
        {
            "id": admin.id,
            "name": admin.name,
            "username": admin.username,
            "email": admin.email,
            "created_at": admin.created_at,
            "last_login_at": admin.last_login_at,
            "last_logout_at": admin.last_logout_at,
            "is_online": admin.id in active_session_user_ids,
        }
        for admin in admins
    ]

@app.post("/admin/add", response_model=MessageOut, status_code=201)
def add_admin(body: AdminAddReq, current_admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    name = f"{body.first_name.strip()} {body.last_name.strip()}"
    username = body.username.strip()
    email = str(body.email).strip().lower()
    existing = db.query(User.id).filter(
        or_(
            func.lower(User.name) == name.lower(),
            func.lower(User.username) == username.lower(),
            func.lower(User.email) == email,
        )
    ).first()
    if existing:
        raise HTTPException(409, "ชื่อ ชื่อผู้ใช้ หรืออีเมลนี้มีอยู่ในระบบแล้ว")

    new_admin = User(
        name=name,
        username=username,
        email=email,
        hashed_password=hash_password(body.password),
        role="admin",
        is_active=True,
    )
    try:
        db.add(new_admin)
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(409, "ข้อมูล Admin ซ้ำกับบัญชีที่มีอยู่แล้ว") from error
    return {"message": "เพิ่มบัญชี Admin แล้ว"}

@app.delete("/admin/{admin_id}", response_model=AdminDeactivateOut)
def deactivate_admin(
    admin_id: int,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    admin = (
        db.query(User)
        .filter(User.id == admin_id, User.role == "admin")
        .with_for_update()
        .first()
    )
    if admin is None or admin.deleted_at is not None:
        raise HTTPException(404, "ไม่พบ Admin ที่ใช้งานอยู่")

    now = datetime.now(timezone.utc)
    admin.is_active = False
    admin.deleted_at = now
    db.query(AuthSession).filter(
        AuthSession.user_id == admin.id,
        AuthSession.revoked_at.is_(None),
    ).update(
        {
            AuthSession.revoked_at: now,
            AuthSession.logged_out_at: now,
        },
        synchronize_session=False,
    )
    db.commit()
    return {
        "admin_id": admin.id,
        "status": "disabled",
        "disabled_at": now,
    }

def _analytics_period_window(
    now: datetime,
    period: AnalyticsPeriod,
    anchor_date: date | None,
) -> tuple[datetime, datetime]:
    bangkok = ZoneInfo("Asia/Bangkok")
    local_now = now.astimezone(bangkok)
    selected_date = anchor_date or local_now.date()

    if selected_date > local_now.date():
        raise HTTPException(
            422,
            "รูปแบบวันที่ไม่ถูกต้อง",
        )
    if period == "daily":
        start_day = selected_date
        end_day = selected_date + timedelta(days=1)
    elif period == "weekly":
        start_day = selected_date - timedelta(days=selected_date.weekday())
        end_day = start_day + timedelta(days=7)
    else:
        start_day = selected_date.replace(day=1)
        if start_day.month == 12:
            end_day = date(start_day.year + 1, 1, 1)
        else:
            end_day = date(start_day.year, start_day.month + 1, 1)

    period_start = datetime.combine(
        start_day,
        datetime.min.time(),
        tzinfo=bangkok,
    ).astimezone(timezone.utc)

    full_period_end = datetime.combine(
        end_day,
        datetime.min.time(),
        tzinfo=bangkok,
    ).astimezone(timezone.utc)

    return period_start, min(full_period_end, now)

def _chart_label_and_key(value: datetime, period: AnalyticsPeriod) -> tuple[str, str]:
    value = _utc_datetime(value).astimezone(ZoneInfo("Asia/Bangkok"))
    if period == "daily":
        return value.strftime("%H:00"), value.strftime("%Y-%m-%d-%H")
    return value.strftime("%d %b"), value.strftime("%Y-%m-%d")

class AnalyticsDetailOut(AnalyticsOut):
    usage_chart: List[ChartData]
    cars_in_chart: List[ChartData]
    cars_out_chart: List[ChartData]

def _occupied_seconds(
    intervals: list[tuple[datetime, datetime]],
    window_start: datetime,
    window_end: datetime,
) -> float:
    clipped = sorted(
        (max(start, window_start), min(end, window_end))
        for start, end in intervals
        if start < window_end and end > window_start
    )
    if not clipped:
        return 0.0
    total = 0.0
    current_start, current_end = clipped[0]
    for start, end in clipped[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            total += (current_end - current_start).total_seconds()
            current_start, current_end = start, end
    return total + (current_end - current_start).total_seconds()

def _analytics_buckets(
    period_start: datetime,
    period_end: datetime,
    period: AnalyticsPeriod,
) -> list[tuple[str, datetime, datetime]]:
    bangkok = ZoneInfo("Asia/Bangkok")
    local_cursor = period_start.astimezone(bangkok)
    local_end = period_end.astimezone(bangkok)
    step = timedelta(hours=1) if period == "daily" else timedelta(days=1)
    buckets: list[tuple[str, datetime, datetime]] = []

    while local_cursor < local_end:
        local_bucket_end = min(local_cursor + step, local_end)

        label = (
            local_cursor.strftime("%H:00")
            if period == "daily"
            else local_cursor.strftime("%d %b")
        )
        buckets.append(
            (
                label,
                local_cursor.astimezone(timezone.utc),
                local_bucket_end.astimezone(timezone.utc),
            )
        )
        local_cursor = local_bucket_end
    return buckets

@app.get("/admin/analytics", response_model=AnalyticsDetailOut)
def get_analytics(
    period: AnalyticsPeriod = Query(default="daily"),
    anchor_date: date | None = Query(default=None),
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    period_start, period_end = _analytics_period_window(
        now=now,
        period=period,
        anchor_date=anchor_date,
    )
    payments = (
        db.query(Payment)
        .filter(
            Payment.status == "paid",
            Payment.paid_at.is_not(None),
            Payment.paid_at >= period_start,
            Payment.paid_at < period_end,
        )
        .order_by(Payment.paid_at.asc())
        .all()
    )
    slot_count = db.query(func.count(Slot.id)).scalar() or 0

    bookings = (
        db.query(Booking)
        .filter(
            Booking.entered_at.is_not(None),
            Booking.entered_at < period_end,
            or_(
                Booking.exited_at.is_(None),
                Booking.exited_at > period_start,
            ),
        )
        .all()
    )
    intervals_by_slot: dict[int, list[tuple[datetime, datetime]]] = {}
    entry_times: list[datetime] = []
    exit_times: list[datetime] = []

    for booking in bookings:
        entered = _utc_datetime(booking.entered_at)
        if period_start <= entered < period_end:
            entry_times.append(entered)
        exited = (
            _utc_datetime(booking.exited_at)
            if booking.exited_at is not None
            else period_end
        )
        if exited > entered:
            intervals_by_slot.setdefault(booking.slot_id, []).append(
                (entered, exited)
            )
        if booking.exited_at is not None:
            actual_exit = _utc_datetime(booking.exited_at)
            if period_start <= actual_exit < period_end:
                exit_times.append(actual_exit)
    occupied_seconds = sum(
        _occupied_seconds(
            intervals,
            period_start,
            period_end,
        )
        for intervals in intervals_by_slot.values()
    )
    capacity_seconds = (
        slot_count * (period_end - period_start).total_seconds()
    )
    usage_percent = (
        round(occupied_seconds / capacity_seconds * 100, 1)
        if capacity_seconds > 0
        else None
    )
    income_chart: list[ChartData] = []
    usage_chart: list[ChartData] = []
    cars_in_chart: list[ChartData] = []
    cars_out_chart: list[ChartData] = []

    for label, bucket_start, bucket_end in _analytics_buckets(
        period_start,
        period_end,
        period,
    ):
        bucket_occupied = sum(
            _occupied_seconds(
                intervals,
                bucket_start,
                bucket_end,
            )
            for intervals in intervals_by_slot.values()
        )
        bucket_capacity = (
            slot_count * (bucket_end - bucket_start).total_seconds()
        )
        income_chart.append(
            ChartData(
                label=label,
                value=float(
                    sum(
                        payment.amount
                        for payment in payments
                        if payment.paid_at is not None
                        and bucket_start
                        <= _utc_datetime(payment.paid_at)
                        < bucket_end
                    )
                ),
            )
        )
        usage_chart.append(
            ChartData(
                label=label,
                value=(
                    round(
                        bucket_occupied / bucket_capacity * 100,
                        1,
                    )
                    if bucket_capacity > 0
                    else 0.0
                ),
            )
        )
        cars_in_chart.append(
            ChartData(
                label=label,
                value=float(
                    sum(
                        bucket_start <= item < bucket_end
                        for item in entry_times
                    )
                ),
            )
        )
        cars_out_chart.append(
            ChartData(
                label=label,
                value=float(
                    sum(
                        bucket_start <= item < bucket_end
                        for item in exit_times
                    )
                ),
            )
        )
    return AnalyticsDetailOut(
        period=period,
        income_total=sum(payment.amount for payment in payments),
        usage_percent=usage_percent,
        cars_in=len(entry_times),
        cars_out=len(exit_times),
        chart=income_chart,
        usage_chart=usage_chart,
        cars_in_chart=cars_in_chart,
        cars_out_chart=cars_out_chart,
    )

@app.get(
    "/admin/analytics/hardware-errors",
    response_model=HardwareErrorAnalyticsOut,
)
def get_hardware_error_analytics(
    period: AnalyticsPeriod = Query(default="daily"),
    anchor_date: date | None = Query(default=None),
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    period_start, period_end = _analytics_period_window(
        now=now,
        period=period,
        anchor_date=anchor_date,
    )
    error_statuses = (
        "failed",
        "no_response",
        "late_failed",
        "offline",
        "degraded",
        "admin_override",
        "admin_conflict",
    )
    logs = (
        db.query(HardwareLog)
        .filter(
            HardwareLog.created_at >= period_start,
            HardwareLog.created_at < period_end,
            HardwareLog.status.in_(error_statuses),
        )
        .order_by(HardwareLog.created_at.desc())
        .all()
    )
    counts: dict[str, int] = {}

    for log in logs:
        counts[log.device_name] = counts.get(log.device_name, 0) + 1

    chart = [
        ChartData(
            label=label,
            value=float(
                sum(
                    1
                    for log in logs
                    if log.created_at is not None
                    and bucket_start
                    <= _utc_datetime(log.created_at)
                    < bucket_end
                )
            ),
        )
        for label, bucket_start, bucket_end in _analytics_buckets(
            period_start,
            period_end,
            period,
        )
    ]
    return HardwareErrorAnalyticsOut(
        period=period,
        total_errors=len(logs),
        by_device=[
            DeviceErrorCount(
                device_name=device_name,
                count=count,
            )
            for device_name, count in sorted(counts.items())
        ],
        history=logs,
        chart=chart,
    )

@app.get("/admin/bookings", response_model=List[AdminBookingHistoryOut])
def get_all_bookings_admin(
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    bookings = (
        db.query(Booking)
        .order_by(Booking.created_at.desc())
        .all()
    )
    vehicle_ids = {booking.vehicle_id for booking in bookings if booking.vehicle_id}
    vehicles = {
        vehicle.id: vehicle
        for vehicle in db.query(Vehicle).filter(Vehicle.id.in_(vehicle_ids or [-1])).all()
    }
    rows = []
    for booking in bookings:
        vehicle = vehicles.get(booking.vehicle_id)
        payment = booking.payment
        payment_method = payment.method if payment else None
        if payment and payment.method == "card" and payment.card_brand:
            payment_method = (
                f"{payment.card_brand} •••• {payment.card_last4 or ''}".strip()
            )
        rows.append(
            AdminBookingHistoryOut(
                id=booking.id,
                slot_name=booking.slot.name,
                user_name=booking.user.name,
                username=booking.user.username,
                license_plate=booking.license_plate,
                vehicle_plate_number=(
                    booking.vehicle_plate_number
                    or (vehicle.plate_number if vehicle else None)
                ),
                vehicle_province=(
                    booking.vehicle_province
                    or (vehicle.province if vehicle else None)
                ),
                booking_code=booking.booking_code,
                booking_status=booking.status,
                payment_status=payment.status if payment else None,
                payment_method=payment_method,
                amount=payment.amount if payment else None,
                booking_created_at=booking.created_at,
                start_time=booking.start_time,
                end_time=booking.end_time,
                entry_time=booking.entered_at,
                exited_at=booking.exited_at,
                payment_paid_at=payment.paid_at if payment else None,
            )
        )
    return rows

@app.get("/admin/devices", response_model=List[DeviceStatusOut])
def get_device_statuses(
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    reported = {
        device.device_name: device
        for device in db.query(DeviceStatus)
        .filter(DeviceStatus.device_name.in_(KNOWN_HARDWARE_DEVICES))
        .all()
    }
    return [
        DeviceStatusOut.model_validate(reported[name])
        if name in reported
        else DeviceStatusOut(device_name=name, status="unknown")
        for name in sorted(KNOWN_HARDWARE_DEVICES)
    ]

@app.get("/admin/hardware-logs", response_model=List[HardwareLogOut])
def get_hardware_logs(
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    return (
        db.query(HardwareLog)
        .order_by(HardwareLog.created_at.desc())
        .limit(200)
        .all()
    )

@app.get("/admin/gate-events", response_model=List[GateEventOut])
def get_gate_events(
    limit: int = Query(default=200, ge=1, le=1000),
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    return (
        db.query(GateEvent)
        .order_by(GateEvent.received_at.desc())
        .limit(limit)
        .all()
    )

@app.get("/admin/slot-status-logs", response_model=List[SlotStatusLogOut])
def get_slot_status_logs(
    limit: int = Query(default=200, ge=1, le=1000),
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    return (
        db.query(SlotStatusLog)
        .order_by(SlotStatusLog.recorded_at.desc())
        .limit(limit)
        .all()
    )

@app.get("/admin/activity", response_model=List[AdminActivityOut])
def get_admin_activity(
    limit: int = Query(default=100, ge=1, le=500),
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    activities: list[AdminActivityOut] = []
    admins = db.query(User).filter(User.role == "admin").all()
    for admin in admins:
        if admin.created_at:
            activities.append(
                AdminActivityOut(
                    id=f"admin-created-{admin.id}",
                    actor_name=None,
                    action="admin_created",
                    source="admin_account",
                    occurred_at=admin.created_at,
                )
            )
        if admin.last_login_at:
            activities.append(
                AdminActivityOut(
                    id=f"admin-login-{admin.id}",
                    actor_name=admin.username or admin.name,
                    action="login",
                    source="authentication",
                    occurred_at=admin.last_login_at,
                )
            )
        if admin.last_logout_at:
            activities.append(
                AdminActivityOut(
                    id=f"admin-logout-{admin.id}",
                    actor_name=admin.username or admin.name,
                    action="logout",
                    source="authentication",
                    occurred_at=admin.last_logout_at,
                )
            )
        if admin.deleted_at:
            activities.append(
                AdminActivityOut(
                    id=f"admin-disabled-{admin.id}",
                    actor_name=None,
                    action="admin_deactivated",
                    source="admin_account",
                    occurred_at=admin.deleted_at,
                )
            )
    for log in (
        db.query(HardwareLog)
        .filter(HardwareLog.source == "admin")
        .order_by(HardwareLog.created_at.desc())
        .limit(limit)
        .all()
    ):
        actor_name = None
        if log.detail and "actor=" in log.detail:
            actor_name = log.detail.split("actor=", 1)[1].split(" |", 1)[0]
        activities.append(
            AdminActivityOut(
                id=f"hardware-log-{log.id}",
                actor_name=actor_name,
                action=log.status,
                source=log.source or "admin",
                occurred_at=log.created_at or datetime.now(timezone.utc),
            )
        )
    return sorted(
        activities,
        key=lambda item: _utc_datetime(item.occurred_at),
        reverse=True,
    )[:limit]

@app.get("/support/messages", response_model=List[SupportMessageOut])
def get_my_support_messages(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    db.query(SupportMessage).filter(
        SupportMessage.user_id == current_user.id,
        SupportMessage.sender_role == "admin",
        SupportMessage.read_at.is_(None),
    ).update({SupportMessage.read_at: now}, synchronize_session=False)
    db.commit()
    return (
        db.query(SupportMessage)
        .filter(SupportMessage.user_id == current_user.id)
        .order_by(SupportMessage.created_at.asc(), SupportMessage.id.asc())
        .all()
    )

@app.post("/support/messages", response_model=SupportMessageOut, status_code=201)
def create_support_message(
    body: SupportMessageCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    message = SupportMessage(
        user_id=current_user.id,
        sender_id=current_user.id,
        sender_role="user",
        body=body.body.strip(),
        created_at=datetime.now(timezone.utc),
    )
    db.add(message)
    db.commit()
    db.refresh(message)
    return message

@app.get("/admin/inbox", response_model=List[SupportConversationOut])
def get_admin_inbox(
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    grouped = (
        db.query(SupportMessage.user_id, func.max(SupportMessage.created_at))
        .group_by(SupportMessage.user_id)
        .all()
    )
    conversations = []
    for user_id, updated_at in grouped:
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            continue
        latest = (
            db.query(SupportMessage)
            .filter(SupportMessage.user_id == user_id)
            .order_by(SupportMessage.created_at.desc(), SupportMessage.id.desc())
            .first()
        )
        unread_count = (
            db.query(func.count(SupportMessage.id))
            .filter(
                SupportMessage.user_id == user_id,
                SupportMessage.sender_role == "user",
                SupportMessage.read_at.is_(None),
            )
            .scalar()
            or 0
        )
        conversations.append(
            SupportConversationOut(
                user_id=user.id,
                user_name=user.name,
                username=user.username,
                last_message=latest.body if latest else None,
                unread_count=int(unread_count),
                updated_at=updated_at,
            )
        )
    return sorted(
        conversations,
        key=lambda item: _utc_datetime(item.updated_at) if item.updated_at else datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

@app.get("/admin/inbox/{user_id}", response_model=List[SupportMessageOut])
def get_admin_conversation(
    user_id: int,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if db.query(User.id).filter(User.id == user_id).first() is None:
        raise HTTPException(404, "ไม่พบผู้ใช้")
    db.query(SupportMessage).filter(
        SupportMessage.user_id == user_id,
        SupportMessage.sender_role == "user",
        SupportMessage.read_at.is_(None),
    ).update(
        {SupportMessage.read_at: datetime.now(timezone.utc)},
        synchronize_session=False,
    )
    db.commit()
    return (
        db.query(SupportMessage)
        .filter(SupportMessage.user_id == user_id)
        .order_by(SupportMessage.created_at.asc(), SupportMessage.id.asc())
        .all()
    )

@app.post("/admin/inbox/{user_id}/reply", response_model=SupportMessageOut, status_code=201)
def reply_to_support_message(
    user_id: int,
    body: SupportMessageCreate,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if db.query(User.id).filter(User.id == user_id).first() is None:
        raise HTTPException(404, "ไม่พบผู้ใช้")
    message = SupportMessage(
        user_id=user_id,
        sender_id=current_admin.id,
        sender_role="admin",
        body=body.body.strip(),
        created_at=datetime.now(timezone.utc),
    )
    db.add(message)
    db.commit()
    db.refresh(message)
    return message

@app.get(
    "/admin/gate-state",
    response_model=GateStateOut,
)
def get_gate_state(
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    state = (
        db.query(GateState)
        .filter(GateState.gate == "main")
        .first()
    )

    if state is None:
        return {
            "gate": "main",
            "last_action": None,
            "source": None,
            "command_id": None,
            "updated_at": None,
        }

    return state

@app.get(
    "/admin/hardware-commands/{command_id}",
    response_model=HardwareCommandOut,
)
def get_hardware_command(
    command_id: str,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    command = (
        db.query(HardwareCommand)
        .filter(HardwareCommand.command_id == command_id)
        .first()
    )

    if command is None:
        raise HTTPException(404, "Hardware command not found")

    return command

@app.post("/admin/override/servo", status_code=202)
def manual_override_servo(
    body: ServoOverrideReq,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    actor = current_admin.username or current_admin.name
    pending = (
        db.query(HardwareCommand)
        .filter(
            HardwareCommand.device_name == "esp32_cam",
            HardwareCommand.action.in_(("open", "close")),
            HardwareCommand.status.in_(("queued", "failed", "no_response")),
        )
        .with_for_update()
        .all()
    )
    for old in pending:
        old.status = "superseded"
    command = _queue_servo_command(
        db=db,
        booking=None,
        qr_token=None,
        gate="admin",
        action=body.action,
        source="admin",
    )
    _stamp_admin_override(command, now)
    command.detail = f"Admin {actor} requested servo {body.action}"
    _record_hardware_log(
        db, "servo", "admin_override",
        f"action={body.action} | actor={actor}",
        command.command_id, "admin",
    )
    db.commit()
    published = _publish_servo_command(command)
    return {
        "message": (
            "Servo command sent; awaiting ESP32-CAM ACK"
            if published else
            "Servo command stored; continuing when connection returns"
        ),
        "command_id": command.command_id,
        "status": "queued" if published else "failed",
    }

@app.post("/admin/override/rgb", status_code=202)
def manual_override_rgb(
    body: RgbOverrideReq,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    slot = (
        db.query(Slot)
        .filter(Slot.name == body.slot_name)
        .first()
    )
    if slot is None:
        raise HTTPException(404, "ไม่พบช่องจอดนี้ในฐานข้อมูล")
    now = datetime.now(timezone.utc)
    actor = current_admin.username or current_admin.name
    pending = (
        db.query(HardwareCommand)
        .filter(
            HardwareCommand.gate == "slot",
            HardwareCommand.action == "set_status",
            HardwareCommand.status.in_(("queued", "failed", "no_response")),
        )
        .with_for_update()
        .all()
    )
    for old in pending:
        if isinstance(old.payload, dict) and old.payload.get("slot") == slot.name:
            old.status = "superseded"
    command = _queue_slot_command(
        db=db,
        slot_name=slot.name,
        status=body.status.value,
        source=f"admin:{actor}",
    )
    _stamp_admin_override(command, now)
    _record_hardware_log(
        db, f"rgb_{slot.name.lower()}", "admin_override",
        f"slot={slot.name} | status={body.status.value} | actor={actor}",
        command.command_id, "admin",
    )
    db.commit()
    published = _publish_hardware_command(
        command,
        f"parking/slot/{slot.name}/command",
        command.payload or {},
    )
    return {
        "message": (
            "RGB command sent; awaiting ESP32-WROOM ACK"
            if published else
            "RGB command stored; continuing when connection returns"
        ),
        "command_id": command.command_id,
        "status": "queued" if published else "failed",
    }