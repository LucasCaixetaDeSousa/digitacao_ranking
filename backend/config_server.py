from __future__ import annotations
import os

# ===============================
# DATABASE
# ===============================

DATABASE_URL = os.getenv("DATABASE_URL", "")

DB_HOST = os.getenv("DB_HOST", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

DB_MINCONN = int(os.getenv("DB_MINCONN", "1"))
DB_MAXCONN = int(os.getenv("DB_MAXCONN", "10"))

DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))

# ===============================
# SERVER
# ===============================

SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "10000"))

# ===============================
# DEBUG
# ===============================

DEBUG = os.getenv("DEBUG", "false").lower() == "true"
