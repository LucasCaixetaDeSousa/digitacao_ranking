from __future__ import annotations

from typing import Any
import psycopg2
from psycopg2.pool import SimpleConnectionPool

from config_server import (
    DATABASE_URL,
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_PORT,
    DB_MINCONN,
    DB_MAXCONN,
    DB_CONNECT_TIMEOUT,
)

_pool: SimpleConnectionPool | None = None


# ==================================================
# CONFIG AUXILIAR
# ==================================================

def _build_conn_kwargs() -> dict[str, Any]:

    if DATABASE_URL:
        return {
            "dsn": DATABASE_URL,
            "connect_timeout": DB_CONNECT_TIMEOUT,
        }

    return {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "port": DB_PORT,
        "connect_timeout": DB_CONNECT_TIMEOUT,
    }


def db_configurada() -> bool:

    if DATABASE_URL:
        return True

    return all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD])


# ==================================================
# INIT POOL
# ==================================================

def init_pool(minconn: int | None = None, maxconn: int | None = None) -> SimpleConnectionPool:

    global _pool

    if _pool is not None:
        return _pool

    if not db_configurada():
        raise RuntimeError(
            "Banco de dados não configurado. Defina DATABASE_URL "
            "ou DB_HOST, DB_NAME, DB_USER e DB_PASSWORD."
        )

    if minconn is None:
        minconn = DB_MINCONN

    if maxconn is None:
        maxconn = DB_MAXCONN

    kwargs = _build_conn_kwargs()

    _pool = SimpleConnectionPool(minconn, maxconn, **kwargs)

    return _pool


# ==================================================
# GET CONNECTION
# ==================================================

def get_connection():

    global _pool

    if _pool is None:
        init_pool()

    return _pool.getconn()


# ==================================================
# RETURN CONNECTION
# ==================================================

def put_connection(conn):

    global _pool

    if _pool and conn:
        _pool.putconn(conn)


# ==================================================
# CLOSE POOL
# ==================================================

def close_pool():

    global _pool

    if _pool:
        _pool.closeall()
        _pool = None


# ==================================================
# INIT DATABASE
# ==================================================

def init_database():

    conn = get_connection()

    try:

        cur = conn.cursor()

        # ======================================
        # TURMAS
        # ======================================

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS turmas (
                nome TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT now()
            );
            """
        )

        # ======================================
        # ALUNOS
        # ======================================

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alunos (
                nome TEXT NOT NULL,
                turma TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (nome, turma)
            );
            """
        )

        # ======================================
        # NIVEIS
        # ======================================

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS niveis (
                id TEXT PRIMARY KEY,
                categoria TEXT NOT NULL,
                frase TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            );
            """
        )

        # ======================================
        # PROGRESSO
        # ======================================

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS progresso (
                nome TEXT NOT NULL,
                turma TEXT NOT NULL,
                nivel_id TEXT NOT NULL,
                concluido BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (nome, turma, nivel_id)
            );
            """
        )

        # ======================================
        # RANKING (scores)
        # ======================================

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scores (
                nome TEXT NOT NULL,
                turma TEXT NOT NULL,
                nivel_id TEXT NOT NULL,
                pontos INTEGER NOT NULL DEFAULT 0,
                tempo INTEGER NOT NULL DEFAULT 9999,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (nome, turma, nivel_id)
            );
            """
        )

        # ======================================
        # ADMIN DATA
        # ======================================

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_data (
                chave TEXT PRIMARY KEY,
                dados JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT now()
            );
            """
        )

        conn.commit()

        cur.close()

    finally:

        put_connection(conn)
