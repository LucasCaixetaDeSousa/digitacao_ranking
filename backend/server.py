from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from config_server import SERVER_HOST, SERVER_PORT, DEBUG
from database import (
    close_pool,
    get_connection,
    init_database,
    init_pool,
    put_connection,
)

# ==================================================
# UTIL
# ==================================================

def json_body(handler: BaseHTTPRequestHandler) -> dict:
    try:
        length = int(handler.headers.get("Content-Length", "0"))

        if length <= 0:
            return {}

        raw = handler.rfile.read(length)
        data = json.loads(raw.decode("utf-8"))

        return data if isinstance(data, dict) else {}

    except Exception:
        return {}


def json_response(handler: BaseHTTPRequestHandler, status: int, payload) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()
    handler.wfile.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def _safe_int(valor, padrao=0) -> int:
    try:
        return int(valor)
    except Exception:
        return padrao


# ==================================================
# ADMIN DATA (persistente)
# ==================================================

ADMIN_FILE = "admin_data.json"


def carregar_admin():

    if os.path.exists(ADMIN_FILE):
        try:
            with open(ADMIN_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    return {
        "niveis": {},
        "turmas": {},
        "alunos": {}
    }


def salvar_admin(data):

    try:
        with open(ADMIN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


ADMIN_DATA = carregar_admin()


# ==================================================
# HANDLER
# ==================================================

class RankingHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        if DEBUG:
            super().log_message(format, *args)

    def do_OPTIONS(self):
        json_response(self, 200, {"ok": True})

    # --------------------------------------------------
    # POST
    # --------------------------------------------------

    def do_POST(self):

        # SCORE
        if self.path == "/score":

            data = json_body(self)

            nome = str(data.get("nome", "")).strip()
            turma = str(data.get("turma", "")).strip()
            nivel = str(data.get("nivel", "")).strip()
            pontos = _safe_int(data.get("pontos", 0), 0)
            tempo = _safe_int(data.get("tempo", 9999), 9999)

            if not nome or not turma or not nivel:
                json_response(self, 400, {"error": "missing_fields"})
                return

            conn = get_connection()

            try:
                cur = conn.cursor()

                cur.execute(
                    """
                    INSERT INTO scores (nome, turma, nivel_id, pontos, tempo)
                    VALUES (%s, %s, %s, %s, %s)

                    ON CONFLICT (nome, turma, nivel_id)
                    DO UPDATE SET
                        pontos = GREATEST(scores.pontos, EXCLUDED.pontos),
                        tempo = CASE
                            WHEN EXCLUDED.pontos > scores.pontos THEN EXCLUDED.tempo
                            WHEN EXCLUDED.pontos = scores.pontos THEN LEAST(scores.tempo, EXCLUDED.tempo)
                            ELSE scores.tempo
                        END,
                        updated_at = now();
                    """,
                    (nome, turma, nivel, pontos, tempo)
                )

                conn.commit()
                cur.close()

            finally:
                put_connection(conn)

            ranking = self._buscar_ranking_global(top=30)
            json_response(self, 201, ranking)
            return

        # ADMIN SAVE
        if self.path == "/admin/salvar":

            data = json_body(self)

            chave = str(data.get("chave", "")).strip()
            conteudo = data.get("dados", {})

            if chave not in ADMIN_DATA:
                json_response(self, 400, {"error": "chave_invalida"})
                return

            ADMIN_DATA[chave] = conteudo
            salvar_admin(ADMIN_DATA)

            json_response(self, 200, {"ok": True})
            return

        json_response(self, 404, {"error": "not_found"})

    # --------------------------------------------------
    # GET
    # --------------------------------------------------

    def do_GET(self):

        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)

        if path == "/health":
            json_response(self, 200, {"status": "ok"})
            return

        if path == "/admin/dados":

            chave = str(q.get("chave", [""])[0]).strip()

            if chave not in ADMIN_DATA:
                json_response(self, 400, {"error": "chave_invalida"})
                return

            json_response(self, 200, ADMIN_DATA[chave])
            return

        top = _safe_int(q.get("top", ["20"])[0], 20)

        if path == "/ranking/global/geral":
            resp = self._buscar_ranking_global(top=top)

        elif path == "/ranking/global/nivel":
            nivel = str(q.get("nivel", [""])[0]).strip()
            resp = self._buscar_ranking_global_nivel(nivel, top)

        elif path == "/ranking/turma/geral":
            turma = str(q.get("turma", [""])[0]).strip()
            resp = self._buscar_ranking_turma(turma, top)

        elif path == "/ranking/turma/nivel":
            turma = str(q.get("turma", [""])[0]).strip()
            nivel = str(q.get("nivel", [""])[0]).strip()
            resp = self._buscar_ranking_turma_nivel(turma, nivel, top)

        else:
            json_response(self, 404, {"error": "not_found"})
            return

        json_response(self, 200, resp)

    # ==================================================
    # CONSULTAS
    # ==================================================

    def _buscar_ranking_global(self, top=20):

        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT nome, turma, SUM(pontos) AS total, SUM(tempo) AS total_tempo
                FROM scores
                GROUP BY nome, turma
                HAVING SUM(pontos) > 0
                ORDER BY total DESC, total_tempo ASC
                LIMIT %s
                """,
                (top,)
            )

            rows = cur.fetchall()
            cur.close()

            return [
                {
                    "nome": r[0],
                    "turma": r[1],
                    "pontos": int(r[2]),
                    "tempo": int(r[3] or 0),
                }
                for r in rows
            ]

        finally:
            put_connection(conn)

    def _buscar_ranking_global_nivel(self, nivel, top=20):

        if not nivel:
            return []

        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT nome, turma, pontos, tempo
                FROM scores
                WHERE nivel_id = %s AND pontos > 0
                ORDER BY pontos DESC, tempo ASC
                LIMIT %s
                """,
                (nivel, top)
            )

            rows = cur.fetchall()
            cur.close()

            return [
                {
                    "nome": r[0],
                    "turma": r[1],
                    "pontos": int(r[2]),
                    "tempo": int(r[3] or 0),
                }
                for r in rows
            ]

        finally:
            put_connection(conn)

    def _buscar_ranking_turma(self, turma, top=20):

        if not turma:
            return []

        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT nome, turma, SUM(pontos) AS total, SUM(tempo) AS total_tempo
                FROM scores
                WHERE turma = %s
                GROUP BY nome, turma
                HAVING SUM(pontos) > 0
                ORDER BY total DESC, total_tempo ASC
                LIMIT %s
                """,
                (turma, top)
            )

            rows = cur.fetchall()
            cur.close()

            return [
                {
                    "nome": r[0],
                    "turma": r[1],
                    "pontos": int(r[2]),
                    "tempo": int(r[3] or 0),
                }
                for r in rows
            ]

        finally:
            put_connection(conn)

    def _buscar_ranking_turma_nivel(self, turma, nivel, top=20):

        if not turma or not nivel:
            return []

        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT nome, turma, pontos, tempo
                FROM scores
                WHERE turma = %s AND nivel_id = %s AND pontos > 0
                ORDER BY pontos DESC, tempo ASC
                LIMIT %s
                """,
                (turma, nivel, top)
            )

            rows = cur.fetchall()
            cur.close()

            return [
                {
                    "nome": r[0],
                    "turma": r[1],
                    "pontos": int(r[2]),
                    "tempo": int(r[3] or 0),
                }
                for r in rows
            ]

        finally:
            put_connection(conn)


# ==================================================
# RUN SERVER
# ==================================================

def run() -> None:

    init_pool()
    init_database()

    server = ThreadingHTTPServer((SERVER_HOST, SERVER_PORT), RankingHandler)

    print(f"Servidor rodando em http://{SERVER_HOST}:{SERVER_PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        close_pool()


if __name__ == "__main__":
    run()
