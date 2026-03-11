from __future__ import annotations

import json
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
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/score":
            self._handle_post_score()
            return

        if path == "/admin/salvar":
            self._handle_post_admin_salvar()
            return

        if path == "/turmas":
            self._handle_post_recurso_admin("turmas", default=[])
            return

        if path == "/alunos":
            self._handle_post_recurso_admin("alunos", default=[])
            return

        if path == "/niveis":
            self._handle_post_recurso_admin("niveis", default={})
            return

        if path == "/progresso":
            self._handle_post_progresso()
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
            self._handle_get_admin_dados(q)
            return

        if path == "/turmas":
            self._handle_get_recurso_admin("turmas", default=[])
            return

        if path == "/alunos":
            self._handle_get_recurso_admin("alunos", default=[])
            return

        if path == "/niveis":
            self._handle_get_recurso_admin("niveis", default={})
            return

        if path == "/progresso":
            self._handle_get_progresso(q)
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
    # POST HANDLERS
    # ==================================================

    def _handle_post_score(self) -> None:
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
                (nome, turma, nivel, pontos, tempo),
            )

            conn.commit()
            cur.close()

        finally:
            put_connection(conn)

        ranking = self._buscar_ranking_global(top=30)
        json_response(self, 201, ranking)

    def _handle_post_admin_salvar(self) -> None:
        data = json_body(self)

        chave = str(data.get("chave", "")).strip()
        conteudo = data.get("dados", None)

        if chave not in {"turmas", "alunos", "niveis"}:
            json_response(self, 400, {"error": "chave_invalida"})
            return

        ok = self._salvar_admin_data(chave, conteudo)

        if not ok:
            json_response(self, 500, {"error": "save_failed"})
            return

        json_response(self, 200, {"ok": True})

    def _handle_post_recurso_admin(self, chave: str, default):
        data = json_body(self)

        payload = data.get("dados", data)

        if payload is None:
            payload = default

        ok = self._salvar_admin_data(chave, payload)

        if not ok:
            json_response(self, 500, {"error": "save_failed"})
            return

        json_response(self, 200, {"ok": True})

    def _handle_post_progresso(self) -> None:
        data = json_body(self)

        nome = str(data.get("nome", "")).strip()
        turma = str(data.get("turma", "")).strip()
        nivel = _safe_int(data.get("nivel", 0), 0)

        if not nome or not turma:
            json_response(self, 400, {"error": "missing_fields"})
            return

        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO progresso (nome, turma, nivel_id, concluido, updated_at)
                VALUES (%s, %s, %s, TRUE, now())
                ON CONFLICT (nome, turma)
                DO UPDATE SET
                    nivel_id = EXCLUDED.nivel_id,
                    updated_at = now()
                """,
                (nome, turma, str(nivel)),
            )

            conn.commit()
            cur.close()

        finally:
            put_connection(conn)

        json_response(
            self,
            200,
            {
                "ok": True,
                "nome": nome,
                "turma": turma,
                "nivel": nivel,
            },
        )

    # ==================================================
    # GET HANDLERS
    # ==================================================

    def _handle_get_admin_dados(self, q) -> None:
        chave = str(q.get("chave", [""])[0]).strip()

        if chave not in {"turmas", "alunos", "niveis"}:
            json_response(self, 400, {"error": "chave_invalida"})
            return

        default = [] if chave in {"turmas", "alunos"} else {}
        dados = self._carregar_admin_data(chave, default=default)

        json_response(self, 200, dados)

    def _handle_get_recurso_admin(self, chave: str, default):
        dados = self._carregar_admin_data(chave, default=default)
        json_response(self, 200, dados)

    def _handle_get_progresso(self, q) -> None:
        nome = str(q.get("nome", [""])[0]).strip()
        turma = str(q.get("turma", [""])[0]).strip()

        if not nome or not turma:
            json_response(self, 400, {"error": "missing_fields"})
            return

        nivel = self._buscar_progresso(nome, turma)

        json_response(
            self,
            200,
            {
                "nome": nome,
                "turma": turma,
                "nivel": nivel,
            },
        )

    # ==================================================
    # ADMIN DATA
    # ==================================================

    def _carregar_admin_data(self, chave: str, default):
        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT dados
                FROM admin_data
                WHERE chave = %s
                """,
                (chave,),
            )

            row = cur.fetchone()
            cur.close()

            if not row or row[0] is None:
                return default

            dados = row[0]

            if isinstance(default, list) and not isinstance(dados, list):
                return default

            if isinstance(default, dict) and not isinstance(dados, dict):
                return default

            return dados

        finally:
            put_connection(conn)

    def _salvar_admin_data(self, chave: str, dados) -> bool:
        conn = get_connection()

        try:
            cur = conn.cursor()

            payload = json.dumps(dados, ensure_ascii=False)

            cur.execute(
                """
                INSERT INTO admin_data (chave, dados, updated_at)
                VALUES (%s, %s::jsonb, now())
                ON CONFLICT (chave)
                DO UPDATE SET
                    dados = EXCLUDED.dados,
                    updated_at = now()
                """,
                (chave, payload),
            )

            conn.commit()
            cur.close()

            return True

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False

        finally:
            put_connection(conn)

    # ==================================================
    # PROGRESSO
    # ==================================================

    def _buscar_progresso(self, nome: str, turma: str) -> int:
        conn = get_connection()

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT nivel_id
                FROM progresso
                WHERE nome = %s AND turma = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (nome, turma),
            )

            row = cur.fetchone()
            cur.close()

            if not row:
                return 0

            return _safe_int(row[0], 0)

        finally:
            put_connection(conn)

    # ==================================================
    # CONSULTAS DE RANKING
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
                (top,),
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
                (nivel, top),
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
                (turma, top),
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
                (turma, nivel, top),
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
