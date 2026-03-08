from __future__ import annotations

import sys
import pygame

from core.game import Game


def encerrar_tudo() -> None:
    try:
        pygame.mixer.quit()
    except Exception:
        pass

    try:
        pygame.quit()
    except Exception:
        pass


def main() -> None:
    # Modo servidor local, usado apenas se você quiser rodar o backend separadamente.
    if "--backend" in sys.argv:
        from backend.server import run
        run()
        return

    try:
        jogo = Game()
        jogo.executar()
    finally:
        encerrar_tudo()


if __name__ == "__main__":
    main()
