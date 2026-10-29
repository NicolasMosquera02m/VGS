"""Compatibility entry point for the application's security service."""

from auth_service import authenticate, create_user, init_db


if __name__ == "__main__":
    init_db()
    print("Sistema de autenticación inicializado.")
    print("Cuenta inicial: admin / Admin123! (cámbiala desde Mi perfil).")
