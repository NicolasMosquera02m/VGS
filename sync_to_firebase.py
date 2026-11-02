"""Script to synchronize local SQLite database with Firebase Cloud Firestore."""

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from auth_service import get_firebase_status, init_db, sync_sqlite_to_firebase


def main():
    print("=" * 60)
    print("🔥 VGS - Sincronizador de Base de Datos con Firebase Firestore")
    print("=" * 60)
    init_db()

    status = get_firebase_status()
    if not status.get("connected"):
        print(f"❌ Error de conexión: {status.get('error_message')}")
        print("\nPara conectar Firebase:")
        print("1. Ve a https://console.firebase.google.com")
        print("2. Abre tu proyecto > Configuración del proyecto > Cuentas de servicio.")
        print("3. Genera una nueva clave privada y guárdala como 'serviceAccountKey.json' en esta carpeta.")
        print("4. Vuelve a ejecutar este script.")
        sys.exit(1)

    print(f"✅ Conectado a Firebase Firestore (Proyecto: {status.get('project_id')})")
    print(f"🔑 Credenciales: {status.get('credentials_file')}")
    print("Iniciando carga de usuarios, roles, permisos, configuraciones y logs...")

    ok, message = sync_sqlite_to_firebase()
    if ok:
        print(f"\n🎉 {message}")
    else:
        print(f"\n⚠️ {message}")
        sys.exit(1)


if __name__ == "__main__":
    main()
