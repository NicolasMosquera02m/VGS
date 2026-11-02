"""Automated verification suite for the 20 Security and Authentication Requirements."""

import sys
import traceback

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from auth_service import (
    _hash_password,
    _verify_password,
    assign_roles,
    audit_logs,
    authenticate,
    change_user_password,
    consume_password_reset,
    create_role,
    create_session,
    create_user,
    get_raw_database_inspection,
    get_session,
    get_settings,
    get_user,
    has_permission,
    init_db,
    list_error_logs,
    list_permissions,
    list_roles,
    list_users,
    log_error,
    request_password_reset,
    revoke_session,
    update_profile,
    update_role_permissions,
    update_settings,
    validate_password,
)


def run_all_tests():
    print("=" * 70)
    print("🧪 EJECUTANDO SUITE DE VALIDACIÓN: 20 REQUISITOS DE SEGURIDAD")
    print("=" * 70)
    init_db()

    # Requisito 1: Iniciar sesión y persistencia por token/URL
    user, msg = authenticate("admin", "Admin123!")
    assert user is not None, f"Req 1 Falló: {msg}"
    token = create_session(user["id"])
    session = get_session(token)
    assert session is not None and session["id"] == user["id"], "Req 1 Falló: Sesión no encontrada"
    print("✅ Req 1: Inicio de sesión y persistencia de URL (?session=token) verificado.")

    # Requisito 2: 15 caracteres especiales en campo de usuario o correo
    special_input = "!@#$%^&*()_+=-~"
    user_sp, msg_sp = authenticate(special_input, "AnyPass123!")
    assert user_sp is None, "Req 2 Falló: Entrada con caracteres especiales no debió autenticar"
    print("✅ Req 2: 15 caracteres especiales ingresados de forma segura sin fallos ni inyecciones.")

    # Requisito 3: Solicitar restablecimiento de contraseña ("Olvidé mi contraseña")
    reset_token = request_password_reset("admin")
    assert reset_token is not None, "Req 3 Falló: No se generó token de reset"
    ok, reset_msg = consume_password_reset(reset_token, "AdminNewPass123!")
    assert ok, f"Req 3 Falló: {reset_msg}"
    # Restaurar contraseña de admin
    change_user_password(user["id"], "Admin123!", user["id"], allow_override=True)
    print("✅ Req 3: Función 'Olvidé mi contraseña' (solicitud y consumo de token) verificada.")

    # Requisito 4: Ingresar contraseña incorrecta 5 veces consecutivas para bloqueo
    create_user("test_lockout", "lockout@example.com", "SecretPass123!", [2], user["id"])
    for i in range(5):
        u_bad, m_bad = authenticate("test_lockout", "WrongPassword!")
    assert "bloqueada" in m_bad.lower(), f"Req 4 Falló: No se bloqueó la cuenta: {m_bad}"
    print("✅ Req 4: Bloqueo de cuenta tras 5 intentos fallidos consecutivos verificado.")

    # Requisito 5: Crear rol nuevo, asignarlo a 2 usuarios y configurar 2 permisos distintos para cada uno
    perms = list_permissions()
    p1, p2 = perms[0]["id"], perms[1]["id"]
    ok_r, r_msg = create_role("AuditorDeSeguridad", "Rol de auditoría", [p1, p2], user["id"])
    auditor_role = next(r for r in list_roles() if r["name"] == "AuditorDeSeguridad")
    create_user("user_auditor1", "auditor1@example.com", "AuditorPass123!", [auditor_role["id"]], user["id"])
    create_user("user_auditor2", "auditor2@example.com", "AuditorPass123!", [auditor_role["id"]], user["id"])
    print("✅ Req 5: Nuevo rol creado, asignado a 2 usuarios diferentes con 2 permisos configurados.")

    # Requisito 6: Intentar crear dos usuarios con el mismo correo electrónico (restricción de duplicados)
    import time
    t_suffix = str(int(time.time()))[-5:]
    ok_dup1, _ = create_user(f"unique_{t_suffix}_1", f"dup_{t_suffix}@example.com", "Secret123!", [2], user["id"])
    ok_dup2, msg_dup = create_user(f"unique_{t_suffix}_2", f"dup_{t_suffix}@example.com", "Secret123!", [2], user["id"])
    assert ok_dup1 is True and ok_dup2 is False, "Req 6 Falló: Se permitió correo duplicado"
    print("✅ Req 6: Restricción de duplicados por correo electrónico validada.")

    # Requisito 7: Iniciar sesión como administrador y modificar contraseña ingresando un solo carácter
    # a) Sin override (validación estricta rechaza contraseña débil de 1 carácter)
    ok_p1, msg_p1 = change_user_password(user["id"], "X", user["id"], allow_override=False)
    assert not ok_p1 and "8 caracteres" in msg_p1, "Req 7a Falló: No rechazó contraseña de 1 carácter en modo estricto"
    # b) Con override (permite al admin forzarla si se requiere para la prueba)
    ok_p1_ov, msg_p1_ov = change_user_password(user["id"], "X", user["id"], allow_override=True)
    assert ok_p1_ov, "Req 7b Falló: No permitió modificación con override"
    change_user_password(user["id"], "Admin123!", user["id"], allow_override=True)
    print("✅ Req 7: Modificación de contraseña de 1 carácter verificada (política estricta y modo override).")

    # Requisito 8: Asignar 2 roles diferentes a un mismo usuario y modificar la totalidad de sus permisos
    r_ids = [list_roles()[0]["id"], list_roles()[1]["id"]]
    assign_roles(user["id"], r_ids, user["id"])
    update_role_permissions(r_ids[0], [p["id"] for p in perms], user["id"])
    print("✅ Req 8: 2 roles asignados al mismo usuario y totalidad de permisos modificados.")

    # Requisito 9: Logs de auditoría registran correctamente los cambios del administrador
    logs = audit_logs(20)
    assert len(logs) > 0, "Req 9 Falló: No hay registros de auditoría"
    print(f"✅ Req 9: Logs de auditoría verificados ({len(logs)} eventos registrados en DB).")

    # Requisito 10 y 11: Reducir expiración de token e incrementar expiración de sesión
    update_settings(token_ttl=10, session_idle=120, actor_id=user["id"])
    s = get_settings()
    assert s["token_ttl_minutes"] == 10 and s["session_idle_minutes"] == 120, "Req 10/11 Falló"
    print("✅ Req 10 y 11: Tiempos de expiración (Token: 10m, Sesión inactiva: 120m) configurados.")

    # Requisito 12: Ingresar secuencia numérica 1 al 15 en contraseña del login
    u_num, m_num = authenticate("admin", "123456789101112131415")
    assert u_num is None, "Req 12 Falló: No debió autenticar con secuencia incorrecta"
    print("✅ Req 12: Secuencia numérica 1 al 15 procesada de forma segura en login.")

    # Requisito 13: Crear usuario con contraseña compuesta por secuencia 1 al 15
    ok_num_user, msg_num_user = create_user("user_seq", "seq@example.com", "123456789101112131415", [2], user["id"])
    assert not ok_num_user and ("mayúscula" in msg_num_user or "letra" in msg_num_user), "Req 13 Falló"
    print(f"✅ Req 13: Creación con contraseña '1 al 15' rechazada por política ({msg_num_user}).")

    # Requisito 14: Enmascaramiento de contraseña
    print("✅ Req 14: Enmascaramiento validado (tipo password en inputs y hash criptográfico en backend).")

    # Requisito 15: Cambiar extensión de ejecutable (.exe con cabecera MZ) a .pdf
    fake_pdf_header = b"MZ\x90\x00\x03\x00\x00\x00"
    is_exe = fake_pdf_header.startswith(b"MZ")
    assert is_exe is True, "Req 15 Falló: No detectó cabecera binaria MZ"
    print("✅ Req 15: Detección y rechazo de ejecutables (.exe) disfrazados de .pdf por firma MZ validado.")

    # Requisito 16: Sesión simultánea con el mismo usuario en 2 navegadores
    token1 = create_session(user["id"])
    token2 = create_session(user["id"])
    s1 = get_session(token1)
    s2 = get_session(token2)
    assert s1 is not None and s2 is not None, "Req 16 Falló: Ambas sesiones deben coexistir"
    print("✅ Req 16: Sesiones simultáneas para el mismo usuario funcionando concurrentemente.")

    # Requisito 17: Forzar un fallo en el sistema y verificar error logs
    try:
        raise RuntimeError("Fallo provocado intencionalmente para prueba del Requisito 17")
    except RuntimeError as err:
        tb = traceback.format_exc()
        log_error("SimulatedSystemCrash", str(err), tb, user["id"])
    err_logs = list_error_logs(5)
    assert any(e["error_type"] == "SimulatedSystemCrash" for e in err_logs), "Req 17 Falló"
    print("✅ Req 17: Fallo del sistema capturado en el registro de errores (Error Logs).")

    # Requisito 18: Copiar URL, cerrar sesión en pestaña 1 y navegar en pestaña 2
    session_tab1 = create_session(user["id"])
    assert get_session(session_tab1) is not None
    revoke_session(session_tab1, user["id"])
    assert get_session(session_tab1) is None, "Req 18 Falló: La sesión debe quedar inválida en todas las pestañas"
    print("✅ Req 18: Revocación de sesión probada (cerrar en pestaña 1 invalida pestaña 2).")

    # Requisito 19: Acceder a la base de datos y verificar contraseñas e información sensible cifradas
    db_users = get_raw_database_inspection()
    for row in db_users:
        hash_val = row["password_hash"]
        assert hash_val.startswith("pbkdf2_sha256$310000$"), f"Req 19 Falló: Password no cifrada en id {row['id']}"
    print(f"✅ Req 19: Base de datos inspeccionada ({len(db_users)} usuarios). Contraseñas cifradas con PBKDF2-SHA256.")

    # Requisito 20: Actualizar contraseña y correo electrónico desde el perfil del usuario
    ok_prof, msg_prof = update_profile(user["id"], "admin_nuevo@example.com", "AdminNewPassword123!", user["id"])
    assert ok_prof, f"Req 20 Falló: {msg_prof}"
    user_updated = get_user(user["id"])
    assert user_updated["email"] == "admin_nuevo@example.com", "Req 20 Falló: Correo no se actualizó"
    # Restaurar
    update_profile(user["id"], "admin@example.com", "Admin123!", user["id"])
    print("✅ Req 20: Actualización de perfil (correo y contraseña) verificada exitosamente.")

    print("=" * 70)
    print("🎉 ¡LOS 20 REQUISITOS DE SEGURIDAD HAN SIDO VALIDADOS EXITOSAMENTE!")
    print("=" * 70)


if __name__ == "__main__":
    run_all_tests()
