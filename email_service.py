"""Email delivery service for password reset and notifications (SMTP / Gmail)."""

from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

ENV_PATH = Path(__file__).resolve().parent / ".env"


def _read_env_file() -> dict[str, str]:
    data = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip().strip("'\"")
    return data


def get_smtp_config() -> dict[str, Any] | None:
    # 1. Streamlit Cloud Secrets (st.secrets["smtp"])
    try:
        import streamlit as st
        if "smtp" in st.secrets:
            cfg = dict(st.secrets["smtp"])
            pwd = str(cfg.get("password", "")).replace(" ", "")
            return {
                "host": cfg.get("host", "smtp.gmail.com"),
                "port": int(cfg.get("port", 587)),
                "user": cfg.get("user", ""),
                "password": pwd,
                "sender_name": cfg.get("sender_name", "VGS Plataforma"),
            }
    except Exception:
        pass

    # 2. Environment variables or local .env file
    env_data = _read_env_file()
    host = os.getenv("SMTP_HOST") or env_data.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT") or env_data.get("SMTP_PORT", 587))
    user = os.getenv("SMTP_USER") or env_data.get("SMTP_USER", "")
    password = (os.getenv("SMTP_PASSWORD") or env_data.get("SMTP_PASSWORD", "")).replace(" ", "")
    sender_name = os.getenv("SMTP_SENDER_NAME") or env_data.get("SMTP_SENDER_NAME", "VGS Plataforma")

    if user and password:
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "sender_name": sender_name,
        }

    return None


def is_smtp_configured() -> bool:
    cfg = get_smtp_config()
    return bool(cfg and cfg.get("user") and cfg.get("password"))


def send_password_reset_email(
    to_email: str, reset_token: str, username: str = ""
) -> tuple[bool, str]:
    """Sends a password recovery email with the reset token."""
    cfg = get_smtp_config()
    if not cfg:
        return False, "El servicio de correo (SMTP) no está configurado."

    display_name = username or to_email
    subject = "Recuperación de contraseña - Plataforma VGS"

    # Plain text alternative
    text_content = f"""Hola {display_name},

Has solicitado restablecer tu contraseña en la Plataforma de Rentabilidad VGS.

Tu token de recuperación es:
{reset_token}

Ingresa este token en la pestaña 'Olvidé mi contraseña' de la aplicación para establecer tu nueva contraseña.
Este código es válido por 15 minutos. Si no solicitaste este cambio, puedes ignorar este mensaje.

Atentamente,
Equipo VGS
"""

    # HTML formatted email
    html_content = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; }}
    .container {{ max-width: 560px; margin: 0 auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }}
    .header {{ background: #1a1a2e; color: #ffffff; padding: 24px; text-align: center; }}
    .header h1 {{ margin: 0; font-size: 22px; }}
    .body {{ padding: 28px; color: #333333; line-height: 1.6; }}
    .token-box {{ background: #f0f4f8; border: 1px dashed #2a9d8f; border-radius: 6px; padding: 16px; text-align: center; margin: 20px 0; font-family: monospace; font-size: 20px; font-weight: bold; color: #1a1a2e; letter-spacing: 1px; word-break: break-all; }}
    .footer {{ font-size: 12px; color: #888888; text-align: center; padding: 16px; border-top: 1px solid #eeeeee; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>🎮 VGS - Plataforma de Videojuegos</h1>
    </div>
    <div class="body">
      <p>Hola <strong>{display_name}</strong>,</p>
      <p>Recibimos una solicitud para restablecer la contraseña de tu cuenta.</p>
      <p>Copia el siguiente <strong>token de recuperación</strong> e ingrésalo en la pestaña <em>"Olvidé mi contraseña"</em>:</p>
      <div class="token-box">{reset_token}</div>
      <p><small>⏱️ Este token vencerá en <strong>15 minutos</strong>. Si tú no realizaste esta solicitud, puedes ignorar este mensaje de forma segura.</small></p>
    </div>
    <div class="footer">
      VGS Plataforma &bull; Sistema Seguro de Autenticación
    </div>
  </div>
</body>
</html>
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{cfg['sender_name']} <{cfg['user']}>"
    msg["To"] = to_email

    msg.attach(MIMEText(text_content, "plain", "utf-8"))
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        server = smtplib.SMTP(cfg["host"], cfg["port"], timeout=15)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(cfg["user"], cfg["password"])
        server.sendmail(cfg["user"], [to_email], msg.as_string())
        server.quit()
        return True, f"Correo enviado exitosamente a {to_email}."
    except smtplib.SMTPAuthenticationError as exc:
        return False, f"Fallo de autenticación con Gmail: {exc}"
    except Exception as exc:
        return False, f"Error al enviar correo: {exc}"
