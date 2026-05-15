"""
User Authentication Service.

Provides user registration, login, JWT token management, and password handling.
"""

import hashlib
import hmac
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import jwt
import bcrypt
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Configuration
ALGORITHM = "HS256"


def _get_secret_key() -> str:
    """Get JWT secret key, failing loudly if not configured."""
    from app.core.config import get_settings

    key = get_settings().jwt_secret_key
    if not key:
        raise RuntimeError(
            "JWT_SECRET_KEY environment variable is required. "
            'Generate one with: python -c "import secrets; print(secrets.token_urlsafe(64))"'
        )
    return key


ACCESS_TOKEN_EXPIRE_MINUTES = 60
REFRESH_TOKEN_EXPIRE_DAYS = 7
PASSWORD_RESET_EXPIRE_HOURS = 24

# Passwordless sign-in (PLAN_063 / SPEC_053)
LOGIN_CODE_EXPIRE_MINUTES = 10
LOGIN_CODE_MAX_ATTEMPTS = 5


class AuthService:
    """User authentication service."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                name VARCHAR(255),
                is_active BOOLEAN DEFAULT TRUE,
                is_verified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login_at TIMESTAMP
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                token VARCHAR(64) NOT NULL UNIQUE,
                expires_at TIMESTAMP NOT NULL,
                used_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                token_hash VARCHAR(64) NOT NULL UNIQUE,
                expires_at TIMESTAMP NOT NULL,
                revoked_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Passwordless sign-in codes / magic-link tokens (PLAN_063 / SPEC_053)
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS login_codes (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                code_hash VARCHAR(64) NOT NULL,
                token_hash VARCHAR(64) NOT NULL,
                purpose VARCHAR(20) DEFAULT 'login',
                expires_at TIMESTAMP NOT NULL,
                consumed_at TIMESTAMP,
                attempts INTEGER DEFAULT 0,
                request_ip VARCHAR(45),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Create indexes
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_reset_tokens_token ON password_reset_tokens(token)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_refresh_tokens_hash ON refresh_tokens(token_hash)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_login_codes_email ON login_codes(email, created_at DESC)
        """)
        )

        self.db.commit()

        # Idempotent column migrations for passwordless users (PLAN_063 / SPEC_053).
        # password_hash must be nullable (passwordless users have none); tier +
        # signup_source support the playground free tier. Each wrapped so a
        # re-run, an already-migrated DB, or a non-Postgres backend can't crash
        # service construction.
        for migration in (
            "ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'free'",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS signup_source VARCHAR(100)",
        ):
            try:
                self.db.execute(text(migration))
                self.db.commit()
            except Exception as exc:  # noqa: BLE001 — best-effort, non-fatal
                self.db.rollback()
                logger.debug("login_codes/users migration skipped (%s): %s", migration, exc)

    def _hash_password(self, password: str) -> str:
        """Hash password with bcrypt."""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode(), salt).decode()

    def _verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash."""
        return bcrypt.checkpw(password.encode(), hashed.encode())

    def _create_access_token(self, user_id: int, email: str) -> str:
        """Create JWT access token."""
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        payload = {
            "sub": str(user_id),
            "email": email,
            "type": "access",
            "exp": expire,
            "iat": datetime.utcnow(),
        }
        return jwt.encode(payload, _get_secret_key(), algorithm=ALGORITHM)

    def _create_refresh_token(self, user_id: int) -> str:
        """Create and store refresh token."""
        token = secrets.token_urlsafe(32)
        token_hash = secrets.token_hex(32)
        expires_at = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

        self.db.execute(
            text("""
            INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
            VALUES (:user_id, :token_hash, :expires_at)
        """),
            {"user_id": user_id, "token_hash": token_hash, "expires_at": expires_at},
        )
        self.db.commit()

        return token

    def _issue_tokens_for_user(
        self, user_id: int, email: str, name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Issue the standard access+refresh token bundle for a user.

        Shared by password login and passwordless sign-in so there is exactly
        one place that mints tokens.
        """
        access_token = self._create_access_token(user_id, email)
        refresh_token = self._create_refresh_token(user_id)
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "user": {"id": user_id, "email": email, "name": name},
        }

    # ------------------------------------------------------------------
    # Passwordless sign-in (PLAN_063 / SPEC_053)
    # ------------------------------------------------------------------

    def _generate_login_code(self) -> str:
        """Return a zero-padded 6-digit numeric login code."""
        return f"{secrets.randbelow(1_000_000):06d}"

    def _hash_login_code(self, code: str) -> str:
        """SHA-256 hex digest of a login code (codes are never stored plaintext)."""
        return hashlib.sha256(code.encode()).hexdigest()

    def _hash_login_token(self, token: str) -> str:
        """SHA-256 hex digest of a magic-link token."""
        return hashlib.sha256(token.encode()).hexdigest()

    def _notify_lead_event(self, event: str, **kwargs) -> None:
        """Best-effort lead-capture hook. Lead capture must never block sign-in,
        and LeadService (PLAN_063 Step 3) may not exist yet — swallow everything."""
        try:
            from app.services.leads.lead_service import LeadService

            lead_service = LeadService(self.db)
            if event == "signup":
                lead_service.upsert_from_signup(
                    email=kwargs["email"],
                    signup_source=kwargs.get("signup_source", "playground"),
                    request_ip=kwargs.get("request_ip"),
                )
            elif event == "verified":
                lead_service.mark_verified(kwargs["email"])
        except Exception as exc:  # noqa: BLE001 — best-effort, non-fatal
            logger.debug("Lead hook (%s) skipped: %s", event, exc)

    def request_login_code(
        self,
        email: str,
        request_ip: Optional[str] = None,
        signup_source: str = "playground",
    ) -> Dict[str, Any]:
        """Create a passwordless sign-in code + magic-link token for an email.

        Rate-limited; auto-creates a `users` row for new emails. Returns
        {"email", "code", "link"} — the caller (an async endpoint) is
        responsible for actually emailing it and must never return the
        code/link to the client.
        """
        email = email.lower().strip()

        # Rate limit: <=3 unconsumed codes / 15 min, <=1 / 60 s
        recent = self.db.execute(
            text("""
            SELECT
                COUNT(*) FILTER (WHERE created_at > :win_15m) AS cnt_15m,
                COUNT(*) FILTER (WHERE created_at > :win_60s) AS cnt_60s
            FROM login_codes
            WHERE email = :email AND consumed_at IS NULL
        """),
            {
                "email": email,
                "win_15m": datetime.utcnow() - timedelta(minutes=15),
                "win_60s": datetime.utcnow() - timedelta(seconds=60),
            },
        ).fetchone()
        if recent and (recent[0] or 0) >= 3:
            raise ValueError("Too many sign-in requests. Please try again later.")
        if recent and (recent[1] or 0) >= 1:
            raise ValueError("Please wait a moment before requesting another code.")

        code = self._generate_login_code()
        token = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(minutes=LOGIN_CODE_EXPIRE_MINUTES)

        self.db.execute(
            text("""
            INSERT INTO login_codes
                (email, code_hash, token_hash, purpose, expires_at, request_ip)
            VALUES (:email, :code_hash, :token_hash, 'login', :expires_at, :request_ip)
        """),
            {
                "email": email,
                "code_hash": self._hash_login_code(code),
                "token_hash": self._hash_login_token(token),
                "expires_at": expires_at,
                "request_ip": request_ip,
            },
        )

        # Auto-create a passwordless user row for new emails.
        existing = self.db.execute(
            text("SELECT id FROM users WHERE email = :email"), {"email": email}
        ).fetchone()
        if not existing:
            self.db.execute(
                text("""
                INSERT INTO users (email, password_hash, is_verified, tier, signup_source)
                VALUES (:email, NULL, FALSE, 'free', :signup_source)
            """),
                {"email": email, "signup_source": signup_source},
            )
        self.db.commit()

        self._notify_lead_event(
            "signup", email=email, signup_source=signup_source, request_ip=request_ip
        )

        from app.core.config import get_settings

        base_url = (get_settings().playground_base_url or "").rstrip("/")
        link = f"{base_url}/playground.html?verify={token}"
        return {"email": email, "code": code, "link": link}

    def _consume_login_row(self, row_id: int) -> None:
        self.db.execute(
            text("UPDATE login_codes SET consumed_at = CURRENT_TIMESTAMP WHERE id = :id"),
            {"id": row_id},
        )
        self.db.commit()

    def _finalize_login(self, email: str) -> Dict[str, Any]:
        """Mark the user verified, bump last_login_at, issue tokens."""
        self.db.execute(
            text("""
            UPDATE users
            SET is_verified = TRUE, last_login_at = CURRENT_TIMESTAMP
            WHERE email = :email
        """),
            {"email": email},
        )
        self.db.commit()

        row = self.db.execute(
            text("SELECT id, email, name, is_active FROM users WHERE email = :email"),
            {"email": email},
        ).fetchone()
        if not row or not row[3]:
            raise ValueError("Account is not active")

        self._notify_lead_event("verified", email=email)
        return self._issue_tokens_for_user(row[0], row[1], row[2])

    def verify_login_code(self, email: str, code: str) -> Dict[str, Any]:
        """Verify a 6-digit login code and issue tokens. Raises ValueError on
        any failure (no row / expired / wrong code / locked out)."""
        email = email.lower().strip()

        row = self.db.execute(
            text("""
            SELECT id, code_hash, expires_at, attempts
            FROM login_codes
            WHERE email = :email AND consumed_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
        """),
            {"email": email},
        ).fetchone()
        if not row:
            raise ValueError("Invalid or expired code")

        row_id, code_hash, expires_at, attempts = row

        # Count this attempt first; lock the row once attempts exceed the cap.
        attempts_now = (attempts or 0) + 1
        self.db.execute(
            text("UPDATE login_codes SET attempts = :a WHERE id = :id"),
            {"a": attempts_now, "id": row_id},
        )
        self.db.commit()

        if attempts_now > LOGIN_CODE_MAX_ATTEMPTS:
            self._consume_login_row(row_id)
            raise ValueError("Too many attempts. Request a new code.")

        if expires_at < datetime.utcnow():
            raise ValueError("Code expired. Request a new one.")

        if not hmac.compare_digest(self._hash_login_code(code), code_hash):
            raise ValueError("Invalid code")

        self._consume_login_row(row_id)
        return self._finalize_login(email)

    def verify_login_token(self, token: str) -> Dict[str, Any]:
        """Verify a magic-link token and issue tokens. Raises ValueError on
        any failure (no row / expired)."""
        token_hash = self._hash_login_token(token)
        row = self.db.execute(
            text("""
            SELECT id, email, expires_at
            FROM login_codes
            WHERE token_hash = :token_hash AND consumed_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
        """),
            {"token_hash": token_hash},
        ).fetchone()
        if not row:
            raise ValueError("Invalid or expired sign-in link")

        row_id, email, expires_at = row
        if expires_at < datetime.utcnow():
            raise ValueError("Sign-in link expired. Request a new one.")

        self._consume_login_row(row_id)
        return self._finalize_login(email)

    def register(
        self, email: str, password: str, name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Register a new user."""
        # Check if email already exists
        result = self.db.execute(
            text("""
            SELECT id FROM users WHERE email = :email
        """),
            {"email": email.lower()},
        )

        if result.fetchone():
            raise ValueError("Email already registered")

        # Validate password strength
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters")

        # Hash password and create user
        password_hash = self._hash_password(password)

        result = self.db.execute(
            text("""
            INSERT INTO users (email, password_hash, name)
            VALUES (:email, :password_hash, :name)
            RETURNING id, created_at
        """),
            {"email": email.lower(), "password_hash": password_hash, "name": name},
        )

        row = result.fetchone()
        self.db.commit()

        user_id = row[0]

        # Generate tokens
        access_token = self._create_access_token(user_id, email.lower())
        refresh_token = self._create_refresh_token(user_id)

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "user": {
                "id": user_id,
                "email": email.lower(),
                "name": name,
                "created_at": row[1].isoformat() if row[1] else None,
            },
        }

    def login(self, email: str, password: str) -> Dict[str, Any]:
        """Authenticate user and return tokens."""
        result = self.db.execute(
            text("""
            SELECT id, email, password_hash, name, is_active
            FROM users WHERE email = :email
        """),
            {"email": email.lower()},
        )

        row = result.fetchone()
        if not row:
            raise ValueError("Invalid email or password")

        user_id, user_email, password_hash, name, is_active = row

        if not is_active:
            raise ValueError("Account is deactivated")

        if not self._verify_password(password, password_hash):
            raise ValueError("Invalid email or password")

        # Update last login
        self.db.execute(
            text("""
            UPDATE users SET last_login_at = CURRENT_TIMESTAMP WHERE id = :user_id
        """),
            {"user_id": user_id},
        )
        self.db.commit()

        # Generate tokens
        access_token = self._create_access_token(user_id, user_email)
        refresh_token = self._create_refresh_token(user_id)

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "user": {"id": user_id, "email": user_email, "name": name},
        }

    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify JWT and return user info."""
        try:
            payload = jwt.decode(token, _get_secret_key(), algorithms=[ALGORITHM])

            if payload.get("type") != "access":
                raise ValueError("Invalid token type")

            user_id = int(payload["sub"])

            # Get user
            result = self.db.execute(
                text("""
                SELECT id, email, name, is_active FROM users WHERE id = :user_id
            """),
                {"user_id": user_id},
            )

            row = result.fetchone()
            if not row or not row[3]:  # not active
                raise ValueError("User not found or inactive")

            return {"user_id": row[0], "email": row[1], "name": row[2]}

        except jwt.ExpiredSignatureError:
            raise ValueError("Token has expired")
        except jwt.InvalidTokenError:
            raise ValueError("Invalid token")

    def refresh_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh access token using refresh token."""
        # For simplicity, we'll generate a new access token based on the refresh token
        # In production, you'd validate the refresh token hash against the database

        # Get user from the token (simplified - in production use token hash lookup)
        try:
            # Decode without verification to get user_id
            # In real implementation, look up the token in the database
            result = self.db.execute(
                text("""
                SELECT rt.user_id, u.email, u.name, u.is_active, rt.expires_at, rt.revoked_at
                FROM refresh_tokens rt
                JOIN users u ON rt.user_id = u.id
                WHERE rt.revoked_at IS NULL
                ORDER BY rt.created_at DESC
                LIMIT 1
            """)
            )

            row = result.fetchone()
            if not row:
                raise ValueError("Invalid refresh token")

            user_id, email, name, is_active, expires_at, revoked_at = row

            if not is_active:
                raise ValueError("User inactive")

            if revoked_at:
                raise ValueError("Token revoked")

            if expires_at < datetime.utcnow():
                raise ValueError("Refresh token expired")

            # Generate new access token
            access_token = self._create_access_token(user_id, email)

            return {
                "access_token": access_token,
                "token_type": "bearer",
                "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            }

        except Exception as e:
            logger.error(f"Refresh token error: {e}")
            raise ValueError("Invalid refresh token")

    def logout(self, user_id: int) -> bool:
        """Revoke all refresh tokens for user."""
        self.db.execute(
            text("""
            UPDATE refresh_tokens
            SET revoked_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id AND revoked_at IS NULL
        """),
            {"user_id": user_id},
        )
        self.db.commit()
        return True

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID."""
        result = self.db.execute(
            text("""
            SELECT id, email, name, is_active, is_verified, created_at, last_login_at
            FROM users WHERE id = :user_id
        """),
            {"user_id": user_id},
        )

        row = result.fetchone()
        if not row:
            return None

        return {
            "id": row[0],
            "email": row[1],
            "name": row[2],
            "is_active": row[3],
            "is_verified": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
            "last_login_at": row[6].isoformat() if row[6] else None,
        }

    def update_user(
        self, user_id: int, updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update user profile."""
        allowed_fields = ["name"]
        set_clauses = []
        params = {"user_id": user_id}

        for field in allowed_fields:
            if field in updates:
                set_clauses.append(f"{field} = :{field}")
                params[field] = updates[field]

        if not set_clauses:
            return self.get_user(user_id)

        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

        query = f"UPDATE users SET {', '.join(set_clauses)} WHERE id = :user_id"
        self.db.execute(text(query), params)
        self.db.commit()

        return self.get_user(user_id)

    def change_password(
        self, user_id: int, old_password: str, new_password: str
    ) -> bool:
        """Change user password."""
        result = self.db.execute(
            text("""
            SELECT password_hash FROM users WHERE id = :user_id
        """),
            {"user_id": user_id},
        )

        row = result.fetchone()
        if not row:
            raise ValueError("User not found")

        if not self._verify_password(old_password, row[0]):
            raise ValueError("Current password is incorrect")

        if len(new_password) < 8:
            raise ValueError("New password must be at least 8 characters")

        new_hash = self._hash_password(new_password)

        self.db.execute(
            text("""
            UPDATE users SET password_hash = :password_hash, updated_at = CURRENT_TIMESTAMP
            WHERE id = :user_id
        """),
            {"user_id": user_id, "password_hash": new_hash},
        )
        self.db.commit()

        return True

    def request_password_reset(self, email: str) -> Optional[str]:
        """Create password reset token."""
        result = self.db.execute(
            text("""
            SELECT id FROM users WHERE email = :email AND is_active = TRUE
        """),
            {"email": email.lower()},
        )

        row = result.fetchone()
        if not row:
            # Don't reveal if email exists
            return None

        user_id = row[0]
        token = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(hours=PASSWORD_RESET_EXPIRE_HOURS)

        self.db.execute(
            text("""
            INSERT INTO password_reset_tokens (user_id, token, expires_at)
            VALUES (:user_id, :token, :expires_at)
        """),
            {"user_id": user_id, "token": token, "expires_at": expires_at},
        )
        self.db.commit()

        # In production, send email with token
        logger.info(f"Password reset token created for user {user_id}")
        return token

    def reset_password(self, token: str, new_password: str) -> bool:
        """Reset password using token."""
        result = self.db.execute(
            text("""
            SELECT prt.id, prt.user_id, prt.expires_at, prt.used_at
            FROM password_reset_tokens prt
            WHERE prt.token = :token
        """),
            {"token": token},
        )

        row = result.fetchone()
        if not row:
            raise ValueError("Invalid reset token")

        token_id, user_id, expires_at, used_at = row

        if used_at:
            raise ValueError("Token already used")

        if expires_at < datetime.utcnow():
            raise ValueError("Token expired")

        if len(new_password) < 8:
            raise ValueError("Password must be at least 8 characters")

        # Update password
        new_hash = self._hash_password(new_password)

        self.db.execute(
            text("""
            UPDATE users SET password_hash = :password_hash, updated_at = CURRENT_TIMESTAMP
            WHERE id = :user_id
        """),
            {"user_id": user_id, "password_hash": new_hash},
        )

        # Mark token as used
        self.db.execute(
            text("""
            UPDATE password_reset_tokens SET used_at = CURRENT_TIMESTAMP
            WHERE id = :token_id
        """),
            {"token_id": token_id},
        )

        # Revoke all refresh tokens
        self.db.execute(
            text("""
            UPDATE refresh_tokens SET revoked_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id AND revoked_at IS NULL
        """),
            {"user_id": user_id},
        )

        self.db.commit()
        return True
