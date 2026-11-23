from __future__ import annotations

import hashlib
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select

from app.database import SessionLocal
from app.models import ApiKey, Tenant

bearer = HTTPBearer(auto_error=False)


def get_current_tenant(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> Tenant:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="A valid API key is required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    key_hash = hashlib.sha256(credentials.credentials.encode("utf-8")).hexdigest()
    with SessionLocal() as db:
        tenant = db.execute(
            select(Tenant)
            .join(ApiKey, ApiKey.tenant_id == Tenant.id)
            .where(
                ApiKey.key_hash == key_hash,
                ApiKey.revoked_at.is_(None),
                Tenant.status == "active",
            )
        ).scalar_one_or_none()
        if tenant is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked API key",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Tenant scalar fields remain available after this read-only session closes.
        return tenant


CurrentTenant = Annotated[Tenant, Depends(get_current_tenant)]
