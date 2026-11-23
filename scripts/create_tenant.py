from __future__ import annotations

import argparse
import hashlib
import secrets

from app.database import SessionLocal
from app.models import ApiKey, Tenant


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a tenant and one API key")
    parser.add_argument("name", help="Display name for this tenant")
    args = parser.parse_args()

    raw_key = "whk_" + secrets.token_urlsafe(32)
    tenant = Tenant(name=args.name.strip())
    api_key = ApiKey(
        tenant_id=tenant.id,
        key_prefix=raw_key[:12],
        key_hash=hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
    )

    with SessionLocal.begin() as db:
        db.add(tenant)
        db.flush()
        api_key.tenant_id = tenant.id
        db.add(api_key)

    print(f"Tenant created: {tenant.name} ({tenant.id})")
    print("Copy this API key now; it is shown only once:")
    print(raw_key)


if __name__ == "__main__":
    main()
