"""Fix duplicate auth import. Run: python fix_auth.py"""

f = open("app/main.py", encoding="utf-8").read()

# Count how many times the import appears
count = f.count("from app.middleware.auth import APIKeyMiddleware")
print(f"Found {count} auth import(s)")

# Remove BOTH mid-file blocks (we'll add one clean version at the top)
# Block 1:
f = f.replace(
    """
# Auth Middleware (Audit Fix 1)
from app.middleware.auth import APIKeyMiddleware
app.add_middleware(APIKeyMiddleware)""",
    "",
)

# Block 2:
f = f.replace(
    """
# ── AUTH MIDDLEWARE (Audit Fix #1) ──
from app.middleware.auth import APIKeyMiddleware
app.add_middleware(APIKeyMiddleware)""",
    "",
)

# Now add the import at the top (after other app imports)
f = f.replace(
    "from app.services.utils import normalize_hotel_name",
    "from app.services.utils import normalize_hotel_name\nfrom app.middleware.auth import APIKeyMiddleware",
)

# Add the middleware registration right after CORS (find the closing paren of CORS)
f = f.replace(
    '    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],\n)',
    '    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],\n)\napp.add_middleware(APIKeyMiddleware)',
)

open("app/main.py", "w", encoding="utf-8").write(f)

# Verify
f2 = open("app/main.py", encoding="utf-8").read()
count2 = f2.count("from app.middleware.auth import APIKeyMiddleware")
count3 = f2.count("app.add_middleware(APIKeyMiddleware)")
print(f"After fix: {count2} import(s), {count3} registration(s)")
print("FIXED" if count2 == 1 and count3 == 1 else "CHECK MANUALLY")
