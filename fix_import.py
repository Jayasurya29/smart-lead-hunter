content = open("app/main.py", "r", encoding="utf-8", errors="replace").read()
old = 'app.mount("/static", StaticFiles(directory="app/static"), name="static")'
new = 'from fastapi.staticfiles import StaticFiles as _SF\napp.mount("/static", _SF(directory="app/static"), name="static")'
content = content.replace(old, new)
open("app/main.py", "w", encoding="utf-8").write(content)
print("Fixed")
