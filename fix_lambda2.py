lines = open("app/main.py", encoding="utf-8").read().split("\n")
del lines[2551:2562]
open("app/main.py", "w", encoding="utf-8").write("\n".join(lines))
print("[FIXED] Removed 11 leftover lambda lines")
