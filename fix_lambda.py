f = open("app/main.py", encoding="utf-8").read()
lines = f.split("\n")
fixed = False
for i, line in enumerate(lines):
    if "_classify_msg_type = lambda msg:" in line:
        indent = "                    "
        new_lines = [
            indent + "def _classify_msg_type(msg):",
            indent
            + '    if any(s in msg for s in ["\\u2705", "\\u2728", "Found", "Added", "QUALIFIED"]):',
            indent + '        return "success"',
            indent + '    if any(s in msg for s in ["\\u274c", "Error", "Failed"]):',
            indent + '        return "error"',
            indent
            + '    if any(s in msg for s in ["\\u26a0\\ufe0f", "Warning", "Skip", "\\u26aa"]):',
            indent + '        return "warning"',
            indent
            + '    if any(s in msg for s in ["\\U0001f4e1", "\\U0001f50d", "\\U0001f9ea", "\\U0001f916", "\\U0001f4be", "Phase", "\\u2550\\u2550\\u2550"]):',
            indent + '        return "phase"',
            indent + '    return "info"',
        ]
        end = i
        for j in range(i, min(i + 10, len(lines))):
            if lines[j].strip() == ")":
                end = j
                break
        lines[i : end + 1] = new_lines
        fixed = True
        break
if fixed:
    open("app/main.py", "w", encoding="utf-8").write("\n".join(lines))
    print("[FIXED] lambda -> def")
else:
    print("[SKIP] lambda not found")
