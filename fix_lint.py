# Fix 1: smart_deduplicator.py - rename 'l' to 'ld'
with open("app/services/smart_deduplicator.py", "r", encoding="utf-8") as f:
    content = f.read()
content = content.replace(" for l in leads", " for ld in leads")
content = content.replace("[l.", "[ld.")
content = content.replace("(l.", "(ld.")
content = content.replace(" l.first_seen)", " ld.first_seen)")
with open("app/services/smart_deduplicator.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Fixed: smart_deduplicator.py - l -> ld")

# Fix 2: source_learning.py - rename 'l' to 'ld'
with open("app/services/source_learning.py", "r", encoding="utf-8") as f:
    content = f.read()
content = content.replace("[l for l in leads if l.get", "[ld for ld in leads if ld.get")
with open("app/services/source_learning.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Fixed: source_learning.py - l -> ld")

# Fix 3: url_filter.py - bare except -> except Exception
with open("app/services/url_filter.py", "r", encoding="utf-8") as f:
    content = f.read()
content = content.replace(
    "        except:\n            return ",
    "        except Exception:\n            return ",
)
with open("app/services/url_filter.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Fixed: url_filter.py - bare except")

# Fix 4: scraping_tasks.py - move sys import up + fix E712
with open("app/tasks/scraping_tasks.py", "r", encoding="utf-8") as f:
    content = f.read()
# Fix E712: == True -> .is_(True)
content = content.replace("Source.is_active == True", "Source.is_active.is_(True)")
with open("app/tasks/scraping_tasks.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Fixed: scraping_tasks.py - E712")

print("\nDone! Run: ruff check app/ --select=E,F,W --ignore=E501")
