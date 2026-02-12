# Fix 1: main.py - bare except
with open("app/main.py", "r", encoding="utf-8") as f:
    c = f.read()
c = c.replace(
    "        except:\n            pass", "        except Exception:\n            pass"
)
with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Fixed: main.py - bare except")

# Fix 2: intelligent_pipeline.py - l -> ld
with open("app/services/intelligent_pipeline.py", "r", encoding="utf-8") as f:
    c = f.read()
c = c.replace(
    "[l for l in qualified_leads\n                       if l.qualification_score",
    "[ld for ld in qualified_leads\n                       if ld.qualification_score",
)
c = c.replace(
    "[l for l in final_leads if l.qualification_score >= 70]",
    "[ld for ld in final_leads if ld.qualification_score >= 70]",
)
c = c.replace(
    "[l for l in final_leads if 40 <= l.qualification_score < 70]",
    "[ld for ld in final_leads if 40 <= ld.qualification_score < 70]",
)
c = c.replace(
    "[l for l in final_leads if l.qualification_score < 40]",
    "[ld for ld in final_leads if ld.qualification_score < 40]",
)
c = c.replace(
    "[l for l in final_leads if 'HOT' in l.lead_priority]",
    "[ld for ld in final_leads if 'HOT' in ld.lead_priority]",
)
c = c.replace(
    "[l for l in final_leads if 'WARM' in l.lead_priority]",
    "[ld for ld in final_leads if 'WARM' in ld.lead_priority]",
)
c = c.replace(
    "sum(l.qualification_score for l in final_leads)",
    "sum(ld.qualification_score for ld in final_leads)",
)
with open("app/services/intelligent_pipeline.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Fixed: intelligent_pipeline.py - l -> ld")

# Fix 3: orchestrator.py - unused imports
with open("app/services/orchestrator.py", "r", encoding="utf-8") as f:
    c = f.read()
c = c.replace("        PipelineResult,\n        ExtractedLead,\n", "")
c = c.replace(
    "from app.services.smart_deduplicator import SmartDeduplicator, MergedLead",
    "from app.services.smart_deduplicator import SmartDeduplicator",
)
with open("app/services/orchestrator.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Fixed: orchestrator.py - unused imports")

# Fix 4: scraping_engine.py - unused imports
with open("app/services/scraping_engine.py", "r", encoding="utf-8") as f:
    c = f.read()
c = c.replace(
    "from app.services.url_filter import URLFilter, URLFilterResult",
    "from app.services.url_filter import URLFilter",
)
c = c.replace(
    "get_link_patterns, get_max_pages, has_patterns, SourcePatterns,",
    "get_link_patterns, get_max_pages, has_patterns,",
)
# Fix l -> ld in filter
c = c.replace(
    "[l for l in clean if self._should_follow_link(l, source)]",
    "[lnk for lnk in clean if self._should_follow_link(lnk, source)]",
)
with open("app/services/scraping_engine.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Fixed: scraping_engine.py - unused imports + l -> lnk")

print("\nRemaining E701/E702 (one-liners) are style issues.")
print("To ignore them in CI, update the ruff command.")
print("Done!")
