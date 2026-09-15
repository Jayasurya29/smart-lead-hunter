"""
patch_gemini3_migration.py -- retire every Gemini 2.5 call in SLH.

Google retires 2.5 Flash / Flash-Lite on Vertex (Lite off Jan 28 2027,
Flash off Mar 31 2027). 17 files hard-coded 2.5 strings that bypassed .env
(incl. /health, which pinged gemini-2.5-flash on every check).

After this patch .env is the ONLY switch:
    GEMINI_MODEL       -> every "full" call (extractor, tier2, news, outreach, grounding)
    GEMINI_MODEL_LITE  -> every "lite" call (classifier, tier1, parser, inbox_sync, critic)

Fallback chains now use only 3.x models (both verified on `global`):
    gemini-3.5-flash, gemini-3.1-flash-lite

Anchored + idempotent. .bak per file, py_compile auto-restore on failure.
Run from repo root:   python patch_gemini3_migration.py
"""

import py_compile
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# (file, marker_if_already_applied, old, new)
EDITS = [
    # ── config defaults ─────────────────────────────────────────────
    ("app/config_app.py",
     'gemini_model: str = "gemini-3.5-flash"',
     'gemini_model: str = "gemini-2.5-flash"',
     'gemini_model: str = "gemini-3.5-flash"'),

    ("app/config/intelligence_config.py",
     "EXTRACTOR_MODEL = _app_settings.gemini_model",
     'CLASSIFIER_MODEL = "gemini-3.1-flash-lite"  # 4,000 RPM / Unlimited RPD\n'
     'EXTRACTOR_MODEL = "gemini-2.5-flash"  # 1,000 RPM / 10,000 RPD\n',
     "from app.config_app import settings as _app_settings  # noqa: E402\n\n"
     "CLASSIFIER_MODEL = _app_settings.gemini_model_lite  # env: GEMINI_MODEL_LITE\n"
     "EXTRACTOR_MODEL = _app_settings.gemini_model  # env: GEMINI_MODEL\n"),

    ("app/config/enrichment_config.py",
     'model = getattr(settings, "gemini_model", "gemini-3.5-flash")',
     '        model = getattr(settings, "gemini_model", "gemini-2.5-flash")\n'
     "        ENRICHMENT_SETTINGS[\"gemini_model\"] = model\n"
     "        return model\n"
     "    except Exception:\n"
     '        return "gemini-2.5-flash"',
     '        model = getattr(settings, "gemini_model", "gemini-3.5-flash")\n'
     "        ENRICHMENT_SETTINGS[\"gemini_model\"] = model\n"
     "        return model\n"
     "    except Exception:\n"
     '        return "gemini-3.5-flash"'),

    # ── ai_client fallback defaults ─────────────────────────────────
    ("app/services/ai_client.py",
     'os.getenv("AI_MODEL", "gemini-3.5-flash")\n            ),',
     'settings, "gemini_model", os.getenv("AI_MODEL", "gemini-2.5-flash")\n'
     "            ),\n"
     '            "model_lite": getattr(\n'
     "                settings,\n"
     '                "gemini_model_lite",\n'
     '                os.getenv("AI_MODEL_LITE", "gemini-2.5-flash-lite"),',
     'settings, "gemini_model", os.getenv("AI_MODEL", "gemini-3.5-flash")\n'
     "            ),\n"
     '            "model_lite": getattr(\n'
     "                settings,\n"
     '                "gemini_model_lite",\n'
     '                os.getenv("AI_MODEL_LITE", "gemini-3.1-flash-lite"),'),

    ("app/services/ai_client.py",
     '"model": os.getenv("AI_MODEL", "gemini-3.5-flash"),',
     '            "model": os.getenv("AI_MODEL", "gemini-2.5-flash"),\n'
     '            "model_lite": os.getenv("AI_MODEL_LITE", "gemini-2.5-flash-lite"),',
     '            "model": os.getenv("AI_MODEL", "gemini-3.5-flash"),\n'
     '            "model_lite": os.getenv("AI_MODEL_LITE", "gemini-3.1-flash-lite"),'),

    ("app/services/ai_client.py",
     'text = await ai_generate(client, prompt, model="gemini-3.1-flash-lite")',
     'text = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")',
     'text = await ai_generate(client, prompt, model="gemini-3.1-flash-lite")'),

    # ── module MODEL constants -> settings ──────────────────────────
    ("app/services/contact_tier1_enrichment.py",
     "MODEL = _settings.gemini_model_lite",
     "from app.services.ai_client import ai_generate\n",
     "from app.services.ai_client import ai_generate\n"
     "from app.config import settings as _settings\n"),
    ("app/services/contact_tier1_enrichment.py",
     "MODEL = _settings.gemini_model_lite",
     'MODEL = "gemini-2.5-flash-lite"\n',
     "MODEL = _settings.gemini_model_lite  # env: GEMINI_MODEL_LITE\n"),

    ("app/services/contact_tier2_enrichment.py",
     "MODEL = _settings.gemini_model",
     "from app.services.ai_client import ai_generate\n",
     "from app.services.ai_client import ai_generate\n"
     "from app.config import settings as _settings\n"),
    ("app/services/contact_tier2_enrichment.py",
     "MODEL = _settings.gemini_model",
     'MODEL = "gemini-2.5-flash"  # synthesis wants the fuller model, not lite\n',
     "MODEL = _settings.gemini_model  # env: GEMINI_MODEL (synthesis wants full, not lite)\n"),

    ("app/services/news_intel.py",
     "MODEL = _settings.gemini_model",
     "from app.services.ai_client import ai_generate\n",
     "from app.services.ai_client import ai_generate\n"
     "from app.config import settings as _settings\n"),
    ("app/services/news_intel.py",
     "MODEL = _settings.gemini_model",
     'MODEL = "gemini-2.5-flash"\n',
     "MODEL = _settings.gemini_model  # env: GEMINI_MODEL\n"),

    ("app/services/contact_query_parser.py",
     "MODEL = _settings.gemini_model_lite",
     "from app.services.ai_client import ai_generate\n",
     "from app.services.ai_client import ai_generate\n"
     "from app.config import settings as _settings\n"),
    ("app/services/contact_query_parser.py",
     "MODEL = _settings.gemini_model_lite",
     'MODEL = "gemini-2.5-flash-lite"\n',
     "MODEL = _settings.gemini_model_lite  # env: GEMINI_MODEL_LITE\n"),

    # ── role_intelligence default param ─────────────────────────────
    ("app/services/role_intelligence.py",
     "model = model or _settings.gemini_model_lite",
     'async def label_roles_llm(client, items: list[dict], model: str = "gemini-2.5-flash-lite") -> dict:\n'
     '    """items: [{role, org_hint}]. Returns {normalized_role: {vertical, priority,\n'
     '    is_relevant, seniority}}. Best-effort; returns {} on parse failure."""\n'
     "    from app.services.ai_client import ai_generate\n",
     "async def label_roles_llm(client, items: list[dict], model: str | None = None) -> dict:\n"
     '    """items: [{role, org_hint}]. Returns {normalized_role: {vertical, priority,\n'
     '    is_relevant, seniority}}. Best-effort; returns {} on parse failure."""\n'
     "    from app.config import settings as _settings\n"
     "    from app.services.ai_client import ai_generate\n\n"
     "    model = model or _settings.gemini_model_lite\n"),

    # ── inbox_sync: 3 hard-coded lite calls ─────────────────────────
    ("app/services/inbox_sync.py",
     "from app.config import settings as _settings\n",
     "from app.services.ai_client import ai_generate\n",
     "from app.services.ai_client import ai_generate\n"
     "from app.config import settings as _settings\n"),
    ("app/services/inbox_sync.py",
     "model=_settings.gemini_model_lite)\n            except Exception as exc:\n"
     '                logger.debug(f"inbox_sync: org-split',
     'raw = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")\n'
     "            except Exception as exc:\n"
     '                logger.debug(f"inbox_sync: org-split',
     "raw = await ai_generate(client, prompt, model=_settings.gemini_model_lite)\n"
     "            except Exception as exc:\n"
     '                logger.debug(f"inbox_sync: org-split'),
    ("app/services/inbox_sync.py",
     "text_out = await ai_generate(client, prompt, model=_settings.gemini_model_lite)",
     'text_out = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")',
     "text_out = await ai_generate(client, prompt, model=_settings.gemini_model_lite)"),
    ("app/services/inbox_sync.py",
     "model=_settings.gemini_model_lite)\n            except Exception as exc:\n"
     '                logger.debug(f"inbox_sync: name-resolve',
     'raw = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")\n'
     "            except Exception as exc:\n"
     '                logger.debug(f"inbox_sync: name-resolve',
     "raw = await ai_generate(client, prompt, model=_settings.gemini_model_lite)\n"
     "            except Exception as exc:\n"
     '                logger.debug(f"inbox_sync: name-resolve'),

    # ── contact_enrichment fallback chain ───────────────────────────
    ("app/services/contact_enrichment.py",
     '("gemini-3.5-flash", "global"),  # 2. full 3.x',
     '    # ── gemini-2.5-flash fallback — 4 endpoints ──\n'
     '    ("gemini-2.5-flash", "global"),  # 2. older model, still good\n'
     '    ("gemini-2.5-flash", "us-central1"),  # 3. regional\n'
     '    ("gemini-2.5-flash", "us-east4"),  # 4. east coast\n'
     '    ("gemini-2.5-flash", "us-west1"),  # 5. west coast\n'
     "    # ── Lite models — last resort ──\n"
     '    ("gemini-3.1-flash-lite", "global"),  # 6. lite 3.x\n'
     '    ("gemini-2.5-flash-lite", "global"),  # 7. lite 2.x\n'
     '    ("gemini-2.5-flash-lite", "us-central1"),  # 8. absolute last resort\n',
     "    # 2.5 retired on Vertex (Lite Jan 2027, Flash Mar 2027) -- 3.x only.\n"
     "    # 3.x is global-only, so fallback = different model (separate quota pool).\n"
     "    # Duplicates of the env primary are skipped at call time.\n"
     '    ("gemini-3.5-flash", "global"),  # 2. full 3.x\n'
     '    ("gemini-3.1-flash-lite", "global"),  # 3. lite 3.x -- last resort\n'),

    # ── grounded_contact_fill fallback ──────────────────────────────
    ("app/services/grounded_contact_fill.py",
     'for _m in ("gemini-3.5-flash", "gemini-3.1-flash-lite"):',
     'for _m in ("gemini-2.5-flash", "gemini-2.5-flash-lite"):',
     'for _m in ("gemini-3.5-flash", "gemini-3.1-flash-lite"):'),

    # ── lead_data_enrichment hard-coded URL ─────────────────────────
    ("app/services/lead_data_enrichment.py",
     "url = get_gemini_url()  # env: GEMINI_MODEL",
     'url = get_gemini_url("gemini-2.5-flash")',
     "url = get_gemini_url()  # env: GEMINI_MODEL"),

    # ── /health was pinging 2.5 on every check ──────────────────────
    ("app/routes/health.py",
     "url = get_gemini_url()  # env: GEMINI_MODEL",
     'url = get_gemini_url("gemini-2.5-flash")',
     "url = get_gemini_url()  # env: GEMINI_MODEL"),

    # ── outreach agents (LangChain) ─────────────────────────────────
    ("app/services/outreach/config.py",
     "_build_llm(settings.gemini_model, temperature=0.1, max_tokens=8192)",
     '    rationale, which failed JSON parse and silently dropped the score\n'
     '    back to default."""\n'
     '    return _build_llm("gemini-2.5-flash", temperature=0.1, max_tokens=8192)',
     '    rationale, which failed JSON parse and silently dropped the score\n'
     '    back to default."""\n'
     "    return _build_llm(settings.gemini_model, temperature=0.1, max_tokens=8192)"),
    ("app/services/outreach/config.py",
     "_build_llm(settings.gemini_model, temperature=0.1, max_tokens=8192)\n\n\n@lru_cache(maxsize=1)\ndef get_analyst_llm",
     '    return _build_llm("gemini-2.5-flash", temperature=0.1, max_tokens=8192)\n\n\n'
     "@lru_cache(maxsize=1)\ndef get_analyst_llm",
     "    return _build_llm(settings.gemini_model, temperature=0.1, max_tokens=8192)\n\n\n"
     "@lru_cache(maxsize=1)\ndef get_analyst_llm"),
    ("app/services/outreach/config.py",
     "_build_llm(settings.gemini_model, temperature=0.4",
     '_build_llm("gemini-2.5-flash", temperature=0.4',
     "_build_llm(settings.gemini_model, temperature=0.4"),
    ("app/services/outreach/config.py",
     "_build_llm(settings.gemini_model_lite, temperature=0.0, max_tokens=2048)",
     '_build_llm("gemini-2.5-flash-lite", temperature=0.0, max_tokens=2048)',
     "_build_llm(settings.gemini_model_lite, temperature=0.0, max_tokens=2048)"),
    ("app/services/outreach/config.py",
     "_build_llm(settings.gemini_model_lite, temperature=0.0, max_tokens=4096)",
     '_build_llm("gemini-2.5-flash-lite", temperature=0.0, max_tokens=4096)',
     "_build_llm(settings.gemini_model_lite, temperature=0.0, max_tokens=4096)"),

    # ── stale comments/docstrings (keeps future greps clean) ────────
    ("app/services/gemini_classifier.py",
     "model=settings.gemini_model_lite,  # env: GEMINI_MODEL_LITE",
     "model=settings.gemini_model_lite,  # gemini-2.5-flash-lite (higher quota, perfect for classification)",
     "model=settings.gemini_model_lite,  # env: GEMINI_MODEL_LITE"),
    ("app/services/intelligent_pipeline.py",
     "-Classifier: GEMINI_MODEL_LITE",
     "-Classifier: gemini-2.5-flash-lite (4,000 RPM / Unlimited RPD)\n"
     "-Extractor: gemini-2.5-flash (1,000 RPM / 10,000 RPD)",
     "-Classifier: GEMINI_MODEL_LITE (.env)\n"
     "-Extractor: GEMINI_MODEL (.env)"),
]


def main() -> int:
    backed_up: dict[Path, Path] = {}
    touched: set[Path] = set()
    applied = skipped = 0
    failed: list[str] = []

    for rel, marker, old, new in EDITS:
        f = ROOT / rel
        if not f.exists():
            failed.append(f"{rel}: file missing")
            continue
        src = f.read_text(encoding="utf-8")
        if marker in src:
            skipped += 1
            continue
        n = src.count(old)
        if n != 1:
            failed.append(f"{rel}: anchor matched {n}x -> {old.splitlines()[0][:70]!r}")
            continue
        if f not in backed_up:
            bak = f.with_suffix(f.suffix + ".bak")
            shutil.copy2(f, bak)
            backed_up[f] = bak
        f.write_text(src.replace(old, new, 1), encoding="utf-8")
        touched.add(f)
        applied += 1

    compile_fail = False
    for f in sorted(touched):
        try:
            py_compile.compile(str(f), doraise=True)
        except py_compile.PyCompileError as e:
            compile_fail = True
            print(f"[COMPILE FAIL] {f.relative_to(ROOT)}: {e.msg}")

    if compile_fail:
        for f, bak in backed_up.items():
            shutil.copy2(bak, f)
        print("[RESTORED] all touched files from .bak -- nothing changed.")
        return 1

    print(f"[OK] applied={applied} already_applied={skipped} files_touched={len(touched)}")
    for f in sorted(touched):
        print(f"     {f.relative_to(ROOT)}")
    if failed:
        print("[WARN] anchors not found (file drifted?):")
        for m in failed:
            print(f"     {m}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
