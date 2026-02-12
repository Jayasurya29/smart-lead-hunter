"""
Dashboard Revamp - All-in-One Deployment
Run from: C:/Users/it2/smart-lead-hunter
Usage: python deploy_dashboard.py
"""

import os
import shutil
import glob

print("=" * 60)
print("  SMART LEAD HUNTER - DASHBOARD REVAMP")
print("=" * 60)

# ============================================================
# 1. CREATE STATIC DIRECTORY + COPY LOGO
# ============================================================
print("\n[1/5] Setting up static files...")
os.makedirs("app/static/img", exist_ok=True)

# Find logo file
logo_found = False
for path in glob.glob("**/JA_DAY_L*", recursive=True):
    shutil.copy2(path, "app/static/img/ja-logo.jpg")
    print(f"  ✓ Logo copied from {path}")
    logo_found = True
    break

if not logo_found:
    print(
        "  ⚠ Logo not found - please copy JA_DAY_L.jpg to app/static/img/ja-logo.jpg manually"
    )

# ============================================================
# 2. ADD STATIC MOUNT TO main.py
# ============================================================
print("\n[2/5] Adding static files mount...")
with open("app/main.py", "r", encoding="utf-8") as f:
    main_content = f.read()

if 'app.mount("/static"' not in main_content:
    main_content = main_content.replace(
        '@app.get("/dashboard"',
        '# Static files\napp.mount("/static", StaticFiles(directory="app/static"), name="static")\n\n\n@app.get("/dashboard"',
    )
    print("  ✓ Static mount added")
else:
    print("  ✓ Static mount already exists")

# ============================================================
# 3. UPDATE DASHBOARD ROUTE (add opening_year, sort params)
# ============================================================
print("\n[3/5] Updating dashboard route...")

if "opening_year" not in main_content.split("def dashboard_page")[1].split("\ndef ")[0]:
    # Add params
    main_content = main_content.replace(
        '    tier: str = "",\n    db: AsyncSession = Depends(get_db)\n):\n    """Dashboard page',
        '    tier: str = "",\n    opening_year: str = "",\n    sort: str = "score_desc",\n    db: AsyncSession = Depends(get_db)\n):\n    """Dashboard page',
    )

    # Add filter + sort logic
    old_block = """    if tier:
        query = query.where(PotentialLead.brand_tier == tier)
    # Order
    query = query.order_by(PotentialLead.lead_score.desc().nullslast())"""

    new_block = """    if tier:
        query = query.where(PotentialLead.brand_tier == tier)
    if opening_year:
        if opening_year == '2028':
            query = query.where(
                or_(
                    PotentialLead.opening_year >= 2028,
                    PotentialLead.opening_date.ilike('%2028%'),
                    PotentialLead.opening_date.ilike('%2029%'),
                    PotentialLead.opening_date.ilike('%2030%'),
                )
            )
        else:
            query = query.where(
                or_(
                    PotentialLead.opening_year == int(opening_year),
                    PotentialLead.opening_date.ilike(f'%{opening_year}%'),
                )
            )
    # Sort order
    if sort == 'score_asc':
        query = query.order_by(PotentialLead.lead_score.asc().nullslast())
    elif sort == 'newest':
        query = query.order_by(PotentialLead.created_at.desc().nullslast())
    elif sort == 'oldest':
        query = query.order_by(PotentialLead.created_at.asc().nullslast())
    elif sort == 'name_asc':
        query = query.order_by(PotentialLead.hotel_name.asc())
    elif sort == 'opening':
        query = query.order_by(PotentialLead.opening_date.asc().nullslast())
    else:
        query = query.order_by(PotentialLead.lead_score.desc().nullslast())"""

    main_content = main_content.replace(old_block, new_block)
    print("  ✓ Added opening_year filter + sort options")
else:
    print("  ✓ Route params already exist")

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(main_content)

# ============================================================
# 4. WRITE ALL TEMPLATE FILES
# ============================================================
print("\n[4/5] Writing template files...")

# --- base.html ---
with open("app/templates/base.html", "w", encoding="utf-8") as f:
    f.write(r"""<!DOCTYPE html>
<html lang="en" class="h-full bg-gray-50">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Smart Lead Hunter{% endblock %}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    colors: {
                        'brand': {
                            50: '#f0f4f8', 100: '#d9e2ec', 200: '#bcccdc', 300: '#9fb3c8',
                            400: '#829ab1', 500: '#627d98', 600: '#486581',
                            700: '#334e68', 800: '#243b53', 900: '#102a43',
                        }
                    }
                }
            }
        }
    </script>
    <script src="https://cdn.jsdelivr.net/npm/htmx.org@1.9.10/dist/htmx.min.js"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.3/dist/cdn.min.js"></script>
    <style>
        [x-cloak] { display: none !important; }
        .htmx-request .htmx-indicator { display: inline-block; }
        .htmx-indicator { display: none; }
        .htmx-swapping { opacity: 0; transition: opacity 0.2s ease-out; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
        .lead-row { transition: all 0.15s ease; }
        .lead-row:hover { background-color: #f0f4f8; }
        .lead-row.active { background-color: #e2e8f0; border-left: 3px solid #334e68; }
        @keyframes slideIn { from { transform: translateX(12px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        .slide-in { animation: slideIn 0.25s ease-out; }
        .tier-badge { font-size: 0.65rem; letter-spacing: 0.03em; padding: 2px 6px; border-radius: 4px; font-weight: 600; text-transform: uppercase; }
        .tier-1 { background: #fef3c7; color: #92400e; border: 1px solid #fcd34d; }
        .tier-2 { background: #ede9fe; color: #5b21b6; border: 1px solid #c4b5fd; }
        .tier-3 { background: #dbeafe; color: #1e40af; border: 1px solid #93c5fd; }
        .tier-4 { background: #f1f5f9; color: #475569; border: 1px solid #cbd5e1; }
        .score-hot { background: linear-gradient(135deg, #fef2f2, #fee2e2); border: 2px solid #fca5a5; color: #dc2626; }
        .score-warm { background: linear-gradient(135deg, #fff7ed, #ffedd5); border: 2px solid #fdba74; color: #ea580c; }
        .score-cold { background: #f1f5f9; border: 2px solid #e2e8f0; color: #64748b; }
        .insight-list li { position: relative; padding-left: 1.25rem; margin-bottom: 0.35rem; line-height: 1.5; }
        .insight-list li::before { content: ''; position: absolute; left: 0; top: 0.55em; width: 6px; height: 6px; border-radius: 50%; background: #f59e0b; }
    </style>
</head>
<body class="h-full">
    <div class="min-h-full">
        <nav class="bg-white border-b border-gray-200 shadow-sm">
            <div class="mx-auto max-w-[1440px] px-4 sm:px-6 lg:px-8">
                <div class="flex h-14 items-center justify-between">
                    <div class="flex items-center">
                        <a href="/dashboard" class="flex items-center gap-3 mr-8">
                            <img src="/static/img/ja-logo.jpg" alt="JA Uniforms" class="h-8 w-auto">
                            <div class="hidden sm:block border-l border-gray-200 pl-3">
                                <span class="text-sm font-bold text-brand-800 tracking-tight">Smart Lead Hunter</span>
                            </div>
                        </a>
                        <div class="hidden sm:flex items-center space-x-1">
                            <a href="/dashboard?tab=pipeline"
                               class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {% if not request.query_params.get('score') and not request.query_params.get('location') %}bg-brand-800 text-white{% else %}text-gray-600 hover:bg-gray-100{% endif %}">
                                Dashboard
                            </a>
                            <a href="/dashboard?tab=pipeline&score=hot"
                               class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {% if request.query_params.get('score') == 'hot' %}bg-red-600 text-white{% else %}text-gray-600 hover:bg-gray-100{% endif %}">
                                🔥 Hot Leads
                            </a>
                            <a href="/dashboard?tab=pipeline&location=florida"
                               class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {% if request.query_params.get('location') == 'florida' %}bg-emerald-600 text-white{% else %}text-gray-600 hover:bg-gray-100{% endif %}">
                                🌴 Florida
                            </a>
                            <a href="/dashboard?tab=pipeline&location=caribbean"
                               class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {% if request.query_params.get('location') == 'caribbean' %}bg-cyan-600 text-white{% else %}text-gray-600 hover:bg-gray-100{% endif %}">
                                🏝️ Caribbean
                            </a>
                        </div>
                    </div>
                    <div class="flex items-center">
                        <span class="text-xs text-gray-400">Hotel Lead Intelligence</span>
                    </div>
                </div>
            </div>
        </nav>
        <main class="py-4">{% block content %}{% endblock %}</main>
    </div>
    <div id="toast-container" class="fixed bottom-4 right-4 z-50 space-y-2"></div>
    <script>
        function showToast(message, type = 'success') {
            const container = document.getElementById('toast-container');
            const colors = { success: 'bg-emerald-600', error: 'bg-red-600', info: 'bg-brand-700', warning: 'bg-amber-500' };
            const toast = document.createElement('div');
            toast.className = `${colors[type]} text-white px-5 py-3 rounded-lg shadow-lg text-sm font-medium transform transition-all duration-300`;
            toast.textContent = message;
            container.appendChild(toast);
            setTimeout(() => { toast.classList.add('opacity-0', 'translate-y-2'); setTimeout(() => toast.remove(), 300); }, 3000);
        }
        document.body.addEventListener('htmx:afterSwap', function(evt) {
            const toast = evt.detail.xhr.getResponseHeader('HX-Trigger');
            if (toast) { try { const data = JSON.parse(toast); if (data.showToast) showToast(data.showToast.message, data.showToast.type); } catch (e) {} }
        });
    </script>
</body>
</html>""")
print("  ✓ base.html")

# --- dashboard.html ---
with open("app/templates/dashboard.html", "w", encoding="utf-8") as f:
    f.write(r"""{% extends "base.html" %}
{% block title %}Dashboard - Smart Lead Hunter{% endblock %}
{% block content %}
<div class="min-h-screen bg-gray-50 max-w-[1440px] mx-auto">
    <div id="stats-container" hx-get="/api/dashboard/stats" hx-trigger="load, every 30s" hx-swap="innerHTML">
        <div class="animate-pulse h-20 bg-gray-100 rounded-xl mx-6 mt-4"></div>
    </div>

    <div class="px-6 mt-4">
        <div class="flex items-center justify-between">
            <nav class="flex space-x-1 bg-white rounded-lg shadow-sm p-1 border border-gray-200">
                <a href="/dashboard?tab=pipeline{% if request.query_params.get('score') %}&score={{ request.query_params.get('score') }}{% endif %}"
                   class="px-4 py-2 text-sm font-medium rounded-md transition-all duration-200
                          {% if active_tab == 'pipeline' %}bg-brand-800 text-white shadow{% else %}text-gray-600 hover:text-gray-900 hover:bg-gray-50{% endif %}">
                    📥 Pipeline
                    {% if pipeline_count %}<span class="ml-1.5 px-1.5 py-0.5 text-xs rounded-full {% if active_tab == 'pipeline' %}bg-brand-700 text-white{% else %}bg-gray-200 text-gray-600{% endif %}">{{ pipeline_count }}</span>{% endif %}
                </a>
                <a href="/dashboard?tab=approved"
                   class="px-4 py-2 text-sm font-medium rounded-md transition-all duration-200
                          {% if active_tab == 'approved' %}bg-emerald-600 text-white shadow{% else %}text-gray-600 hover:text-gray-900 hover:bg-gray-50{% endif %}">
                    ✅ Approved
                    {% if approved_count %}<span class="ml-1.5 px-1.5 py-0.5 text-xs rounded-full {% if active_tab == 'approved' %}bg-emerald-500 text-white{% else %}bg-gray-200 text-gray-600{% endif %}">{{ approved_count }}</span>{% endif %}
                </a>
                <a href="/dashboard?tab=rejected"
                   class="px-4 py-2 text-sm font-medium rounded-md transition-all duration-200
                          {% if active_tab == 'rejected' %}bg-red-600 text-white shadow{% else %}text-gray-600 hover:text-gray-900 hover:bg-gray-50{% endif %}">
                    ❌ Rejected
                    {% if rejected_count %}<span class="ml-1.5 px-1.5 py-0.5 text-xs rounded-full {% if active_tab == 'rejected' %}bg-red-500 text-white{% else %}bg-gray-200 text-gray-600{% endif %}">{{ rejected_count }}</span>{% endif %}
                </a>
            </nav>
            <button onclick="window.dispatchEvent(new CustomEvent('open-scrape-modal'))"
                    class="inline-flex items-center px-4 py-2 bg-brand-800 text-white text-sm font-medium rounded-lg hover:bg-brand-900 shadow-sm transition-all">
                <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                </svg>
                Run Scrape Now
            </button>
        </div>
    </div>

    <div class="px-6 mt-4 flex gap-6" x-data="dashboardFilters()">
        <div class="flex-1 min-w-0">
            <div class="bg-white rounded-lg shadow-sm border border-gray-200 p-4 mb-4">
                <form id="filter-form" method="GET" action="/dashboard" class="flex flex-wrap items-end gap-3">
                    <input type="hidden" name="tab" value="{{ active_tab }}">
                    <div class="flex-1 min-w-[200px]">
                        <label class="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Search</label>
                        <div class="relative">
                            <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
                            </svg>
                            <input type="text" name="search" value="{{ request.query_params.get('search', '') }}"
                                   placeholder="Hotel name, city, brand..."
                                   @input.debounce.400ms="submitForm()"
                                   class="w-full pl-10 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-brand-500 focus:border-transparent bg-gray-50 focus:bg-white transition-colors">
                        </div>
                    </div>
                    <div class="w-[130px]">
                        <label class="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Score</label>
                        <select name="score" @change="submitForm()" class="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-brand-500 bg-gray-50">
                            <option value="">All</option>
                            <option value="hot" {% if request.query_params.get('score') == 'hot' %}selected{% endif %}>🔥 Hot (70+)</option>
                            <option value="warm" {% if request.query_params.get('score') == 'warm' %}selected{% endif %}>⚡ Warm (50-69)</option>
                            <option value="cold" {% if request.query_params.get('score') == 'cold' %}selected{% endif %}>❄️ Cold (&lt;50)</option>
                        </select>
                    </div>
                    <div class="w-[130px]">
                        <label class="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Location</label>
                        <select name="location" @change="submitForm()" class="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-brand-500 bg-gray-50">
                            <option value="">All</option>
                            <option value="florida" {% if request.query_params.get('location') == 'florida' %}selected{% endif %}>🌴 Florida</option>
                            <option value="caribbean" {% if request.query_params.get('location') == 'caribbean' %}selected{% endif %}>🏝️ Caribbean</option>
                            <option value="usa" {% if request.query_params.get('location') == 'usa' %}selected{% endif %}>🇺🇸 USA</option>
                        </select>
                    </div>
                    <div class="w-[150px]">
                        <label class="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Tier</label>
                        <select name="tier" @change="submitForm()" class="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-brand-500 bg-gray-50">
                            <option value="">All Tiers</option>
                            <option value="tier1_ultra_luxury" {% if request.query_params.get('tier') == 'tier1_ultra_luxury' %}selected{% endif %}>✨ Ultra Luxury</option>
                            <option value="tier2_luxury" {% if request.query_params.get('tier') == 'tier2_luxury' %}selected{% endif %}>💎 Luxury</option>
                            <option value="tier3_upper_upscale" {% if request.query_params.get('tier') == 'tier3_upper_upscale' %}selected{% endif %}>⭐ Upper Upscale</option>
                            <option value="tier4_upscale" {% if request.query_params.get('tier') == 'tier4_upscale' %}selected{% endif %}>🏨 Upscale</option>
                        </select>
                    </div>
                    <div class="w-[110px]">
                        <label class="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Opening</label>
                        <select name="opening_year" @change="submitForm()" class="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-brand-500 bg-gray-50">
                            <option value="">All Years</option>
                            <option value="2025" {% if request.query_params.get('opening_year') == '2025' %}selected{% endif %}>2025</option>
                            <option value="2026" {% if request.query_params.get('opening_year') == '2026' %}selected{% endif %}>2026</option>
                            <option value="2027" {% if request.query_params.get('opening_year') == '2027' %}selected{% endif %}>2027</option>
                            <option value="2028" {% if request.query_params.get('opening_year') == '2028' %}selected{% endif %}>2028+</option>
                        </select>
                    </div>
                    <div class="w-[140px]">
                        <label class="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Sort By</label>
                        <select name="sort" @change="submitForm()" class="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-brand-500 bg-gray-50">
                            <option value="score_desc" {% if request.query_params.get('sort', 'score_desc') == 'score_desc' %}selected{% endif %}>Score ↓</option>
                            <option value="score_asc" {% if request.query_params.get('sort') == 'score_asc' %}selected{% endif %}>Score ↑</option>
                            <option value="newest" {% if request.query_params.get('sort') == 'newest' %}selected{% endif %}>Newest First</option>
                            <option value="oldest" {% if request.query_params.get('sort') == 'oldest' %}selected{% endif %}>Oldest First</option>
                            <option value="name_asc" {% if request.query_params.get('sort') == 'name_asc' %}selected{% endif %}>Name A-Z</option>
                            <option value="opening" {% if request.query_params.get('sort') == 'opening' %}selected{% endif %}>Opening Date</option>
                        </select>
                    </div>
                    <a href="/dashboard?tab={{ active_tab }}" class="px-3 py-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors" title="Reset filters">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
                        </svg>
                    </a>
                </form>
            </div>

            <div class="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
                    <div>
                        <span class="text-sm font-bold text-gray-800">
                            {% if active_tab == 'pipeline' %}📥 Pipeline{% elif active_tab == 'approved' %}✅ Approved{% else %}❌ Rejected{% endif %}
                        </span>
                        <span class="text-xs text-gray-400 ml-2">({{ leads|length }} total)</span>
                    </div>
                    {% if leads|length > 0 %}<span class="text-xs text-gray-400">Click row for details</span>{% endif %}
                </div>
                <div id="leads-table" class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-gray-100">
                        <thead class="bg-gray-50">
                            <tr>
                                <th class="px-3 py-2.5 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-16">Score</th>
                                <th class="px-3 py-2.5 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">Hotel</th>
                                <th class="px-3 py-2.5 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-24">Tier</th>
                                <th class="px-3 py-2.5 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">Location</th>
                                <th class="px-3 py-2.5 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-28">Opening</th>
                                {% if active_tab == 'pipeline' %}
                                <th class="px-3 py-2.5 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-16">Src</th>
                                {% endif %}
                                <th class="px-3 py-2.5 text-center text-xs font-bold text-gray-500 uppercase tracking-wider w-24">Actions</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-gray-50">
                            {% for lead in leads %}{% include "partials/lead_row.html" %}{% else %}
                            <tr><td colspan="7" class="px-6 py-12 text-center text-gray-400">
                                <p class="text-sm font-medium">No leads found</p>
                                <p class="text-xs mt-1">Try adjusting your filters or run a new scrape</p>
                            </td></tr>{% endfor %}
                        </tbody>
                    </table>
                </div>
                {% if total_pages and total_pages > 1 %}
                <div class="px-4 py-3 border-t border-gray-100 flex items-center justify-between bg-gray-50">
                    <span class="text-xs text-gray-500">Page {{ current_page }} of {{ total_pages }}</span>
                    <div class="flex gap-1">
                        {% for p in range(1, total_pages + 1) %}
                        <a href="/dashboard?tab={{ active_tab }}&page={{ p }}{% if request.query_params.get('search') %}&search={{ request.query_params.get('search') }}{% endif %}{% if request.query_params.get('score') %}&score={{ request.query_params.get('score') }}{% endif %}{% if request.query_params.get('location') %}&location={{ request.query_params.get('location') }}{% endif %}{% if request.query_params.get('tier') %}&tier={{ request.query_params.get('tier') }}{% endif %}{% if request.query_params.get('opening_year') %}&opening_year={{ request.query_params.get('opening_year') }}{% endif %}{% if request.query_params.get('sort') %}&sort={{ request.query_params.get('sort') }}{% endif %}"
                           class="px-2.5 py-1 text-xs rounded {% if p == current_page %}bg-brand-800 text-white{% else %}bg-gray-100 text-gray-600 hover:bg-gray-200{% endif %}">{{ p }}</a>
                        {% endfor %}
                    </div>
                </div>
                {% endif %}
            </div>
        </div>

        <div class="w-[420px] flex-shrink-0">
            <div id="detail-panel" class="sticky top-4">
                <div class="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center">
                    <svg class="mx-auto h-10 w-10 text-gray-300 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>
                    </svg>
                    <p class="text-sm font-medium text-gray-500">Select a lead to view details</p>
                    <p class="text-xs text-gray-400 mt-1">Click any row in the table</p>
                </div>
            </div>
        </div>
    </div>
</div>
{% include "partials/scrape_modal.html" %}
<script>
function dashboardFilters() {
    return {
        submitForm() { document.getElementById('filter-form').submit(); },
        clearSearch() { document.querySelector('input[name="search"]').value = ''; this.submitForm(); }
    }
}
document.addEventListener('htmx:afterSwap', function(e) {
    if (e.detail.target.id === 'detail-panel') {
        document.querySelectorAll('tr[data-lead-row]').forEach(r => r.classList.remove('active'));
        const trigger = e.detail.requestConfig?.elt;
        if (trigger) trigger.classList.add('active');
    }
});
</script>
{% endblock %}""")
print("  ✓ dashboard.html")

# --- lead_row.html ---
with open("app/templates/partials/lead_row.html", "w", encoding="utf-8") as f:
    f.write(r"""<tr id="lead-row-{{ lead.id }}" data-lead-row
    hx-get="/api/dashboard/leads/{{ lead.id }}" hx-target="#detail-panel" hx-swap="innerHTML"
    class="lead-row cursor-pointer transition-colors duration-150">
    <td class="px-3 py-3 whitespace-nowrap">
        {% if lead.lead_score and lead.lead_score >= 70 %}
        <span class="inline-flex items-center justify-center w-10 h-10 rounded-full text-sm font-bold score-hot">{{ lead.lead_score }}</span>
        {% elif lead.lead_score and lead.lead_score >= 50 %}
        <span class="inline-flex items-center justify-center w-10 h-10 rounded-full text-sm font-bold score-warm">{{ lead.lead_score }}</span>
        {% elif lead.lead_score %}
        <span class="inline-flex items-center justify-center w-10 h-10 rounded-full text-sm font-bold score-cold">{{ lead.lead_score }}</span>
        {% else %}
        <span class="inline-flex items-center justify-center w-10 h-10 rounded-full text-sm font-bold bg-gray-50 text-gray-400">?</span>
        {% endif %}
    </td>
    <td class="px-3 py-3">
        <div class="text-sm font-semibold text-gray-900 truncate max-w-[220px]" title="{{ lead.hotel_name }}">{{ lead.hotel_name }}</div>
        {% if lead.brand %}<div class="text-xs text-gray-500 truncate max-w-[220px]">{{ lead.brand }}</div>{% endif %}
    </td>
    <td class="px-3 py-3 whitespace-nowrap">
        {% if lead.brand_tier == 'tier1_ultra_luxury' %}<span class="tier-badge tier-1">Ultra Lux</span>
        {% elif lead.brand_tier == 'tier2_luxury' %}<span class="tier-badge tier-2">Luxury</span>
        {% elif lead.brand_tier == 'tier3_upper_upscale' %}<span class="tier-badge tier-3">Upper Ups</span>
        {% elif lead.brand_tier == 'tier4_upscale' %}<span class="tier-badge tier-4">Upscale</span>
        {% else %}<span class="tier-badge" style="background:#f8fafc;color:#94a3b8;border:1px solid #e2e8f0;">—</span>{% endif %}
    </td>
    <td class="px-3 py-3 whitespace-nowrap">
        <div class="text-sm text-gray-900">{{ lead.city or 'Unknown' }}{% if lead.state %}, {{ lead.state }}{% endif %}</div>
        {% if lead.location_type %}
        <span class="inline-flex items-center mt-0.5 px-1.5 py-0.5 rounded text-xs font-medium
            {% if lead.location_type == 'florida' %}bg-emerald-50 text-emerald-700 border border-emerald-200
            {% elif lead.location_type == 'caribbean' %}bg-cyan-50 text-cyan-700 border border-cyan-200
            {% else %}bg-gray-50 text-gray-600 border border-gray-200{% endif %}">
            {% if lead.location_type == 'florida' %}🌴{% elif lead.location_type == 'caribbean' %}🏝️{% endif %} {{ lead.location_type | title }}
        </span>{% endif %}
    </td>
    <td class="px-3 py-3 whitespace-nowrap text-sm text-gray-600">{{ lead.opening_date or lead.opening_year or 'TBD' }}</td>
    {% if lead.status == 'new' %}
    <td class="px-3 py-3 whitespace-nowrap text-center">
        {% set source_count = (lead.source_urls | length) if lead.source_urls else 1 %}
        {% if source_count > 1 %}<span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold bg-purple-100 text-purple-700">{{ source_count }}x</span>
        {% else %}<span class="text-xs text-gray-400">1</span>{% endif %}
    </td>
    {% endif %}
    <td class="px-3 py-3 whitespace-nowrap text-center" onclick="event.stopPropagation()">
        {% if lead.status == 'new' %}
        <div class="flex items-center justify-center space-x-1">
            <button hx-post="/api/dashboard/leads/{{ lead.id }}/approve" hx-target="#lead-row-{{ lead.id }}" hx-swap="outerHTML"
                    class="p-1.5 rounded-md text-emerald-600 hover:bg-emerald-50 transition-colors" title="Approve">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
            </button>
            <button hx-post="/api/dashboard/leads/{{ lead.id }}/reject" hx-target="#lead-row-{{ lead.id }}" hx-swap="outerHTML"
                    class="p-1.5 rounded-md text-red-500 hover:bg-red-50 transition-colors" title="Reject">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
            </button>
        </div>
        {% elif lead.status == 'approved' %}
        <span class="text-emerald-600"><svg class="w-4 h-4 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg></span>
        {% elif lead.status == 'rejected' %}
        <button hx-post="/api/dashboard/leads/{{ lead.id }}/restore" hx-target="#lead-row-{{ lead.id }}" hx-swap="outerHTML"
                class="p-1.5 rounded-md text-blue-600 hover:bg-blue-50 transition-colors" title="Restore">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h10a8 8 0 018 8v2M3 10l6 6m-6-6l6-6"/></svg>
        </button>
        {% endif %}
    </td>
</tr>""")
print("  ✓ lead_row.html")

# --- stats.html ---
with open("app/templates/partials/stats.html", "w", encoding="utf-8") as f:
    f.write(r"""<div class="px-6">
    <div class="flex items-center justify-between mb-3">
        <h2 class="text-base font-bold text-gray-800">Dashboard Overview</h2>
    </div>
    <div class="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <div class="bg-white rounded-lg border border-gray-200 shadow-sm p-3">
            <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-gray-100 flex items-center justify-center flex-shrink-0">
                    <svg class="h-5 w-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"/></svg>
                </div>
                <div><div class="text-xl font-bold text-gray-900 leading-none">{{ stats.total_leads }}</div><div class="text-xs text-gray-500 mt-0.5">Total Leads</div></div>
            </div>
        </div>
        <div class="bg-white rounded-lg border border-red-200 shadow-sm p-3">
            <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-red-50 flex items-center justify-center flex-shrink-0">
                    <svg class="h-5 w-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 18.657A8 8 0 016.343 7.343S7 9 9 10c0-2 .5-5 2.986-7C14 5 16.09 5.777 17.656 7.343A7.975 7.975 0 0120 13a7.975 7.975 0 01-2.343 5.657z"/></svg>
                </div>
                <div><div class="text-xl font-bold text-red-600 leading-none">{{ stats.hot_leads }}</div><div class="text-xs text-gray-500 mt-0.5">Hot (70+)</div></div>
            </div>
        </div>
        <div class="bg-white rounded-lg border border-orange-200 shadow-sm p-3">
            <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-orange-50 flex items-center justify-center flex-shrink-0">
                    <svg class="h-5 w-5 text-orange-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
                </div>
                <div><div class="text-xl font-bold text-orange-600 leading-none">{{ stats.warm_leads }}</div><div class="text-xs text-gray-500 mt-0.5">Warm (50-69)</div></div>
            </div>
        </div>
        <div class="bg-white rounded-lg border border-blue-200 shadow-sm p-3">
            <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0">
                    <svg class="h-5 w-5 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6"/></svg>
                </div>
                <div><div class="text-xl font-bold text-blue-600 leading-none">{{ stats.new_leads }}</div><div class="text-xs text-gray-500 mt-0.5">New</div></div>
            </div>
        </div>
        <div class="bg-white rounded-lg border border-emerald-200 shadow-sm p-3">
            <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-emerald-50 flex items-center justify-center flex-shrink-0">
                    <svg class="h-5 w-5 text-emerald-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                </div>
                <div><div class="text-xl font-bold text-emerald-600 leading-none">{{ stats.approved_leads }}</div><div class="text-xs text-gray-500 mt-0.5">Approved</div></div>
            </div>
        </div>
        <div class="bg-white rounded-lg border border-gray-200 shadow-sm p-3">
            <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-gray-100 flex items-center justify-center flex-shrink-0">
                    <svg class="h-5 w-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
                </div>
                <div><div class="text-xl font-bold text-gray-900 leading-none">{{ stats.leads_this_week }}</div><div class="text-xs text-gray-500 mt-0.5">This Week</div></div>
            </div>
        </div>
    </div>
</div>""")
print("  ✓ stats.html")

# --- lead_detail.html ---
with open("app/templates/partials/lead_detail.html", "w", encoding="utf-8") as f:
    f.write(r"""<div class="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden slide-in" x-data="leadEditor({{ lead.id }})">
    <div class="px-4 py-3 {% if lead.lead_score and lead.lead_score >= 70 %}bg-gradient-to-r from-red-50 to-orange-50 border-b border-red-100{% elif lead.lead_score and lead.lead_score >= 50 %}bg-gradient-to-r from-amber-50 to-orange-50 border-b border-orange-100{% else %}bg-gradient-to-r from-gray-50 to-white border-b border-gray-200{% endif %}">
        <div class="flex items-start justify-between">
            <div class="flex-1 min-w-0 pr-3">
                <h3 class="text-sm font-bold text-gray-900 leading-tight">{{ lead.hotel_name }}</h3>
                {% if lead.brand %}<p class="text-xs text-gray-500 mt-0.5">{{ lead.brand }}</p>{% endif %}
            </div>
            {% if lead.lead_score and lead.lead_score >= 70 %}
            <span class="flex-shrink-0 px-3 py-1.5 rounded-lg text-sm font-bold score-hot">{{ lead.lead_score }}</span>
            {% elif lead.lead_score and lead.lead_score >= 50 %}
            <span class="flex-shrink-0 px-3 py-1.5 rounded-lg text-sm font-bold score-warm">{{ lead.lead_score }}</span>
            {% else %}
            <span class="flex-shrink-0 px-3 py-1.5 rounded-lg text-sm font-bold score-cold">{{ lead.lead_score or '?' }}</span>
            {% endif %}
        </div>
        <div class="flex items-center mt-2 gap-2 flex-wrap">
            <span class="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-semibold
                {% if lead.status == 'new' %}bg-blue-100 text-blue-700{% elif lead.status == 'approved' %}bg-emerald-100 text-emerald-700{% elif lead.status == 'rejected' %}bg-red-100 text-red-700{% else %}bg-gray-100 text-gray-600{% endif %}">{{ lead.status | title }}</span>
            {% if lead.brand_tier == 'tier1_ultra_luxury' %}<span class="tier-badge tier-1">Ultra Luxury</span>
            {% elif lead.brand_tier == 'tier2_luxury' %}<span class="tier-badge tier-2">Luxury</span>
            {% elif lead.brand_tier == 'tier3_upper_upscale' %}<span class="tier-badge tier-3">Upper Upscale</span>
            {% elif lead.brand_tier == 'tier4_upscale' %}<span class="tier-badge tier-4">Upscale</span>{% endif %}
            {% if lead.source_urls and lead.source_urls | length > 1 %}<span class="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-700">{{ lead.source_urls | length }} sources</span>{% endif %}
            <span class="text-xs text-gray-400 ml-auto">ID: {{ lead.id }}</span>
        </div>
    </div>
    <div class="max-h-[calc(100vh-320px)] overflow-y-auto">
        <div class="px-4 pt-3 flex justify-end">
            <button @click="editing = !editing" class="text-xs px-2.5 py-1 rounded-md transition-colors"
                    :class="editing ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'">
                <span x-show="!editing">✏️ Edit</span><span x-show="editing">🔒 Lock</span>
            </button>
        </div>
        <form id="lead-edit-form" class="px-4 py-2 space-y-3 text-sm">
            {% if lead.description %}
            <div class="bg-amber-50 border border-amber-200 rounded-lg p-3" x-data="{ expanded: false }">
                <div class="flex items-center justify-between mb-2">
                    <h4 class="text-xs font-bold text-amber-800 uppercase tracking-wider flex items-center">
                        <svg class="w-3.5 h-3.5 mr-1 text-amber-500" fill="currentColor" viewBox="0 0 20 20"><path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z"/></svg>
                        Key Insights
                    </h4>
                    <button @click="expanded = !expanded" class="text-xs text-amber-600 hover:text-amber-800 font-medium">
                        <span x-show="!expanded">Show more ▼</span><span x-show="expanded">Show less ▲</span>
                    </button>
                </div>
                <ul class="insight-list text-xs text-gray-700" :class="{ 'max-h-[100px] overflow-hidden': !expanded }">
                    {% for line in lead.description.replace('\r\n', '\n').replace('\r', '\n').split('\n') %}
                    {% set clean = line.strip().lstrip('•-*·▸► ').strip() %}
                    {% if clean %}<li>{{ clean }}</li>{% endif %}
                    {% endfor %}
                </ul>
                <div x-show="!expanded" class="bg-gradient-to-t from-amber-50 to-transparent h-6 -mt-6 relative z-10 pointer-events-none"></div>
            </div>
            {% endif %}
            <fieldset class="space-y-2">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Hotel Info</legend>
                <div>
                    <label class="text-xs text-gray-500">Name</label>
                    <input name="hotel_name" value="{{ lead.hotel_name or '' }}" :disabled="!editing"
                           class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md transition-colors"
                           :class="editing ? 'border-blue-300 bg-white focus:ring-2 focus:ring-blue-500' : 'border-transparent bg-gray-50'">
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <div><label class="text-xs text-gray-500">Brand</label>
                        <input name="brand" value="{{ lead.brand or '' }}" :disabled="!editing"
                               class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md transition-colors"
                               :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">Type</label>
                        <input name="hotel_type" value="{{ lead.hotel_type or '' }}" :disabled="!editing"
                               class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md transition-colors"
                               :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                </div>
                <div><label class="text-xs text-gray-500">Tier</label>
                    <select name="brand_tier" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md transition-colors"
                            :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'">
                        <option value="">—</option>
                        <option value="tier1_ultra_luxury" {% if lead.brand_tier == 'tier1_ultra_luxury' %}selected{% endif %}>Tier 1 — Ultra Luxury</option>
                        <option value="tier2_luxury" {% if lead.brand_tier == 'tier2_luxury' %}selected{% endif %}>Tier 2 — Luxury</option>
                        <option value="tier3_upper_upscale" {% if lead.brand_tier == 'tier3_upper_upscale' %}selected{% endif %}>Tier 3 — Upper Upscale</option>
                        <option value="tier4_upscale" {% if lead.brand_tier == 'tier4_upscale' %}selected{% endif %}>Tier 4 — Upscale</option>
                        <option value="tier5_skip" {% if lead.brand_tier == 'tier5_skip' %}selected{% endif %}>Tier 5 — Skip</option>
                    </select>
                </div>
            </fieldset>
            <fieldset class="space-y-2 pt-2 border-t border-gray-100">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Location</legend>
                <div class="grid grid-cols-3 gap-2">
                    <div><label class="text-xs text-gray-500">City</label><input name="city" value="{{ lead.city or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">State</label><input name="state" value="{{ lead.state or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">Country</label><input name="country" value="{{ lead.country or 'USA' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                </div>
            </fieldset>
            <fieldset class="space-y-2 pt-2 border-t border-gray-100">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Opening Details</legend>
                <div class="grid grid-cols-2 gap-2">
                    <div><label class="text-xs text-gray-500">Opening Date</label><input name="opening_date" value="{{ lead.opening_date or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">Rooms</label><input name="room_count" type="number" value="{{ lead.room_count or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                </div>
                {% if lead.estimated_revenue %}<div><label class="text-xs text-gray-500">Est. Revenue</label><p class="mt-0.5 text-sm font-bold text-emerald-600">${{ "{:,}".format(lead.estimated_revenue) }}</p></div>{% endif %}
            </fieldset>
            <fieldset class="space-y-2 pt-2 border-t border-gray-100">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Stakeholders</legend>
                <div><label class="text-xs text-gray-500">Management Company</label><input name="management_company" value="{{ lead.management_company or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                <div class="grid grid-cols-2 gap-2">
                    <div><label class="text-xs text-gray-500">Developer</label><input name="developer" value="{{ lead.developer or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">Owner</label><input name="owner" value="{{ lead.owner or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                </div>
            </fieldset>
            <fieldset class="space-y-2 pt-2 border-t border-gray-100">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Contact</legend>
                <div class="grid grid-cols-2 gap-2">
                    <div><label class="text-xs text-gray-500">Name</label><input name="contact_name" value="{{ lead.contact_name or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">Title</label><input name="contact_title" value="{{ lead.contact_title or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <div><label class="text-xs text-gray-500">Email</label><input name="contact_email" type="email" value="{{ lead.contact_email or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                    <div><label class="text-xs text-gray-500">Phone</label><input name="contact_phone" type="tel" value="{{ lead.contact_phone or '' }}" :disabled="!editing" class="w-full mt-0.5 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'"></div>
                </div>
            </fieldset>
            <fieldset class="pt-2 border-t border-gray-100" x-show="editing">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Key Insights (Raw Edit)</legend>
                <textarea name="description" rows="4" class="w-full mt-1 px-2 py-1.5 text-sm border border-blue-300 bg-white rounded-md">{{ lead.description or '' }}</textarea>
            </fieldset>
            <fieldset class="pt-2 border-t border-gray-100">
                <legend class="text-xs font-bold text-gray-400 uppercase tracking-wider">Notes</legend>
                <textarea name="notes" rows="2" placeholder="Add internal notes..." :disabled="!editing"
                          class="w-full mt-1 px-2 py-1.5 text-sm border rounded-md" :class="editing ? 'border-blue-300 bg-white' : 'border-transparent bg-gray-50'">{{ lead.notes or '' }}</textarea>
            </fieldset>
            <div x-show="editing" class="pt-2">
                <button type="button" @click="saveLead()" class="w-full px-4 py-2 bg-brand-800 text-white text-sm font-medium rounded-lg hover:bg-brand-900 transition-colors flex items-center justify-center">
                    <span x-show="!saving">💾 Save Changes</span><span x-show="saving">Saving...</span>
                </button>
                <p x-show="saveMsg" x-text="saveMsg" class="text-xs text-center mt-1" :class="saveError ? 'text-red-500' : 'text-emerald-600'"></p>
            </div>
        </form>
        {% if lead.source_urls and lead.source_urls | length > 0 %}
        <div class="px-4 py-3 border-t border-gray-200 bg-gray-50">
            <h4 class="text-xs font-bold text-gray-400 uppercase tracking-wider mb-2">🔗 Sources ({{ lead.source_urls | length }})</h4>
            <div class="space-y-1.5">
                {% for url in lead.source_urls %}
                <div class="bg-white rounded border border-gray-200 px-2.5 py-2" x-data="{ open: false }">
                    <div class="flex items-center justify-between">
                        <a href="{{ url }}" target="_blank" class="text-xs text-blue-600 hover:underline truncate flex-1 pr-2">{{ url | truncate(55) }}</a>
                        {% if lead.source_extractions and lead.source_extractions.get(url) %}
                        <button @click="open = !open" class="text-xs text-gray-400 hover:text-gray-600"><span x-show="!open">▶</span><span x-show="open">▼</span></button>
                        {% endif %}
                    </div>
                    {% if lead.source_extractions and lead.source_extractions.get(url) %}
                    {% set ext = lead.source_extractions[url] %}
                    <div x-show="open" x-collapse class="mt-2 pt-2 border-t border-gray-100 grid grid-cols-2 gap-1 text-xs">
                        {% if ext.get('hotel_name') %}<div><span class="text-gray-400">Name:</span> {{ ext.hotel_name }}</div>{% endif %}
                        {% if ext.get('city') %}<div><span class="text-gray-400">City:</span> {{ ext.city }}</div>{% endif %}
                        {% if ext.get('room_count') %}<div><span class="text-gray-400">Rooms:</span> {{ ext.room_count }}</div>{% endif %}
                        {% if ext.get('opening_date') %}<div><span class="text-gray-400">Opening:</span> {{ ext.opening_date }}</div>{% endif %}
                    </div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
        </div>
        {% elif lead.source_url %}
        <div class="px-4 py-3 border-t border-gray-200 bg-gray-50">
            <h4 class="text-xs font-bold text-gray-400 uppercase tracking-wider mb-2">Source</h4>
            <div class="text-xs text-gray-600 font-medium">🔗 {{ lead.source_site or 'Unknown' }}</div>
            <a href="{{ lead.source_url }}" target="_blank" class="text-xs text-blue-600 hover:underline break-all">{{ lead.source_url }}</a>
        </div>
        {% endif %}
        <div class="px-4 py-2 text-xs text-gray-400 border-t border-gray-100">Added: {{ lead.created_at.strftime('%b %d, %Y %I:%M %p') if lead.created_at else '—' }}</div>
    </div>
    <div class="px-4 py-3 bg-gray-50 border-t border-gray-200">
        {% if lead.status == 'new' %}
        <div class="flex space-x-2">
            <button hx-post="/api/dashboard/leads/{{ lead.id }}/approve" hx-swap="none" hx-on::after-request="window.location.reload()"
                    class="flex-1 inline-flex justify-center items-center px-3 py-2.5 text-sm font-semibold rounded-lg text-white bg-emerald-600 hover:bg-emerald-700 shadow-sm">✅ Approve</button>
            <button hx-post="/api/dashboard/leads/{{ lead.id }}/reject" hx-swap="none" hx-on::after-request="window.location.reload()"
                    class="flex-1 inline-flex justify-center items-center px-3 py-2.5 border border-gray-300 text-sm font-semibold rounded-lg text-gray-600 bg-white hover:bg-gray-50">❌ Reject</button>
        </div>
        {% elif lead.status == 'approved' %}
        <div class="flex items-center justify-between">
            <span class="text-emerald-600 text-sm font-medium">✅ Approved</span>
            <button hx-post="/api/dashboard/leads/{{ lead.id }}/restore" hx-swap="none" hx-on::after-request="window.location.reload()" class="text-xs text-gray-400 hover:text-gray-600 underline">Move back</button>
        </div>
        {% elif lead.status == 'rejected' %}
        <div class="flex items-center justify-between">
            <span class="text-red-500 text-sm font-medium">❌ Rejected{% if lead.rejection_reason %} <span class="text-gray-400 text-xs">({{ lead.rejection_reason }})</span>{% endif %}</span>
            <button hx-post="/api/dashboard/leads/{{ lead.id }}/restore" hx-swap="none" hx-on::after-request="window.location.reload()" class="text-xs text-blue-500 hover:text-blue-700 underline">Restore</button>
        </div>
        {% endif %}
    </div>
</div>
<script>
function leadEditor(leadId) {
    return {
        editing: false, saving: false, saveMsg: '', saveError: false,
        async saveLead() {
            this.saving = true; this.saveMsg = '';
            const form = document.getElementById('lead-edit-form');
            const formData = new FormData(form);
            const data = {};
            for (const [key, value] of formData.entries()) {
                data[key] = key === 'room_count' ? (value ? parseInt(value) : null) : (value || null);
            }
            try {
                const resp = await fetch(`/api/dashboard/leads/${leadId}/edit`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
                if (resp.ok) { this.saveMsg = '✅ Saved!'; this.saveError = false; this.editing = false;
                    htmx.ajax('GET', `/api/dashboard/leads/${leadId}/row`, {target: `#lead-row-${leadId}`, swap: 'outerHTML'});
                } else { const err = await resp.json(); this.saveMsg = '❌ ' + (err.detail || 'Save failed'); this.saveError = true; }
            } catch (e) { this.saveMsg = '❌ Network error'; this.saveError = true; }
            this.saving = false; setTimeout(() => { this.saveMsg = ''; }, 3000);
        }
    }
}
</script>""")
print("  ✓ lead_detail.html")

# ============================================================
# 5. VERIFY
# ============================================================
print("\n[5/5] Verifying deployment...")
files_ok = True
for path in [
    "app/static/img/ja-logo.jpg",
    "app/templates/base.html",
    "app/templates/dashboard.html",
    "app/templates/partials/lead_row.html",
    "app/templates/partials/lead_detail.html",
    "app/templates/partials/stats.html",
]:
    exists = os.path.exists(path)
    status = "✓" if exists else "✗ MISSING"
    if not exists:
        files_ok = False
    print(f"  {status} {path}")

# Check main.py has the mount
with open("app/main.py", "r") as f:
    content = f.read()
    has_mount = 'app.mount("/static"' in content
    has_opening = "opening_year" in content
    has_sort = "sort == 'score_asc'" in content
    print(f"  {'✓' if has_mount else '✗'} Static mount in main.py")
    print(f"  {'✓' if has_opening else '✗'} Opening year filter")
    print(f"  {'✓' if has_sort else '✗'} Sort options")

print("\n" + "=" * 60)
if files_ok and has_mount and has_opening:
    print("  ✅ DEPLOYMENT COMPLETE!")
    print("  Restart your server and refresh the dashboard.")
else:
    print("  ⚠ Some issues detected - check above")
print("=" * 60)
print("\nChanges made:")
print("  • JA Uniforms logo in navbar")
print("  • Key Insights: bulleted, expandable, no cropping")
print("  • Live search (type-ahead, no Enter needed)")
print("  • New filters: Opening Year, Sort By")
print("  • Tier badge column in lead table")
print("  • Cleaner visual design throughout")
print("  • Score badges with gradient styling")
print("  • Static file serving enabled")
