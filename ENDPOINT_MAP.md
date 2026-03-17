# Smart Lead Hunter — Endpoint Map

> Auto-generated from `/workspace/app/main.py`

---

## Authentication

All `/api/` routes are protected by `APIKeyMiddleware` which checks the `X-API-Key` header (compared against `API_AUTH_KEY` env var). In dev mode with no key configured, auth is disabled.

**Excluded from API key auth:** `/health`, `/dashboard*`, `/docs`, `/redoc`, `/openapi.json`, and three SSE stream endpoints plus three read-only dashboard endpoints (see below).

Endpoints marked with `_require_ajax` also need one of:
- `X-Requested-With: XMLHttpRequest`, **or**
- `HX-Request` header (HTMX), **or**
- `Content-Type: application/json`

---

## 1. Auth Verify

```
GET /api/auth/verify → JSON
  Params: none
  Returns: { "status": "ok" }
  Headers: X-API-Key
```

---

## 2. Stats

```
GET /stats → JSON (StatsResponse)
  Params: none
  Returns: {
    "total_leads": int,
    "new_leads": int,
    "approved_leads": int,
    "pending_leads": int,
    "rejected_leads": int,
    "hot_leads": int,
    "urgent_leads": int,
    "warm_leads": int,
    "cool_leads": int,
    "total_sources": int,
    "active_sources": int,
    "healthy_sources": int,
    "leads_today": int,
    "leads_this_week": int
  }
  Headers: none (not under /api/ prefix, so no API key middleware)
```

---

## 3. Leads List

```
GET /leads → JSON (LeadListResponse)
  Params (all query, all optional):
    page: int          (default 1, min 1)
    per_page: int      (default 20, min 1, max 100)
    status: str|null
    min_score: int|null
    state: str|null
    location_type: str|null
    brand_tier: str|null
    search: str|null
  Returns: {
    "leads": [ LeadResponse, ... ],
    "total": int,
    "page": int,
    "per_page": int,
    "pages": int
  }
  Headers: none (not under /api/ prefix)
```

### LeadResponse shape

```json
{
  "id": int,
  "hotel_name": "str",
  "contact_email": "str|null",
  "contact_phone": "str|null",
  "contact_name": "str|null",
  "contact_title": "str|null",
  "city": "str|null",
  "state": "str|null",
  "country": "str|null (default USA)",
  "opening_date": "str|null",
  "room_count": "int|null",
  "hotel_type": "str|null",
  "brand": "str|null",
  "brand_tier": "str|null",
  "location_type": "str|null",
  "hotel_website": "str|null",
  "description": "str|null",
  "notes": "str|null",
  "lead_score": "int|null",
  "score_breakdown": "dict|null",
  "status": "str",
  "source_url": "str|null",
  "source_site": "str|null",
  "created_at": "datetime",
  "updated_at": "datetime|null"
}
```

---

## 4. Hot Leads

```
GET /leads/hot → JSON (LeadListResponse)
  Params (query):
    page: int       (default 1, min 1)
    per_page: int   (default 20, min 1, max 100)
  Returns: same shape as LeadListResponse above
  Headers: none (not under /api/ prefix)
```

---

## 5. Single Lead

```
GET /leads/{lead_id} → JSON (LeadResponse)
  Params: lead_id (path, int)
  Returns: LeadResponse (see shape above)
  Headers: none (not under /api/ prefix)
  Errors: 404 { "detail": "Lead not found" }
```

---

## 6. Create Lead

```
POST /leads → JSON (LeadResponse)
  Params: JSON body (LeadCreate):
    hotel_name: str (required)
    contact_email, contact_phone, contact_name, contact_title: str|null
    city, state, country, opening_date: str|null
    room_count: int|null
    hotel_type, brand, brand_tier, location_type: str|null
    hotel_website, description, notes: str|null
    lead_score: int|null
    source_url: str|null
    source_site: str|null
  Returns: LeadResponse
  Headers: none (not under /api/ prefix)
  Errors: 422 { "detail": "..." }, 409 { "detail": "A lead with a similar name already exists (ID: ...)" }
```

---

## 7. Update Lead

```
PATCH /leads/{lead_id} → JSON (LeadResponse)
  Params:
    lead_id (path, int)
    JSON body (LeadUpdate, all optional):
      status, contact_email, contact_phone, contact_name, contact_title: str|null
      notes: str|null
      lead_score: int|null
      rejection_reason: str|null
  Returns: LeadResponse
  Headers: none (not under /api/ prefix)
  Errors: 404 { "detail": "Lead not found" }
```

---

## 8. Approve Lead (API)

```
POST /leads/{lead_id}/approve → JSON (LeadResponse)
  Params: lead_id (path, int)
  Returns: LeadResponse
  Headers: none (not under /api/ prefix)
  Errors: 404 { "detail": "Lead not found" }
  Side-effects: sets status="approved", pushes to Insightly CRM
```

---

## 9. Reject Lead (API)

```
POST /leads/{lead_id}/reject → JSON (LeadResponse)
  Params:
    lead_id (path, int)
    reason: str|null (query param)
  Returns: LeadResponse
  Headers: none (not under /api/ prefix)
  Errors: 404 { "detail": "Lead not found" }
  Side-effects: sets status="rejected", stores rejection_reason
```

---

## 10. Delete Lead (API)

```
DELETE /leads/{lead_id} → JSON
  Params: lead_id (path, int)
  Returns: { "message": "Lead deleted", "id": int }
  Headers: none (not under /api/ prefix)
  Errors: 404 { "detail": "Lead not found" }
  Side-effects: hard deletes the lead from DB
```

---

## 11. Dashboard Edit Lead

```
PATCH /api/dashboard/leads/{lead_id}/edit → JSON (JSONResponse)
  Params:
    lead_id (path, int)
    JSON body with any of these editable fields:
      hotel_name, brand, brand_tier, hotel_type, city, state, country,
      opening_date, room_count, management_company, developer, owner,
      contact_name, contact_title, contact_email, contact_phone,
      description, notes
  Returns: {
    "status": "ok",
    "id": int,
    "new_score": int|null,
    "new_tier": str|null
  }
  Headers: X-API-Key + one of (X-Requested-With: XMLHttpRequest | Content-Type: application/json | HX-Request)
  Errors: 404 JSONResponse { "detail": "Lead not found" }
```

---

## 12. Dashboard Approve Lead

```
POST /api/dashboard/leads/{lead_id}/approve → ⚠️ HTMLResponse (response_class=HTMLResponse)
  Params: lead_id (path, int)
  Returns: HTML partial (templates/partials/lead_row.html) — rendered Jinja2 template
           On no-contacts: HTML string '<div class="text-red-600 ...">Enrich first — no contacts to push to CRM</div>'
  Headers: X-API-Key + _require_ajax header
  Errors: 404 HTMLResponse "Lead not found"
  Side-effects: sets status="approved", pushes contacts to Insightly CRM
```

**⚠️ CRITICAL: Returns HTMLResponse, NOT JSON. A React frontend calling this will get raw HTML, not parseable JSON.**

---

## 13. Dashboard Reject Lead

```
POST /api/dashboard/leads/{lead_id}/reject → ⚠️ HTMLResponse (response_class=HTMLResponse)
  Params:
    lead_id (path, int)
    reason: str|null (query param)
  Returns: HTML partial (templates/partials/lead_row.html) — rendered Jinja2 template
  Headers: X-API-Key + _require_ajax header
  Errors: 404 HTMLResponse "Lead not found"
  Side-effects: sets status="rejected", deletes from Insightly if previously pushed
```

**⚠️ CRITICAL: Returns HTMLResponse, NOT JSON.**

---

## 14. Dashboard Restore Lead

```
POST /api/dashboard/leads/{lead_id}/restore → ⚠️ HTMLResponse (response_class=HTMLResponse)
  Params: lead_id (path, int)
  Returns: HTML partial (templates/partials/lead_row.html)
  Headers: X-API-Key + _require_ajax header
  Errors: 404 HTMLResponse "<div class='text-red-500 p-2'>Lead not found</div>"
  Side-effects: sets status="new", clears rejection_reason, deletes from Insightly
```

**⚠️ CRITICAL: Returns HTMLResponse, NOT JSON.**

---

## 15. Dashboard Delete Lead

```
POST /api/dashboard/leads/{lead_id}/delete → ⚠️ HTMLResponse (response_class=HTMLResponse)
  Params: lead_id (path, int)
  Returns: empty HTML string "" (status 200) — HTMX removes the row
  Headers: X-API-Key + _require_ajax header
  Errors: 404 HTMLResponse "<div class='text-red-500 p-2'>Lead not found</div>"
  Side-effects: soft-delete (sets status="deleted")
```

**⚠️ CRITICAL: Returns HTMLResponse (empty body), NOT JSON.**

---

## 16. Sources List

```
GET /api/dashboard/sources/list → JSON (implicit dict)
  Params: none
  Returns: {
    "sources": [
      {
        "id": int,
        "name": str,
        "type": str,           // source_type
        "priority": int,
        "frequency": str,      // scrape_frequency or "daily"
        "health": str,         // health_status or "new"
        "leads": int,          // leads_found or 0
        "gold_count": int,     // active gold URLs (miss_streak < 3)
        "last_scraped": str|null  // ISO datetime
      },
      ...
    ],
    "due_sources": [
      {
        // same fields as above, plus:
        "reason": str,         // e.g. "Never scraped" or "daily (last: 48h ago)"
        "mode": str            // "discover" or "gold"
      },
      ...
    ],
    "categories": [
      { "type": str, "label": str, "count": int },
      ...
    ],
    "total": int,
    "total_due": int
  }
  Headers: none (excluded from API key auth)
```

---

## 17. Trigger Scrape

```
POST /api/dashboard/scrape → JSON
  Params: JSON body (optional):
    mode: str        (default "full")
    source_ids: int[] (default [])
  Returns: {
    "status": "started",
    "message": "Scrape job started ({mode} mode)",
    "scrape_id": str (UUID),
    "mode": str,
    "source_count": int|"all"
  }
  On error: { "status": "error", "message": str }
  Headers: X-API-Key + _require_ajax header
```

---

## 18. Scrape SSE Stream

```
GET /api/dashboard/scrape/stream → SSE (text/event-stream)
  Params (query):
    scrape_id: str (UUID, required — returned from POST /api/dashboard/scrape)
  Returns: Server-Sent Events, each line: data: {JSON}\n\n
    Event types include: { "type": "progress"|"error"|"complete", "message": str, ... }
  Headers: none (excluded from API key auth — gated by one-time scrape_id token)
  Error SSE: { "type": "error", "message": "No scrape config found. Please trigger scrape again." }
```

---

## 19. Cancel Scrape

```
POST /api/dashboard/scrape/cancel/{scrape_id} → JSON
  Params: scrape_id (path, str UUID)
  Returns:
    Found:     { "status": "cancelling", "message": "Cancellation requested" }
    Not found: { "status": "not_found", "message": "Scrape job not found" }
  Headers: X-API-Key + _require_ajax header
```

---

## 20. Extract URL Trigger

```
POST /api/dashboard/extract-url → JSON
  Params: JSON body:
    url: str (required)
  Returns: {
    "status": "started",
    "message": "Extracting leads from URL",
    "url": str,
    "extract_id": str (UUID)
  }
  On error: { "status": "error", "message": str }
  Headers: X-API-Key + _require_ajax header
```

---

## 21. Extract URL SSE Stream

```
GET /api/dashboard/extract-url/stream → SSE (text/event-stream)
  Params (query):
    extract_id: str (UUID, required — returned from POST /api/dashboard/extract-url)
  Returns: Server-Sent Events, each line: data: {JSON}\n\n
  Headers: none (excluded from API key auth — gated by one-time extract_id token)
  Error SSE: { "type": "error", "message": "No URL pending. Please click Extract again." }
```

---

## 22. Discovery Start

```
POST /api/dashboard/discovery/start → JSON
  Params: JSON body:
    mode: str           (default "full")
    extract_leads: bool (default true)
    dry_run: bool       (default false)
  Returns: {
    "status": "started",
    "message": "Discovery started ({mode} mode)",
    "mode": str,
    "discovery_id": str (UUID)
  }
  On error: { "status": "error", "message": str }
  Headers: X-API-Key + _require_ajax header
```

---

## 23. Discovery SSE Stream

```
GET /api/dashboard/discovery/stream → SSE (text/event-stream)
  Params (query):
    discovery_id: str (UUID, required — returned from POST /api/dashboard/discovery/start)
  Returns: Server-Sent Events, each line: data: {JSON}\n\n
  Headers: none (excluded from API key auth — gated by one-time discovery_id token)
```

---

## 24. Enrich Lead

```
POST /api/dashboard/leads/{lead_id}/enrich → JSON
  Params: lead_id (path, int)
  Returns (success): {
    "status": str,                    // from save_result
    "lead_id": int,
    "hotel_name": str,
    "contacts_found": int,
    "best_contact": dict|null,
    "management_company": str|null,
    "developer": str|null,
    "layers_tried": str[],
    "sources_used": str[],
    "updated_fields": str[],
    "errors": str[]
  }
  Returns (already running): {
    "status": "already_running",
    "message": "Enrichment already in progress for this lead"
  }
  Returns (error): { "status": "error", "message": str }
  Headers: X-API-Key + _require_ajax header
```

---

## 25. Contacts CRUD

### List Contacts

```
GET /api/dashboard/leads/{lead_id}/contacts → JSON (array)
  Params: lead_id (path, int)
  Returns: [ ContactDict, ... ]   // ordered by is_saved desc, is_primary desc, score desc
           Each contact via LeadContact.to_dict()
  Headers: none (path starts with /api/dashboard/leads — API key middleware checks startsWith, 
           but this path is NOT in the exclude list, so X-API-Key IS required)
```

### Save Contact

```
POST /api/dashboard/leads/{lead_id}/contacts/{contact_id}/save → JSON
  Params: lead_id (path, int), contact_id (path, int)
  Returns: { "status": "saved", "contact_id": int }
  Headers: X-API-Key + _require_ajax header
  Errors: 404 { "detail": "Contact not found" }
```

### Unsave Contact

```
POST /api/dashboard/leads/{lead_id}/contacts/{contact_id}/unsave → JSON
  Params: lead_id (path, int), contact_id (path, int)
  Returns: { "status": "unsaved", "contact_id": int }
  Headers: X-API-Key + _require_ajax header
  Errors: 404 { "detail": "Contact not found" }
```

### Delete Contact

```
DELETE /api/dashboard/leads/{lead_id}/contacts/{contact_id} → JSON
  Params: lead_id (path, int), contact_id (path, int)
  Returns: { "status": "deleted", "contact_id": int }
  Headers: X-API-Key + _require_ajax header
  Errors: 404 { "detail": "Contact not found" }
  Side-effects: also rescores the lead after deletion
```

### Set Primary Contact

```
POST /api/dashboard/leads/{lead_id}/contacts/{contact_id}/set-primary → JSON
  Params: lead_id (path, int), contact_id (path, int)
  Returns: { "status": "primary_set", "contact_id": int }
  Headers: X-API-Key + _require_ajax header
  Errors: 404 { "detail": "Contact not found" }
  Side-effects: clears is_primary on all other contacts for this lead,
                copies contact fields to lead (contact_name, contact_title, contact_email, contact_phone, contact_linkedin)
```

---

## Summary of the HTML vs JSON Problem

| Endpoint | Path | Returns |
|---|---|---|
| Dashboard Approve | `POST /api/dashboard/leads/{id}/approve` | **HTMLResponse** (Jinja2 partial) |
| Dashboard Reject | `POST /api/dashboard/leads/{id}/reject` | **HTMLResponse** (Jinja2 partial) |
| Dashboard Restore | `POST /api/dashboard/leads/{id}/restore` | **HTMLResponse** (Jinja2 partial) |
| Dashboard Delete | `POST /api/dashboard/leads/{id}/delete` | **HTMLResponse** (empty string) |
| Dashboard Edit | `PATCH /api/dashboard/leads/{id}/edit` | **JSONResponse** (correct) |
| Enrich | `POST /api/dashboard/leads/{id}/enrich` | **JSON dict** (correct) |
| All Contacts CRUD | various | **JSON** (correct) |

**The four dashboard action endpoints (approve/reject/restore/delete) all return `HTMLResponse` with `response_class=HTMLResponse` explicitly set. These are designed for HTMX (`templates.TemplateResponse` rendering `partials/lead_row.html`). A React frontend expecting JSON will receive unparseable HTML strings from these four endpoints.**

The pure API endpoints (`/leads/{id}/approve`, `/leads/{id}/reject`) correctly return `LeadResponse` as JSON — but they live under `/leads/` (no `/api/` prefix), not under `/api/dashboard/`.
