"""Fix frontend JS to pass scrape_id/extract_id/discovery_id. Run: python fix_frontend.py"""


def fix(path, old, new, label):
    f = open(path, encoding="utf-8").read()
    if old not in f:
        if new in f:
            print(f"  [DONE]  {label}")
        else:
            print(f"  [SKIP]  {label} - target not found")
        return
    f = f.replace(old, new, 1)
    open(path, "w", encoding="utf-8").write(f)
    print(f"  [FIXED] {label}")


SM = "app/templates/partials/scrape_modal.html"
DM = "app/templates/partials/discovery_modal.html"

print("--- Scrape Modal ---")

# 1. URL Extract: pass extract_id
fix(
    SM,
    """                if (resp.ok && data.status !== 'error') {
                    this.log('success', 'URL extract started');
                    this.connectSSE('/api/dashboard/extract-url/stream');""",
    """                if (resp.ok && data.status !== 'error') {
                    this.log('success', 'URL extract started');
                    this.connectSSE('/api/dashboard/extract-url/stream?extract_id=' + data.extract_id);""",
    "1. URL extract passes extract_id",
)

# 2. Scrape: pass scrape_id
fix(
    SM,
    """                if (resp.ok) {
                    this.log('success', `Scrape queued (${this.scrapeMode} mode)`);
                    this.connectSSE('/api/dashboard/scrape/stream');""",
    """                if (resp.ok) {
                    this.log('success', `Scrape queued (${this.scrapeMode} mode)`);
                    this.connectSSE('/api/dashboard/scrape/stream?scrape_id=' + data.scrape_id);""",
    "2. Scrape passes scrape_id",
)

print("\n--- Discovery Modal ---")

# 3. Discovery: pass discovery_id
fix(
    DM,
    """            this.eventSource = new EventSource('/api/dashboard/discovery/stream');""",
    """            this.eventSource = new EventSource('/api/dashboard/discovery/stream?discovery_id=' + data.discovery_id);""",
    "3. Discovery passes discovery_id",
)

# 3b. Need to capture data in outer scope for discovery
# Currently data is in a try block that ends before the EventSource line
# Move the data variable to be accessible
fix(
    DM,
    """            } catch (e) {
                this.addLog('error', '❌ Failed to start: ' + e.message);
                this.running = false;
                this.hasErrors = true;
                return;
            }

            // 2. Connect to SSE stream
            const startTime = Date.now();
            this.eventSource = new EventSource('/api/dashboard/discovery/stream?discovery_id=' + data.discovery_id);""",
    """            } catch (e) {
                this.addLog('error', '❌ Failed to start: ' + e.message);
                this.running = false;
                this.hasErrors = true;
                return;
            }

            // 2. Connect to SSE stream
            const startTime = Date.now();
            this.eventSource = new EventSource('/api/dashboard/discovery/stream?discovery_id=' + this._discoveryId);""",
    "3b. Discovery uses instance var for ID",
)

# 3c. Store discovery_id on instance inside the try block
fix(
    DM,
    """                const data = await resp.json();
                if (data.status === 'error') {""",
    """                const data = await resp.json();
                this._discoveryId = data.discovery_id || '';
                if (data.status === 'error') {""",
    "3c. Store discovery_id from response",
)

print("\nDone! Run: ruff format app/templates/ or just commit.")
