"""
SMART LEAD HUNTER - SOURCE LEARNING SYSTEM
===========================================
Automatically tests each source, learns what works, and improves over time.

PHILOSOPHY:
- Test every source systematically
- Track what URL patterns produce leads
- Learn from successes and failures
- Continuously optimize

This system will:
1. Test each source with broad settings
2. Analyze which URLs produced leads vs junk
3. Auto-generate optimal patterns
4. Save learnings to a database
5. Improve with every run

Last Updated: January 2026
"""

import json
import os
import re
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from urllib.parse import urlparse
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class URLAnalysis:
    """Analysis of a single URL's performance"""
    url: str
    domain: str
    path_pattern: str           # Generalized pattern (e.g., /news/\d+/)
    scraped_at: datetime
    produced_lead: bool
    lead_quality: Optional[float] = None  # 0-1 confidence score
    lead_location: Optional[str] = None   # USA, Caribbean, International
    response_time_ms: int = 0
    content_length: int = 0


@dataclass
class SourceLearning:
    """Accumulated learnings about a source"""
    name: str
    domain: str
    entry_url: str
    
    # Stats
    total_urls_tested: int = 0
    urls_with_leads: int = 0
    urls_without_leads: int = 0
    total_leads_found: int = 0
    usa_caribbean_leads: int = 0
    
    # Learned patterns
    gold_patterns: List[str] = field(default_factory=list)      # Patterns that produce leads
    junk_patterns: List[str] = field(default_factory=list)      # Patterns that never produce leads
    maybe_patterns: List[str] = field(default_factory=list)     # Need more data
    
    # Pattern stats: pattern -> {tested: N, leads: N, lead_rate: %}
    pattern_stats: Dict[str, Dict] = field(default_factory=dict)
    
    # Quality metrics
    lead_yield_rate: float = 0.0        # leads / urls scraped
    usa_caribbean_rate: float = 0.0     # relevant leads / total leads
    avg_confidence: float = 0.0
    
    # Timestamps
    first_tested: Optional[str] = None
    last_tested: Optional[str] = None
    last_updated: Optional[str] = None
    
    # Recommendations
    recommended_max_pages: int = 50
    recommended_priority: int = 5
    is_worth_scraping: bool = True
    notes: List[str] = field(default_factory=list)


class SourceLearningSystem:
    """
    Automatically learns which URL patterns produce leads for each source.
    
    Usage:
        learner = SourceLearningSystem()
        
        # After scraping and extraction, record results
        learner.record_result(url, produced_lead=True, lead_quality=0.85)
        
        # Get recommendations
        gold_patterns = learner.get_gold_patterns("Hotel Dive")
        junk_patterns = learner.get_junk_patterns("Hotel Dive")
        
        # Save learnings
        learner.save()
    """
    
    def __init__(self, data_dir: str = "data/learnings"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.learnings_file = self.data_dir / "source_learnings.json"
        self.history_file = self.data_dir / "url_history.json"
        
        # In-memory data
        self.learnings: Dict[str, SourceLearning] = {}
        self.url_history: List[URLAnalysis] = []
        
        # Load existing data
        self._load()
    
    def _load(self):
        """Load existing learnings from disk"""
        # Load learnings
        if self.learnings_file.exists():
            try:
                with open(self.learnings_file, 'r') as f:
                    data = json.load(f)
                    for name, learning_data in data.items():
                        self.learnings[name] = SourceLearning(**learning_data)
                logger.info(f"✅ Loaded learnings for {len(self.learnings)} sources")
            except Exception as e:
                logger.warning(f"Could not load learnings: {e}")
        
        # Load recent history (last 1000 URLs)
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    data = json.load(f)
                    # Only keep last 1000
                    for item in data[-1000:]:
                        item['scraped_at'] = datetime.fromisoformat(item['scraped_at'])
                        self.url_history.append(URLAnalysis(**item))
            except Exception as e:
                logger.warning(f"Could not load history: {e}")
    
    def save(self):
        """Save learnings to disk"""
        # Save learnings
        learnings_data = {}
        for name, learning in self.learnings.items():
            learnings_data[name] = asdict(learning)
        
        with open(self.learnings_file, 'w') as f:
            json.dump(learnings_data, f, indent=2, default=str)
        
        # Save history (last 1000)
        history_data = []
        for analysis in self.url_history[-1000:]:
            item = asdict(analysis)
            item['scraped_at'] = analysis.scraped_at.isoformat()
            history_data.append(item)
        
        with open(self.history_file, 'w') as f:
            json.dump(history_data, f, indent=2)
        
        logger.info(f"✅ Saved learnings for {len(self.learnings)} sources")
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower().replace('www.', '')
        # M4 FIX: bare except → except Exception (don't catch KeyboardInterrupt/SystemExit)
        except Exception:
            return ''
    
    def _extract_path_pattern(self, url: str) -> str:
        """
        Convert URL path to a generalized pattern.
        
        Examples:
            /news/12345.html -> /news/\d+\.html
            /2024/01/15/hotel-opens/ -> /\d{4}/\d{2}/\d{2}/[^/]+/
            /releases/marriott-opens-new-hotel -> /releases/[^/]+
        """
        try:
            parsed = urlparse(url)
            path = parsed.path
            
            # Replace numeric IDs with \d+
            pattern = re.sub(r'/\d+/', r'/\\d+/', path)
            pattern = re.sub(r'/\d+\.', r'/\\d+\\.', pattern)
            pattern = re.sub(r'\d{4}/\d{2}/\d{2}', r'\\d{4}/\\d{2}/\\d{2}', pattern)
            
            # Replace slug-like segments with [^/]+
            # But keep meaningful prefixes like /news/, /releases/, etc.
            parts = pattern.split('/')
            new_parts = []
            for i, part in enumerate(parts):
                if not part:
                    new_parts.append(part)
                elif part.startswith('\\d'):
                    new_parts.append(part)
                elif re.match(r'^[a-z]{2,20}$', part):
                    # Keep short lowercase words (likely categories)
                    new_parts.append(part)
                elif len(part) > 30:
                    # Long slugs -> pattern
                    new_parts.append('[^/]+')
                else:
                    new_parts.append(part)
            
            return '/'.join(new_parts)
        
        # M4 FIX: bare except → except Exception (don't catch KeyboardInterrupt/SystemExit)
        except Exception:
            return url
    
    def _get_or_create_learning(self, source_name: str, url: str) -> SourceLearning:
        """Get or create learning record for a source"""
        if source_name not in self.learnings:
            domain = self._extract_domain(url)
            self.learnings[source_name] = SourceLearning(
                name=source_name,
                domain=domain,
                entry_url=url,
                first_tested=datetime.now().isoformat()
            )
        return self.learnings[source_name]
    
    def record_result(
        self,
        source_name: str,
        url: str,
        produced_lead: bool,
        lead_quality: Optional[float] = None,
        lead_location: Optional[str] = None,
        response_time_ms: int = 0,
        content_length: int = 0
    ):
        """
        Record the result of scraping a URL.
        
        Args:
            source_name: Name of the source (e.g., "Hotel Dive")
            url: The URL that was scraped
            produced_lead: Whether this URL produced at least one lead
            lead_quality: Average quality/confidence of leads (0-1)
            lead_location: "USA", "Caribbean", "International", or None
            response_time_ms: How long the request took
            content_length: Size of the content
        """
        domain = self._extract_domain(url)
        path_pattern = self._extract_path_pattern(url)
        
        # Create analysis record
        analysis = URLAnalysis(
            url=url,
            domain=domain,
            path_pattern=path_pattern,
            scraped_at=datetime.now(),
            produced_lead=produced_lead,
            lead_quality=lead_quality,
            lead_location=lead_location,
            response_time_ms=response_time_ms,
            content_length=content_length
        )
        self.url_history.append(analysis)
        
        # Update learning
        learning = self._get_or_create_learning(source_name, url)
        learning.total_urls_tested += 1
        learning.last_tested = datetime.now().isoformat()
        
        if produced_lead:
            learning.urls_with_leads += 1
            learning.total_leads_found += 1
            if lead_location in ("USA", "Caribbean", "Florida"):
                learning.usa_caribbean_leads += 1
        else:
            learning.urls_without_leads += 1
        
        # Update pattern stats
        if path_pattern not in learning.pattern_stats:
            learning.pattern_stats[path_pattern] = {
                'tested': 0,
                'leads': 0,
                'total_quality': 0.0,
                'usa_caribbean': 0
            }
        
        stats = learning.pattern_stats[path_pattern]
        stats['tested'] += 1
        if produced_lead:
            stats['leads'] += 1
            if lead_quality:
                stats['total_quality'] += lead_quality
            if lead_location in ("USA", "Caribbean", "Florida"):
                stats['usa_caribbean'] += 1
        
        # Recalculate metrics
        self._recalculate_metrics(learning)
    
    def record_batch_results(
        self,
        source_name: str,
        results: List[Dict]
    ):
        """
        Record results for multiple URLs at once.
        
        Args:
            source_name: Name of the source
            results: List of dicts with keys: url, produced_lead, lead_quality, lead_location
        """
        for result in results:
            self.record_result(
                source_name=source_name,
                url=result.get('url', ''),
                produced_lead=result.get('produced_lead', False),
                lead_quality=result.get('lead_quality'),
                lead_location=result.get('lead_location'),
                response_time_ms=result.get('response_time_ms', 0),
                content_length=result.get('content_length', 0)
            )
    
    def _recalculate_metrics(self, learning: SourceLearning):
        """Recalculate all metrics for a source"""
        # Lead yield rate
        if learning.total_urls_tested > 0:
            learning.lead_yield_rate = learning.urls_with_leads / learning.total_urls_tested
        
        # USA/Caribbean rate
        if learning.total_leads_found > 0:
            learning.usa_caribbean_rate = learning.usa_caribbean_leads / learning.total_leads_found
        
        # Classify patterns
        learning.gold_patterns = []
        learning.junk_patterns = []
        learning.maybe_patterns = []
        
        for pattern, stats in learning.pattern_stats.items():
            tested = stats['tested']
            leads = stats['leads']
            
            if tested < 3:
                # Not enough data
                learning.maybe_patterns.append(pattern)
            elif tested > 0:
                lead_rate = leads / tested
                if lead_rate >= 0.3:  # 30%+ lead rate = GOLD
                    learning.gold_patterns.append(pattern)
                elif lead_rate == 0 and tested >= 5:  # 0% with 5+ tests = JUNK
                    learning.junk_patterns.append(pattern)
                else:
                    learning.maybe_patterns.append(pattern)
        
        # Determine if worth scraping
        if learning.total_urls_tested >= 10:
            learning.is_worth_scraping = learning.lead_yield_rate >= 0.05  # At least 5% yield
        
        # Recommend max pages based on yield
        if learning.lead_yield_rate >= 0.5:
            learning.recommended_max_pages = 100  # High yield, scrape more
        elif learning.lead_yield_rate >= 0.2:
            learning.recommended_max_pages = 50
        elif learning.lead_yield_rate >= 0.1:
            learning.recommended_max_pages = 30
        else:
            learning.recommended_max_pages = 20  # Low yield, scrape less
        
        # Recommend priority based on USA/Caribbean rate
        if learning.usa_caribbean_rate >= 0.5 and learning.lead_yield_rate >= 0.2:
            learning.recommended_priority = 10
        elif learning.usa_caribbean_rate >= 0.3 or learning.lead_yield_rate >= 0.3:
            learning.recommended_priority = 8
        elif learning.usa_caribbean_rate >= 0.1 or learning.lead_yield_rate >= 0.1:
            learning.recommended_priority = 6
        else:
            learning.recommended_priority = 4
        
        learning.last_updated = datetime.now().isoformat()
    
    def get_gold_patterns(self, source_name: str) -> List[str]:
        """Get patterns that produce leads for a source"""
        if source_name in self.learnings:
            return self.learnings[source_name].gold_patterns
        return []
    
    def get_junk_patterns(self, source_name: str) -> List[str]:
        """Get patterns that never produce leads for a source"""
        if source_name in self.learnings:
            return self.learnings[source_name].junk_patterns
        return []
    
    def get_recommendations(self, source_name: str) -> Dict:
        """Get recommended settings for a source"""
        if source_name not in self.learnings:
            return {
                'status': 'untested',
                'message': 'No data yet - run a test first'
            }
        
        learning = self.learnings[source_name]
        
        return {
            'status': 'tested',
            'total_urls_tested': learning.total_urls_tested,
            'lead_yield_rate': f"{learning.lead_yield_rate:.1%}",
            'usa_caribbean_rate': f"{learning.usa_caribbean_rate:.1%}",
            'is_worth_scraping': learning.is_worth_scraping,
            'recommended_priority': learning.recommended_priority,
            'recommended_max_pages': learning.recommended_max_pages,
            'gold_patterns': learning.gold_patterns,
            'junk_patterns': learning.junk_patterns,
            'needs_more_data': learning.maybe_patterns,
        }
    
    def get_source_report(self, source_name: str) -> str:
        """Get a detailed report for a source"""
        if source_name not in self.learnings:
            return f"❌ No data for '{source_name}' - run a test first"
        
        learning = self.learnings[source_name]
        
        report = []
        report.append("=" * 60)
        report.append(f"📊 SOURCE REPORT: {source_name}")
        report.append("=" * 60)
        
        report.append(f"\n🌐 Domain: {learning.domain}")
        report.append(f"📅 First tested: {learning.first_tested}")
        report.append(f"📅 Last tested: {learning.last_tested}")
        
        report.append(f"\n📈 PERFORMANCE:")
        report.append(f"   URLs tested: {learning.total_urls_tested}")
        report.append(f"   URLs with leads: {learning.urls_with_leads}")
        report.append(f"   Lead yield rate: {learning.lead_yield_rate:.1%}")
        report.append(f"   USA/Caribbean rate: {learning.usa_caribbean_rate:.1%}")
        
        report.append(f"\n⭐ RECOMMENDATION:")
        report.append(f"   Worth scraping: {'✅ YES' if learning.is_worth_scraping else '❌ NO'}")
        report.append(f"   Priority: {learning.recommended_priority}/10")
        report.append(f"   Max pages: {learning.recommended_max_pages}")
        
        if learning.gold_patterns:
            report.append(f"\n🥇 GOLD PATTERNS (produce leads):")
            for pattern in learning.gold_patterns[:10]:
                stats = learning.pattern_stats.get(pattern, {})
                rate = stats.get('leads', 0) / max(stats.get('tested', 1), 1)
                report.append(f"   ✅ {pattern} ({rate:.0%} yield)")
        
        if learning.junk_patterns:
            report.append(f"\n🗑️ JUNK PATTERNS (no leads, block these):")
            for pattern in learning.junk_patterns[:10]:
                stats = learning.pattern_stats.get(pattern, {})
                report.append(f"   ❌ {pattern} (tested {stats.get('tested', 0)}x)")
        
        if learning.maybe_patterns:
            report.append(f"\n❓ NEEDS MORE DATA:")
            for pattern in learning.maybe_patterns[:5]:
                stats = learning.pattern_stats.get(pattern, {})
                report.append(f"   ⚪ {pattern} (tested {stats.get('tested', 0)}x)")
        
        report.append("\n" + "=" * 60)
        
        return "\n".join(report)
    
    def get_all_sources_summary(self) -> str:
        """Get summary of all tested sources"""
        if not self.learnings:
            return "❌ No sources tested yet"
        
        report = []
        report.append("=" * 70)
        report.append("📊 ALL SOURCES SUMMARY")
        report.append("=" * 70)
        
        # Sort by yield rate
        sorted_sources = sorted(
            self.learnings.items(),
            key=lambda x: (x[1].usa_caribbean_rate, x[1].lead_yield_rate),
            reverse=True
        )
        
        report.append(f"\n{'Source':<30} {'Tested':<8} {'Yield':<8} {'USA/Car':<8} {'Priority':<8}")
        report.append("-" * 70)
        
        for name, learning in sorted_sources:
            worth = "✅" if learning.is_worth_scraping else "❌"
            report.append(
                f"{name:<30} {learning.total_urls_tested:<8} "
                f"{learning.lead_yield_rate:.0%:<8} {learning.usa_caribbean_rate:.0%:<8} "
                f"{learning.recommended_priority}/10 {worth}"
            )
        
        report.append("\n" + "=" * 70)
        
        # Top recommendations
        top_sources = [s for s in sorted_sources if s[1].is_worth_scraping][:5]
        if top_sources:
            report.append("\n🏆 TOP RECOMMENDED SOURCES:")
            for name, learning in top_sources:
                report.append(f"   1. {name} - {learning.lead_yield_rate:.0%} yield, {learning.usa_caribbean_rate:.0%} relevant")
        
        return "\n".join(report)
    
    def export_tuning_config(self, output_file: str = "data/learned_tuning.py"):
        """Export learned patterns as a Python config file"""
        lines = []
        lines.append('"""')
        lines.append('AUTO-GENERATED SOURCE TUNING CONFIG')
        lines.append(f'Generated: {datetime.now().isoformat()}')
        lines.append('Based on actual scraping results')
        lines.append('"""')
        lines.append('')
        lines.append('LEARNED_PATTERNS = {')
        
        for name, learning in self.learnings.items():
            lines.append(f'    "{name}": {{')
            lines.append(f'        "domain": "{learning.domain}",')
            lines.append(f'        "priority": {learning.recommended_priority},')
            lines.append(f'        "max_pages": {learning.recommended_max_pages},')
            lines.append(f'        "is_worth_scraping": {learning.is_worth_scraping},')
            lines.append(f'        "lead_yield_rate": {learning.lead_yield_rate:.3f},')
            lines.append(f'        "gold_patterns": {learning.gold_patterns},')
            lines.append(f'        "junk_patterns": {learning.junk_patterns},')
            lines.append(f'    }},')
        
        lines.append('}')
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            f.write('\n'.join(lines))
        
        logger.info(f"✅ Exported tuning config to {output_file}")
        return output_file


# =============================================================================
# INTEGRATION WITH ORCHESTRATOR
# =============================================================================

class LearningOrchestrator:
    """
    Wraps the orchestrator to automatically learn from results.
    
    Usage:
        learner = LearningOrchestrator()
        await learner.run_and_learn(sources=["Hotel Dive", "Caribbean Journal"])
        learner.print_learnings()
    """
    
    def __init__(self):
        self.learning_system = SourceLearningSystem()
    
    async def run_and_learn(self, sources: Optional[List[str]] = None):
        """Run the pipeline and learn from results"""
        # Import here to avoid circular imports
        from app.services.orchestrator import LeadHunterOrchestrator
        
        orchestrator = LeadHunterOrchestrator()
        await orchestrator.initialize()
        
        try:
            # Run the pipeline
            leads = await orchestrator.run(source_names=sources)
            
            # Get scrape results from the engine
            scrape_results = orchestrator.scraping_engine._last_results if hasattr(orchestrator.scraping_engine, '_last_results') else {}
            
            # Learn from results
            self._learn_from_leads(orchestrator, leads, sources)
            
            # Save learnings
            self.learning_system.save()
            
            return leads
            
        finally:
            await orchestrator.close()
    
    def _learn_from_leads(self, orchestrator, leads: List[Dict], sources: Optional[List[str]]):
        """Extract learnings from pipeline results"""
        # Get the source names we tested
        source_names = sources or list(orchestrator.scraping_engine._sources.keys())
        
        # Build URL -> lead mapping
        url_to_lead = {}
        for lead in leads:
            source_url = lead.get('source_url', '')
            if source_url:
                url_to_lead[source_url] = lead
        
        # For each source, record which URLs produced leads
        for source_name in source_names:
            # Get all URLs that were scraped for this source
            # We'll track this from the leads we found
            source_leads = [l for l in leads if l.get('source_name') == source_name]
            
            for lead in source_leads:
                url = lead.get('source_url', '')
                if not url:
                    continue
                
                # Get lead details
                lead_quality = lead.get('confidence_score', 0.5)
                country = lead.get('country', '')
                state = lead.get('state', '')
                
                # Determine location category
                if country in ('USA', 'United States', 'US'):
                    if state and 'Florida' in state:
                        lead_location = 'Florida'
                    else:
                        lead_location = 'USA'
                elif country in ('Aruba', 'Bahamas', 'Jamaica', 'Puerto Rico', 
                                'Turks and Caicos', 'Cayman Islands', 'Barbados',
                                'St. Lucia', 'Antigua', 'Bermuda', 'Virgin Islands',
                                'Dominican Republic', 'Trinidad', 'Curacao'):
                    lead_location = 'Caribbean'
                else:
                    lead_location = 'International'
                
                # Record this URL produced a lead
                self.learning_system.record_result(
                    source_name=source_name,
                    url=url,
                    produced_lead=True,
                    lead_quality=lead_quality,
                    lead_location=lead_location
                )
        
        # Log summary
        total_leads = len(leads)
        logger.info(f"📚 Recorded learnings from {total_leads} leads")
    
    def print_learnings(self):
        """Print summary of all learnings"""
        print(self.learning_system.get_all_sources_summary())
    
    def print_source_report(self, source_name: str):
        """Print detailed report for a source"""
        print(self.learning_system.get_source_report(source_name))
    
    def export_config(self):
        """Export learned patterns as config file"""
        return self.learning_system.export_tuning_config()


# =============================================================================
# CLI INTERFACE
# =============================================================================

def print_help():
    print("""
SMART LEAD HUNTER - SOURCE LEARNING SYSTEM
==========================================

Commands:
    python -m app.services.source_learning test <source_name>
        Test a single source and learn from results
    
    python -m app.services.source_learning test-all
        Test all configured sources
    
    python -m app.services.source_learning report <source_name>
        Show detailed report for a source
    
    python -m app.services.source_learning summary
        Show summary of all tested sources
    
    python -m app.services.source_learning export
        Export learned patterns to config file

Examples:
    python -m app.services.source_learning test "Hotel Dive"
    python -m app.services.source_learning test "Caribbean Journal"
    python -m app.services.source_learning summary
""")


async def main():
    import sys
    
    if len(sys.argv) < 2:
        print_help()
        return
    
    command = sys.argv[1]
    
    learner = LearningOrchestrator()
    
    if command == "test" and len(sys.argv) >= 3:
        source_name = sys.argv[2]
        print(f"\n🧪 Testing source: {source_name}\n")
        await learner.run_and_learn(sources=[source_name])
        learner.print_source_report(source_name)
        
    elif command == "test-all":
        print("\n🧪 Testing all sources...\n")
        await learner.run_and_learn()
        learner.print_learnings()
        
    elif command == "report" and len(sys.argv) >= 3:
        source_name = sys.argv[2]
        learner.print_source_report(source_name)
        
    elif command == "summary":
        learner.print_learnings()
        
    elif command == "export":
        output = learner.export_config()
        print(f"✅ Exported to {output}")
        
    else:
        print_help()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
