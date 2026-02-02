"""
SMART LEAD HUNTER - SOURCE TESTER & TUNER
==========================================
Test each source individually and generate a report
showing which sources are working and which need tuning.

Usage:
    python -m app.services.source_tester --all          # Test all sources
    python -m app.services.source_tester --source "Hotel Dive"
    python -m app.services.source_tester --florida      # Test Florida sources only
    python -m app.services.source_tester --caribbean    # Test Caribbean sources only
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SourceTestResult:
    """Results from testing a single source"""
    source_name: str
    tested_at: str = ""
    
    # Scraping results
    pages_scraped: int = 0
    scrape_success: bool = False
    scrape_time_seconds: float = 0.0
    scrape_errors: List[str] = field(default_factory=list)
    
    # Classification results
    pages_classified: int = 0
    pages_relevant: int = 0
    relevance_rate: float = 0.0  # % of pages that were relevant
    
    # Extraction results
    leads_extracted: int = 0
    leads_qualified: int = 0
    qualification_rate: float = 0.0  # % of leads that qualified
    
    # Quality metrics
    avg_lead_score: float = 0.0
    high_quality_leads: int = 0  # Score 70+
    medium_quality_leads: int = 0  # Score 40-69
    low_quality_leads: int = 0  # Score < 40
    
    # Contact enrichment
    leads_with_email: int = 0
    leads_with_phone: int = 0
    leads_with_contact_name: int = 0
    contact_rate: float = 0.0  # % of leads with any contact info
    
    # Location breakdown
    florida_leads: int = 0
    caribbean_leads: int = 0
    other_us_leads: int = 0
    international_leads: int = 0
    
    # Sample leads (top 3)
    sample_leads: List[Dict] = field(default_factory=list)
    
    # Recommendations
    status: str = "UNKNOWN"  # EXCELLENT, GOOD, NEEDS_TUNING, FAILING
    recommendations: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SourceTester:
    """Test sources and generate tuning recommendations"""
    
    def __init__(self, output_dir: str = "./test_results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results: Dict[str, SourceTestResult] = {}
    
    async def test_source(self, source_name: str) -> SourceTestResult:
        """Test a single source and return results"""
        result = SourceTestResult(
            source_name=source_name,
            tested_at=datetime.now().isoformat()
        )
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🧪 TESTING SOURCE: {source_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Import orchestrator
            from app.services.orchestrator import LeadHunterOrchestrator
            
            # Create orchestrator
            orchestrator = LeadHunterOrchestrator(
                output_dir=str(self.output_dir / source_name.replace(" ", "_"))
            )
            
            # Initialize
            await orchestrator.initialize()
            
            # Run on single source
            start_time = time.time()
            leads = await orchestrator.run(source_names=[source_name])
            result.scrape_time_seconds = time.time() - start_time
            result.scrape_success = True
            
            # Extract stats from orchestrator
            stats = orchestrator.stats
            result.pages_scraped = stats.pages_scraped
            result.pages_classified = stats.pages_processed
            result.leads_extracted = stats.leads_extracted
            result.leads_qualified = stats.leads_after_dedup
            result.high_quality_leads = stats.high_quality_leads
            result.medium_quality_leads = stats.medium_quality_leads
            result.low_quality_leads = stats.low_quality_leads
            result.leads_with_email = stats.leads_with_email
            result.leads_with_phone = stats.leads_with_phone
            result.leads_with_contact_name = stats.leads_with_contact_name
            
            # Calculate rates
            if result.pages_scraped > 0:
                result.relevance_rate = (result.pages_classified / result.pages_scraped) * 100 if result.pages_classified > 0 else 0
            
            if result.leads_extracted > 0:
                result.qualification_rate = (result.leads_qualified / result.leads_extracted) * 100
            
            if result.leads_qualified > 0:
                result.contact_rate = ((result.leads_with_email + result.leads_with_phone + result.leads_with_contact_name) / (result.leads_qualified * 3)) * 100
            
            # Analyze leads for location breakdown
            for lead in leads:
                country = lead.get('country', '').upper()
                state = lead.get('state', '').upper()
                
                if 'FLORIDA' in state or 'FL' == state:
                    result.florida_leads += 1
                elif country in ('USA', 'US', 'UNITED STATES'):
                    result.other_us_leads += 1
                elif country in ('BAHAMAS', 'JAMAICA', 'ARUBA', 'PUERTO RICO', 
                               'TURKS AND CAICOS', 'CAYMAN ISLANDS', 'BARBADOS',
                               'ST. LUCIA', 'ANTIGUA', 'BERMUDA', 'DOMINICAN REPUBLIC',
                               'TRINIDAD', 'CURACAO', 'ST. MARTIN', 'ANGUILLA'):
                    result.caribbean_leads += 1
                else:
                    result.international_leads += 1
            
            # Calculate average score
            scores = [lead.get('confidence_score', 0) for lead in leads]
            if scores:
                result.avg_lead_score = sum(scores) / len(scores) * 100  # Convert to 0-100
            
            # Sample leads (top 3 by score)
            sorted_leads = sorted(leads, key=lambda x: -x.get('confidence_score', 0))
            result.sample_leads = sorted_leads[:3]
            
            # Clean up
            await orchestrator.close()
            
        except Exception as e:
            result.scrape_success = False
            result.scrape_errors.append(str(e))
            logger.error(f"❌ Error testing {source_name}: {e}")
        
        # Generate recommendations
        self._analyze_and_recommend(result)
        
        # Store result
        self.results[source_name] = result
        
        # Print summary
        self._print_source_summary(result)
        
        return result
    
    def _analyze_and_recommend(self, result: SourceTestResult):
        """Analyze results and generate recommendations"""
        recommendations = []
        
        # Determine status
        if not result.scrape_success:
            result.status = "FAILING"
            recommendations.append("❌ Scraping failed - check crawler type and URL patterns")
        elif result.leads_qualified == 0:
            result.status = "NEEDS_TUNING"
            recommendations.append("⚠️ No qualified leads - check gold_patterns and content")
        elif result.leads_qualified < 3:
            result.status = "NEEDS_TUNING"
            recommendations.append("⚠️ Very few leads - consider expanding max_pages or gold_patterns")
        elif result.contact_rate < 10:
            result.status = "GOOD"
            recommendations.append("📧 Low contact rate - needs contact enrichment")
        elif result.florida_leads + result.caribbean_leads == 0:
            result.status = "GOOD"
            recommendations.append("🌴 No FL/Caribbean leads - may not be core market source")
        else:
            result.status = "EXCELLENT"
            recommendations.append("✅ Source is performing well!")
        
        # Specific recommendations
        if result.relevance_rate and result.relevance_rate < 20:
            recommendations.append("🎯 Low relevance rate - tighten gold_patterns to target better URLs")
        
        if result.pages_scraped < 10:
            recommendations.append("📄 Few pages scraped - check entry_url and link_patterns")
        
        if result.qualification_rate and result.qualification_rate < 50:
            recommendations.append("⭐ Low qualification rate - extracted leads may be low quality")
        
        if result.leads_with_email == 0 and result.leads_qualified > 0:
            recommendations.append("📧 No emails found - add contact page scraping")
        
        if result.international_leads > result.florida_leads + result.caribbean_leads + result.other_us_leads:
            recommendations.append("🌍 Mostly international leads - add location filtering")
        
        result.recommendations = recommendations
    
    def _print_source_summary(self, result: SourceTestResult):
        """Print summary for a source"""
        status_emoji = {
            "EXCELLENT": "🏆",
            "GOOD": "✅",
            "NEEDS_TUNING": "⚠️",
            "FAILING": "❌",
            "UNKNOWN": "❓"
        }
        
        print(f"""
┌{'─'*58}┐
│ {status_emoji.get(result.status, '❓')} {result.source_name:<52} │
├{'─'*58}┤
│ Status: {result.status:<48} │
│ Time: {result.scrape_time_seconds:.1f}s{' '*47}│
├{'─'*58}┤
│ 📄 Pages Scraped: {result.pages_scraped:<38} │
│ 🎯 Pages Relevant: {result.pages_classified:<37} │
│ 📝 Leads Extracted: {result.leads_extracted:<36} │
│ ✅ Leads Qualified: {result.leads_qualified:<36} │
├{'─'*58}┤
│ ⭐ Avg Score: {result.avg_lead_score:.0f}/100{' '*40}│
│ 🔴 High Quality (70+): {result.high_quality_leads:<33} │
│ 🟠 Medium (40-69): {result.medium_quality_leads:<37} │
│ 🔵 Low (<40): {result.low_quality_leads:<42} │
├{'─'*58}┤
│ 📧 With Email: {result.leads_with_email:<41} │
│ 📞 With Phone: {result.leads_with_phone:<41} │
│ 👤 With Contact Name: {result.leads_with_contact_name:<34} │
├{'─'*58}┤
│ 🌴 Florida: {result.florida_leads:<44} │
│ 🏝️ Caribbean: {result.caribbean_leads:<43} │
│ 🇺🇸 Other US: {result.other_us_leads:<43} │
│ 🌍 International: {result.international_leads:<38} │
└{'─'*58}┘
""")
        
        if result.recommendations:
            print("📋 RECOMMENDATIONS:")
            for rec in result.recommendations:
                print(f"   {rec}")
        
        if result.sample_leads:
            print("\n🏨 SAMPLE LEADS:")
            for i, lead in enumerate(result.sample_leads[:3], 1):
                name = lead.get('hotel_name', 'Unknown')[:40]
                loc = f"{lead.get('city', '')}, {lead.get('state', '')}".strip(', ')[:20]
                score = lead.get('confidence_score', 0) * 100
                print(f"   {i}. {name} ({loc}) - Score: {score:.0f}")
    
    async def test_all_sources(self) -> Dict[str, SourceTestResult]:
        """Test all tuned sources"""
        from app.services.source_tuning import get_all_tuned_sources
        
        sources = get_all_tuned_sources()
        logger.info(f"\n🧪 TESTING ALL {len(sources)} SOURCES...")
        
        for name in sources:
            await self.test_source(name)
            await asyncio.sleep(2)  # Rate limiting between sources
        
        return self.results
    
    async def test_florida_sources(self) -> Dict[str, SourceTestResult]:
        """Test Florida-focused sources"""
        from app.services.source_tuning import get_florida_sources
        
        sources = get_florida_sources()
        logger.info(f"\n🌴 TESTING {len(sources)} FLORIDA SOURCES...")
        
        for name in sources:
            await self.test_source(name)
            await asyncio.sleep(2)
        
        return self.results
    
    async def test_caribbean_sources(self) -> Dict[str, SourceTestResult]:
        """Test Caribbean-focused sources"""
        from app.services.source_tuning import get_caribbean_sources
        
        sources = get_caribbean_sources()
        logger.info(f"\n🏝️ TESTING {len(sources)} CARIBBEAN SOURCES...")
        
        for name in sources:
            await self.test_source(name)
            await asyncio.sleep(2)
        
        return self.results
    
    def generate_report(self) -> str:
        """Generate comprehensive test report"""
        report = []
        report.append("=" * 70)
        report.append("SMART LEAD HUNTER - SOURCE TEST REPORT")
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append("=" * 70)
        
        # Summary stats
        total = len(self.results)
        excellent = len([r for r in self.results.values() if r.status == "EXCELLENT"])
        good = len([r for r in self.results.values() if r.status == "GOOD"])
        needs_tuning = len([r for r in self.results.values() if r.status == "NEEDS_TUNING"])
        failing = len([r for r in self.results.values() if r.status == "FAILING"])
        
        report.append(f"\n📊 OVERALL SUMMARY")
        report.append(f"   Total Sources Tested: {total}")
        report.append(f"   🏆 Excellent: {excellent}")
        report.append(f"   ✅ Good: {good}")
        report.append(f"   ⚠️ Needs Tuning: {needs_tuning}")
        report.append(f"   ❌ Failing: {failing}")
        
        # Total leads
        total_leads = sum(r.leads_qualified for r in self.results.values())
        total_florida = sum(r.florida_leads for r in self.results.values())
        total_caribbean = sum(r.caribbean_leads for r in self.results.values())
        total_with_email = sum(r.leads_with_email for r in self.results.values())
        
        report.append(f"\n📈 TOTAL LEADS FOUND")
        report.append(f"   Total Qualified: {total_leads}")
        report.append(f"   🌴 Florida: {total_florida}")
        report.append(f"   🏝️ Caribbean: {total_caribbean}")
        report.append(f"   📧 With Email: {total_with_email}")
        
        # Ranked sources
        report.append(f"\n🏆 SOURCES RANKED BY LEADS")
        ranked = sorted(self.results.values(), key=lambda x: -x.leads_qualified)
        for i, r in enumerate(ranked[:10], 1):
            report.append(f"   {i}. {r.source_name}: {r.leads_qualified} leads ({r.status})")
        
        # Sources needing attention
        report.append(f"\n⚠️ SOURCES NEEDING ATTENTION")
        for name, r in self.results.items():
            if r.status in ("NEEDS_TUNING", "FAILING"):
                report.append(f"\n   {name} ({r.status}):")
                for rec in r.recommendations:
                    report.append(f"      • {rec}")
        
        report.append("\n" + "=" * 70)
        
        report_text = "\n".join(report)
        
        # Save report
        report_path = self.output_dir / f"source_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        # Save JSON results
        json_path = self.output_dir / f"source_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({name: r.to_dict() for name, r in self.results.items()}, f, indent=2, ensure_ascii=False)
        
        print(report_text)
        print(f"\n📁 Report saved to: {report_path}")
        print(f"📁 JSON saved to: {json_path}")
        
        return report_text


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test and tune sources")
    parser.add_argument("--source", type=str, help="Test a specific source")
    parser.add_argument("--all", action="store_true", help="Test all sources")
    parser.add_argument("--florida", action="store_true", help="Test Florida sources")
    parser.add_argument("--caribbean", action="store_true", help="Test Caribbean sources")
    parser.add_argument("--quick", action="store_true", help="Quick test - top 5 sources only")
    
    args = parser.parse_args()
    
    tester = SourceTester()
    
    if args.source:
        await tester.test_source(args.source)
    elif args.all:
        await tester.test_all_sources()
    elif args.florida:
        await tester.test_florida_sources()
    elif args.caribbean:
        await tester.test_caribbean_sources()
    elif args.quick:
        # Quick test: top priority sources
        top_sources = ["Hotel Dive", "Caribbean Journal", "Four Seasons Press", "Marriott News", "Orange Studio"]
        for name in top_sources:
            await tester.test_source(name)
            await asyncio.sleep(2)
    else:
        # Default: quick test
        print("Usage:")
        print("  --source 'Hotel Dive'  - Test specific source")
        print("  --all                  - Test all sources")
        print("  --florida              - Test Florida sources")
        print("  --caribbean            - Test Caribbean sources")
        print("  --quick                - Quick test (5 top sources)")
        return
    
    # Generate report
    tester.generate_report()


if __name__ == "__main__":
    asyncio.run(main())