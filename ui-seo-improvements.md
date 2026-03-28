# UI/UX and SEO Improvements

Review the current site and implement these changes.

## UI/UX Fixes

### Global
- The background is too flat — everything is the same gray with white cards. Add more contrast. Try a slightly warmer off-white background with crisper white cards using subtle box-shadows instead of just borders.
- Add subtle hover states on all clickable rows and cards — the lists look static and it's not obvious things are clickable. The entire row should be a clickable link.
- Typography: increase font weight contrast between headings and body. Add more letter-spacing to uppercase stat labels (INSPECTIONS, CRITICAL, etc.).

### Name Cleanup
- Fix apostrophe capitalization bug: "Dee'S Deli" should be "Dee's Deli", "Applebee'S" should be "Applebee's". The title-case logic is capitalizing the character after apostrophes — fix this.
- Strip "Llc", "Inc", "Corp" suffixes from display names. Keep them in the database but don't render them on the page.
- Keep store numbers like "7-Eleven 33426" — those are useful for distinguishing locations.

### Homepage
- The "Recently Inspected" feed shows schools, hospitals, and other non-restaurant facilities. Add a toggle at the top of the list: "Restaurants | All Facilities" — default to "Restaurants" so the homepage feels like a restaurant-focused site. Show all facilities when toggled.
- The "Lowest Scores This Month" section should visually differ from "Recently Inspected" — give it a subtle warm/red-tinted background to signal "warning" content.
- Add a one-line description under each section header. Under "Lowest Scores This Month": "Facilities with the lowest inspection scores in the past 30 days."

### Region Page (/rhode-island/)
- "Neighborhoods & Cities" section: Add a "Top Cities" row at the top showing the 6-8 cities with the most restaurants as larger featured cards (Providence, Cranston, Warwick, etc.). Then show the full alphabetical list below. Right now Angola with 1 restaurant appears before Providence with hundreds, which buries the most useful content.
- "Browse by Type" grid: Add a restaurant count to each type box so users can see how many are in each category. Consider adding a subtle emoji or icon per type to make the grid more scannable.

### Category/Type Pages (/rhode-island/american/)
- Change table header "FACILITY" to "NAME" — facility sounds too clinical.
- Default sort to "Last Inspected" (most recent first) instead of score. A page full of 100s is boring to browse and doesn't feel like fresh content. Allow users to click column headers to re-sort by score, name, or date.
- Add pagination — 25-50 results per page. Don't dump all 579 on one page. This also creates more indexable URLs for Google.

### Restaurant Page
- Give the AI summary section a visual accent — a slightly different background color or a left border accent — to distinguish it from the rest of the page.
- Make sure the score ring color matches the risk tier: green for 70+, amber for 50-70, red for below 50.
- The footer internal links ("All facilities in Providence", "More School/Childcare in Rhode Island") are great for SEO but look like an afterthought. Make them more visually prominent — give them button-like styling or at least more padding and a background.

## SEO Improvements

### Title Tags
Every page needs a unique, keyword-rich title tag:
- Homepage: `Restaurant Health Inspection Scores | [SiteName]`
- Region: `Restaurant Health Inspections in Rhode Island — Scores & Violations | [SiteName]`
- City/Neighborhood: `Restaurant Health Inspections in Providence, RI — Scores & Rankings | [SiteName]`
- Type page: `American Restaurant Health Inspections in Rhode Island | [SiteName]`
- Restaurant: `[Name] Health Inspection Score & History — [City], [State] | [SiteName]`

### Meta Descriptions
Unique meta descriptions per page template:
- Restaurant pages: `View the full health inspection history for [Name] in [City], [State]. Current score: [Score]. [X] inspections on record. See violations, risk tier, and nearby restaurants.`
- City pages: `Browse health inspection scores for [X] restaurants in [City], [State]. See the cleanest and lowest-scoring restaurants, ranked by inspection score.`
- Type pages: `Health inspection scores for [X] [Type] restaurants in [State]. Browse scores, violations, and risk tiers.`

### Structured Data (JSON-LD)
Add to restaurant pages if not already present:
- LocalBusiness schema with name, address, geo coordinates
- BreadcrumbList schema matching the visible breadcrumbs

### Internal Linking
- On restaurant pages, add "More [cuisine type] restaurants in [city]" links. E.g., "More Italian restaurants in Providence"
- On city pages, link to adjacent/nearby cities. E.g., Providence page links to Cranston, East Providence, Warwick.
- Homepage "Lowest Scores This Month" — restaurant name links to restaurant page, city name links to city page (two separate links).

### Fresh Content Signals
- Add a "Last updated" date to region and city pages showing when data was last refreshed.
- Add a "Recent Inspections" section to each city page showing the 10 most recent inspections in that city.

### Sitemap
- Confirm sitemap.xml includes `<lastmod>` dates based on most recent inspection for each restaurant.
- Split into sitemap index with child sitemaps if over 1,000 URLs.
- Add `Sitemap: https://[domain]/sitemap.xml` to robots.txt.

### Page Speed
- Minify CSS.
- No unnecessary JavaScript.
- Add cache headers for static assets.
