# ForkGrade

[ForkGrade](https://forkgrade.com) is a free, independent tool that aggregates restaurant and food facility health inspection records from government sources and presents them in a consistent, searchable format.

Health inspection data is public, but it's often buried in agency databases that are slow, hard to search, and inconsistent across regions. ForkGrade pulls that data into one place so it's easy to look up any location, see its full inspection history, and understand its risk level at a glance.

## Coverage

| Region | Source | Locations |
|---|---|---|
| New York City | NYC Dept. of Health and Mental Hygiene | ~27,400 |
| Texas | Houston Health Dept. + San Antonio Metro Health | ~24,000 |
| Maricopa County | Maricopa County Environmental Services | ~23,100 |
| Rhode Island | RI Dept. of Health | ~6,700 |

Over 75,000 facilities and 250,000+ inspection records. Additional regions are added on an ongoing basis.

## Features

- Search any restaurant, cafe, food truck, school, or grocery store by name
- View full inspection history with scores, violation details, and risk tier (Low / Medium / High)
- NYC inspections include official DOH letter grades (A, B, C)
- AI-generated plain-English summaries of recent inspections
- Data refreshed daily from government sources

## Stack

- **Backend:** Python / Flask
- **Database:** PostgreSQL (hosted on Fly.io)
- **Import scripts:** Per-region scrapers/API clients in `scripts/`
- **AI summaries:** Google Gemini
- **Hosting:** Fly.io
- **Backups:** Backblaze B2 (daily automated via GitHub Actions)
