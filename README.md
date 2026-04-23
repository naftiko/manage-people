# Manage People

This capability tracks individuals in the Naftiko people network — customers, partners, community members, and influencers — using a Schema.org compliant YAML file in `context/people/people.yml`. Each person also has an optional long-form profile markdown in `context/people/people/`.

The YAML is the source of truth and syncs upstream to the Naftiko People Network database in Notion (GTM Home).

**Source:** `context/people/people.yml`  
**Profiles:** `context/people/people/`  
**Notion Database:** People Network (GTM Home) — ID `34b4adce-3d02-8164-8ad8-e9f79a8d4d7a`  
**Skill:** `.claude/skills/sync-people-network-to-notion/`

## How It Works

1. Add or update a person in `context/people/people.yml` using Schema.org Person format
2. Optionally add a long-form profile in `context/people/people/{slug}.md` and reference it via `profile_file`
3. Run the sync skill to upsert into the Notion People Network database
4. Local YAML is the source of truth — Notion is the display/collaboration layer

## Schema

Each person uses Schema.org `Person`:

- `name` — full name
- `jobTitle` — current title
- `worksFor` — Organization
- `homeLocation` — Place with address
- `sameAs` — array of profile URLs (LinkedIn, etc.)
- `keywords` — tags for filtering
- `profile_file` — relative path to the long-form profile markdown

## Distinct From

- `context/conversations/people.yaml` + `sync-people-to-notion` — narrower scope: only people who have been on a recorded conversation
- This capability tracks the broader Naftiko network

## Folders

- `capabilities/` — Naftiko capability YAML defining people management tools
- `data/`, `openapi/`, `schema/`, `skills/` — supporting working files

## Tasks

- First task
