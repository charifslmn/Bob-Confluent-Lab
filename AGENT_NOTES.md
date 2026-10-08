# Agent Notes — How Lab CLI is Being Built

## What I'm Doing

I (Bob, the AI agent) am recreating the `Lab UI Driven` lab as a **Bob-assisted CLI lab**. The idea is that the finished lab doc (`Lab CLI/README.md`) will guide a user through the same Confluent Cloud trade forecasting pipeline — but instead of clicking through the UI, the user just gives **natural language prompts to Bob**, and Bob runs the CLI commands on their behalf.

The user confirms each step worked by checking the CLI outputs or by **checking the Confluent Cloud UI** — which is why high-quality, verified screenshots are included.

---

## Screenshot Quality & Verification Guidelines

To ensure screenshots in `Lab CLI/screenshots/` match the clean aesthetic and high quality of `Lab UI Driven/screenshots/`:
1. **Full Page & Data Loading:** Always wait for the page, network requests, and DOM elements to fully finish loading before capturing. Never capture while loading spinners, progress bars, or skeleton loaders are active.
2. **Clean UI State:** Dismiss or close any promotional overlays, announcement modals, cookie banners, or popups (e.g., VS Code extension prompts, helper dialogs) before taking screenshots.
3. **Consistent Theme:** Maintain consistent visual theme (dark mode preferred if matching the UI lab) and clear resolution.
4. **Cropping & Focus:** Ensure screenshots are focused on the relevant panel, table, or overview without unnecessary blank space or distractions.
5. **Visual Verification:** Always visually inspect the resulting image file using `read_file` to confirm that the captured screenshot is clean, crisp, legible, and shows the intended state before referencing it in documentation.

---

## How I'm Building It

### Step 1 — Run the lab myself first
I am going through every stage of the lab end-to-end using the **Confluent CLI** (`confluent`), in a dedicated environment called `dev-day-env` (ID: `env-qq71wd`). This lets me:
- Discover the exact CLI commands that work
- Catch any flags, ordering, or auth quirks
- Verify outputs match expectations

### Step 2 — Verify checkpoints & capture clean screenshots
After each CLI action, verify through CLI outputs and capture clean UI screenshots in `Lab CLI/screenshots/`, following the quality guidelines above.

### Step 3 — Write the lab doc
Once all stages are verified, write `Lab CLI/README.md`. Each section contains:
- Clear step-by-step CLI instructions
- The exact CLI commands and configurations
- Verification commands and sample outputs
- UI verification checkpoints with clean screenshots
- Transparent breakdown of what each command does

---

## Lab Stages (Checklist)

| # | Stage | Status |
|---|---|---|
| 1 | Login + environment setup | ✅ Done — environment `env-qq71wd` created |
| 2 | Create Kafka cluster | ✅ Done — cluster `lkc-383k7ww` running |
| 3 | Deploy Users Datagen connector | ✅ Done — connector `lcc-0x8k8d2` running |
| 4 | Deploy Stock Trades Datagen connector | ⏳ Pending |
| 5 | Verify topics are streaming | ⏳ Pending |
| 6 | Create Flink compute pool | ⏳ Pending |
| 7 | Flink SQL — `users_keyed` materialized table | ⏳ Pending |
| 8 | Flink SQL — `trades_enriched` materialized table | ⏳ Pending |
| 9 | Flink SQL — `trades_forecast` + query | ⏳ Pending |
| 10 | Write `Lab CLI/README.md` | ⏳ Pending |
| 11 | Cleanup section & validation | ⏳ Pending |
