# Source freshness audit — November 16, 2026

This is a dated source audit, not a guarantee that mutable sources will remain unchanged. A matching release label is weaker evidence than matching source bytes. Build dates and file dates are distinct. The catalog retains the last verified artifact while a replacement is being validated.

| Dataset | November 16 finding |
|---|---|
| eoir | Published the September 2026 archive dated October 1: 174,560,256 rows in 98 source tables (including one technical schema table). All 97 prior tables retained. Independent record audit and original archive included; 159 invalid-UTF8 cases records are excluded by the documented legacy importer. |
| dol-visas | Published FY2026 Q4: 7,596,060 LCA and 1,391,471 PERM rows. All four disjoint LCA quarters included; PERM Q4 is cumulative. Five input hashes and preservation of FY2015-FY2025 typed values verified. |
| fjc-judges | Refreshed November 16: 4,078 judges, 37,807 stored rows across six tables. Seven official source CSV identities verified. |
| medicaid-enrollment | October 30 source is still latest and byte-identical to the audited input. Published 11,220 enrollment records and 44 field definitions; no rebuild needed. |
| ice-287g | Refreshed to publisher commit 906601f9901555012fb49f19c66c569af1883a0b: 3,257 agreements and 2,535 agencies, listings through November 10. Full source equality passed. |
| fec | November 9 release is the current monthly snapshot: 360,160,461 stored rows. GitHub release verifier and documentation now match Hugging Face. Live-cycle filings continue to change between monthly snapshots. |
| ipeds-db | Published 27,889,094 rows in 25 tables. Rechecked all 496 historical partitions and both 2024 Cost inputs; applied 308 additional historical revisions. Three repackaged recent archives have byte-identical CSV members. Existing schema/harmonization limits remain documented. |
| clinicaltrials | Published the November 16 AACT snapshot: 611,764 studies and 59,949,300 stored rows in 48 tables. All 49 source members strictly parsed; one header-only file omitted. Two numeric fields changed to VARCHAR to retain 29 source values. Daily source; no later named snapshot was listed at retrieval. |
| cms-medicare | All three source metadata responses match the November 2 audit; CY2024 remains the listed edition. Historical payloads were not all rehashed. |
| openpayments | PY2025 remains the listed annual edition. No claim is made that every historical payload is byte-identical. |
| fjc | Official civil, criminal and appeals pages still list the June 30, 2026 edition. |
| ussc | FY2025 remains the listed edition. |
| scdb | Official release page matches the prior audit; 2026_01 remains the listed edition. |
| onet | Official source page matches the prior audit; version 31.0 remains listed. |
| open-policing | Published source listing matches the prior audit. The mutable historical files were not all rehashed. |
| cook-sao | Closed source series through December 30, 2024; no ongoing refresh expected. |
| courtlistener | Complete current public S3 listing checked: all 23 scoped object identities (edition, size, ETag and modification time) match the prior verified September 30 release. No refresh indicated. |
| ice | Rendered publisher listing checked: all five August 2026 raw-release links and four linked 2012-2023 archive links exactly match the existing source script. No later raw release listed. Payload bytes were not rehashed. |
| cbp | Rendered current-release table checked: all 26 file identifiers and listed windows match the published source manifest, through December 2025 apprehensions and January 2026 inadmissibles. Historical archive and payload bytes were not all rechecked. |
| fars | Official NHTSA listing still ends at 2024 Annual Report File. The current national CSV ZIP was downloaded through the browser: 32,672,161 bytes and SHA-256 5112727a8c0dc91ffee27ca05bddb073934f2d192ce4fae997da767dccdbe04f exactly match the published source lock. No refresh indicated. |
| qcew | Retained 2025 annual snapshot. BLS refresh work remains stopped at the owner's prior instruction. |
| sahie | Census still lists 2024 as the latest edition, released August 2026. |
| ice-reference | Both publisher repository heads exactly match the existing source pins (facilities a311796374555f100435ae7578cac2a945eea5b1; offices 1915d7a7a5bf0c16a395102fc8140ad6cd8bc83e). |
| ice-detention-management | Publisher head exactly matches source pin 5126c6e8482cb8d28d5a7704e96a9cab0df8b528. |
| uscis | Publisher head exactly matches source pin dfe86ad18fd04ed6ed8281366d8bfb9c56716c1b. |

## Evidence and limits

Release source locks and validation records are published with the refreshed [FJC judges](https://huggingface.co/datasets/Nason/fjc-judges-database), [Medicaid enrollment](https://huggingface.co/datasets/Nason/medicaid-enrollment-database), [ICE 287(g)](https://huggingface.co/datasets/Nason/ice-287g-database), [DOL visas](https://huggingface.co/datasets/Nason/dol-visas-database), [EOIR](https://huggingface.co/datasets/Nason/eoir-database), and [ClinicalTrials](https://huggingface.co/datasets/Nason/clinicaltrials-database) snapshots. The [November 16 IPEDS replay](https://github.com/ian-nason/ipeds-database/blob/master/releases/2026-11-16/REPLAY.md) records the full historical audit, 308 additional revisions, and the reviewed change of 25 IC_PY status-flag fields from DOUBLE to VARCHAR. Existing HD close-date and GR line-label limitations remain documented; this is not a claim of complete raw-text preservation.

The [Deportation Data Project announcements](https://deportationdata.org/updates.html), [CBP source page](https://deportationdata.org/data/cbp.html), [NHTSA FARS download listing](https://www.nhtsa.gov/file-downloads?p=nhtsa%2Fdownloads%2FFARS%2F2024%2FNational%2F), and [Census SAHIE source listing](https://www.census.gov/data/datasets/time-series/demo/sahie/estimates-acs.html) support the named-release checks above.

Current ICE and CBP file-link comparisons are recorded in [ICE listing evidence](evidence/ice-browser-listing-2026-11-16.json) and [CBP listing evidence](evidence/cbp-browser-listing-2026-11-16.json). The [FARS byte comparison](evidence/fars-byte-check-2026-11-16.json) verifies the complete current national CSV archive. Browser access resolved the earlier direct-request HTTP 403 limitations for these checks.

The November 16 refreshes are published. No additional out-of-date named release was identified within this audit scope. BLS refresh work remains excluded at the owner's instruction. Not all mutable historical payloads were rehashed. This audit therefore distinguishes current listed editions from verified byte identities; it does not guarantee that no undiscovered historical revision exists. Source updates may occur after this audit; no automatic future schedule is implied.
