# Source freshness audit — November 16, 2026

This is a dated source audit, not a guarantee that mutable sources will remain unchanged. A matching release label is weaker evidence than matching source bytes. Build dates and file dates are distinct. The catalog retains the last verified artifact while a replacement is being validated.

| Dataset | November 16 finding |
|---|---|
| eoir | September 2026 archive is newer than the hosted snapshot. Rebuild and independent record audit in progress; catalog still points to the prior verified file. |
| dol-visas | Published FY2026 Q4: 7,596,060 LCA and 1,391,471 PERM rows. All four disjoint LCA quarters included; PERM Q4 is cumulative. Five input hashes and preservation of FY2015-FY2025 typed values verified. |
| fjc-judges | Refreshed November 16: 4,078 judges, 37,807 stored rows across six tables. Seven official source CSV identities verified. |
| medicaid-enrollment | October 30 source is still latest and byte-identical to the audited input. Published 11,220 enrollment records and 44 field definitions; no rebuild needed. |
| ice-287g | Refreshed to publisher commit 906601f9901555012fb49f19c66c569af1883a0b: 3,257 agreements and 2,535 agencies, listings through November 10. Full source equality passed. |
| fec | November 9 release is the current monthly snapshot: 360,160,461 stored rows. GitHub release verifier and documentation now match Hugging Face. Live-cycle filings continue to change between monthly snapshots. |
| ipeds-db | November 9 release has 25 tables and 27,889,050 rows. Only 60 of 496 historical partitions were rechecked for that release. Remaining 436 archive directories are now being audited; full historical currency is not certified. |
| clinicaltrials | Refresh remains required. AACT snapshot download requires account access; no newly authenticated snapshot was acquired. |
| cms-medicare | All three source metadata responses match the November 2 audit; CY2024 remains the listed edition. Historical payloads were not all rehashed. |
| openpayments | PY2025 remains the listed annual edition. No claim is made that every historical payload is byte-identical. |
| fjc | Official civil, criminal and appeals pages still list the June 30, 2026 edition. |
| ussc | FY2025 remains the listed edition. |
| scdb | Official release page matches the prior audit; 2026_01 remains the listed edition. |
| onet | Official source page matches the prior audit; version 31.0 remains listed. |
| open-policing | Published source listing matches the prior audit. The mutable historical files were not all rehashed. |
| cook-sao | Closed source series through December 30, 2024; no ongoing refresh expected. |
| courtlistener | Complete current public S3 listing checked: all 23 scoped object identities (edition, size, ETag and modification time) match the prior verified September 30 release. No refresh indicated. |
| ice | Publisher announcements list the August 2026 raw release and later processed-data changes. Direct file-list access returned HTTP 403; complete raw-link identity revalidation is unresolved. |
| cbp | Publisher page still describes the same December 2025/January 2026 coverage. Direct file-list access returned HTTP 403; complete file identity revalidation is unresolved. |
| fars | Official NHTSA download listing still ends at 2024 and lists the national CSV ZIP as updated April 1, 2026. NHTSA still labels 2024 as the Annual Report File; no later named release indicated. Direct payload access returned HTTP 403, so its bytes were not rehashed. |
| qcew | Retained 2025 annual snapshot. BLS refresh work remains stopped at the owner's prior instruction. |
| sahie | Census still lists 2024 as the latest edition, released August 2026. |
| ice-reference | Both publisher repository heads exactly match the existing source pins (facilities a311796374555f100435ae7578cac2a945eea5b1; offices 1915d7a7a5bf0c16a395102fc8140ad6cd8bc83e). |
| ice-detention-management | Publisher head exactly matches source pin 5126c6e8482cb8d28d5a7704e96a9cab0df8b528. |
| uscis | Publisher head exactly matches source pin dfe86ad18fd04ed6ed8281366d8bfb9c56716c1b. |

## Evidence and limits

Release source locks and validation records are published with the refreshed [FJC judges](https://huggingface.co/datasets/Nason/fjc-judges-database), [Medicaid enrollment](https://huggingface.co/datasets/Nason/medicaid-enrollment-database), and [ICE 287(g)](https://huggingface.co/datasets/Nason/ice-287g-database) snapshots. [IPEDS release scope](https://github.com/ian-nason/ipeds-database/blob/master/releases/2026-11-09/REPLAY.md) records the historical coverage limit. The [Deportation Data Project announcements](https://deportationdata.org/updates.html), [CBP source page](https://deportationdata.org/data/cbp.html), and [Census SAHIE source listing](https://www.census.gov/data/datasets/time-series/demo/sahie/estimates-acs.html) support the named-release checks above.

The remaining blockers and incomplete checks mean that **“no further refreshes are required” is not established**. Source updates may be discovered after this audit; no automatic future schedule is implied.
