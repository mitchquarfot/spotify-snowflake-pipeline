# Archive - Non-Essential SQL Scripts

This archive contains SQL scripts that are not part of the core quick start process but may be useful for reference or advanced use cases.

## 📁 Folder Structure

### `sql_iterations/`
Alternative and experimental versions of the medallion architecture:
- `medallion_architecture_views_backup.sql` - Backup copy of the main architecture (identical to current)
- `medallion_min_fixes.sql` - Development script to fix MIN() aggregation issues (superseded)
- `fix_medallion_simple_approach.sql` - Alternative simplified approach (superseded)

### `utilities/`
Specific table refresh utilities (replaced by `refresh_all_dynamic_tables.sql` in root):
- `refresh_primary_deduped_table.sql` - Refresh only the primary deduped table
- `refresh_artist_genres_table.sql` - Refresh only the artist genres table

### `analysis_scripts/`
One-time analysis and data cleanup scripts:
- `identify_and_enhance_empty_genres.sql` - Analysis of artists with missing genres
- `find_and_populate_missing_artists.sql` - Find missing artists in genre data
- `clean_and_repopulate_artists.sql` - One-time cleanup for artist data

### `medallion_iterations/`
Early iterations of medallion architecture development:
- `fix_medallion_full_refresh.sql` - Full refresh approach
- `fix_medallion_min_issues.sql` - MIN() aggregation fixes 
- `fix_medallion_without_views.sql` - Version without utility views

### `misc_scripts/`
Miscellaneous Python scripts for data processing:
- Various artist genre processing utilities
- Force reprocessing scripts
- Enhanced genre classifiers

## 🎯 Why Archived?

These files were moved to keep the root directory clean and focused on the **core quick start experience**:

### ✅ **Core Files Remaining in Root:**
- `snowflake_setup.sql` - Initial Snowflake setup (essential)
- `medallion_architecture_views.sql` - Complete medallion architecture (essential)
- `refresh_all_dynamic_tables.sql` - Operational utility (useful)
- `snowflake_external_access.sql` - Advanced feature (extended functionality)

### 📦 **Archived Files:**
- **Development iterations** - No longer needed since final version exists
- **Backup copies** - Identical to current files
- **One-time scripts** - Used during initial development/data cleanup
- **Specific utilities** - Replaced by comprehensive refresh script

## 🔄 If You Need These Files

All archived files are still functional. If you need them:

1. **Copy them back to root** if needed for specific use cases
2. **Reference for troubleshooting** - these show the development evolution
3. **Extract specific queries** - many contain useful analysis patterns

## 📚 Related Documentation

- Main project: `../README.md`
- Quick start: `../QUICK_START.md`
- Current SQL structure focuses on simplicity and getting started quickly

---

*Files archived on: $(date)*
*Archive reason: Simplify quick start process and focus on essential components*
