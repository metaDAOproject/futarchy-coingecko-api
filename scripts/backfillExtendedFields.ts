/**
 * Backfill Script for Extended Fields
 * 
 * This script safely backfills missing extended fields in existing database records.
 * It:
 * 1. Identifies 10-minute records missing extended fields
 * 2. Optionally re-fetches those records from Dune using the extended query (if SKIP_DUNE not set)
 * 3. Updates only the missing fields (preserves existing data)
 * 4. Aggregates hourly and daily records from the updated 10-minute data
 * 
 * Run this BEFORE starting services to ensure all existing data has extended fields.
 * 
 * Usage: 
 *   bun run scripts/backfillExtendedFields.ts
 *   SKIP_DUNE=true bun run scripts/backfillExtendedFields.ts  # Skip Dune, use DB aggregation only
 *   RECENT_DAYS=7 bun run scripts/backfillExtendedFields.ts   # Only backfill last 7 days from Dune
 */

import { DatabaseService } from '../src/services/databaseService.js';
import { DuneService } from '../src/services/duneService.js';
import { FutarchyService } from '../src/services/futarchyService.js';
import { TenMinuteVolumeFetcherService } from '../src/services/tenMinuteVolumeFetcherService.js';
import { config } from '../src/config.js';

async function backfillExtendedFields() {
  console.log('=== Starting Extended Fields Backfill ===\n');

  // Check environment variables for options
  const skipDune = process.env.SKIP_DUNE === 'true';
  const recentDays = process.env.RECENT_DAYS ? parseInt(process.env.RECENT_DAYS) : null;

  if (skipDune) {
    console.log('⚠ SKIP_DUNE=true: Will skip Dune backfill and use database aggregation only\n');
  }
  if (recentDays) {
    console.log(`⚠ RECENT_DAYS=${recentDays}: Will only backfill last ${recentDays} days from Dune\n`);
  }

  // Initialize services
  const databaseService = new DatabaseService();
  const duneService = new DuneService();
  const futarchyService = new FutarchyService();
  const tenMinuteService = new TenMinuteVolumeFetcherService(duneService, databaseService, futarchyService);

  try {
    // 1. Connect to database
    console.log('1. Connecting to database...');
    const dbConnected = await databaseService.initialize();
    if (!dbConnected) {
      console.error('Failed to connect to database');
      process.exit(1);
    }
    console.log('✓ Database connected\n');

    // 2. Check for existing 10-minute records with missing extended fields
    console.log('2. Checking for records with missing extended fields...');
    if (!databaseService.pool) {
      console.error('Database pool not available');
      process.exit(1);
    }

    const missingFieldsCheck = await databaseService.pool.query(`
      SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN average_price IS NULL OR average_price = 0 THEN 1 END) as missing_average_price,
        MIN(bucket) as earliest_bucket,
        MAX(bucket) as latest_bucket
      FROM ten_minute_volumes
    `);

    const stats = missingFieldsCheck.rows[0];
    console.log(`   Total 10-minute records: ${stats.total_records}`);
    console.log(`   Missing average_price (needs backfill): ${stats.missing_average_price}`);
    console.log(`   Date range: ${stats.earliest_bucket} to ${stats.latest_bucket}\n`);

    if (parseInt(stats.missing_average_price) === 0) {
      console.log('✓ All 10-minute records already have extended fields\n');
    } else if (skipDune) {
      console.log('3. Skipping Dune backfill (SKIP_DUNE=true)\n');
      console.log('   Will use database aggregation to fill missing fields in hourly/daily tables\n');
    } else {
      // 3. Find the earliest missing average_price date
      console.log('3. Finding earliest missing average_price date...');
      
      const earliestMissingResult = await databaseService.pool.query(`
        SELECT MIN(bucket) as earliest_missing
        FROM ten_minute_volumes
        WHERE (average_price IS NULL OR average_price = 0)
      `);

      const earliestMissing = earliestMissingResult.rows[0].earliest_missing;
      
      if (!earliestMissing) {
        console.log('   ✓ No missing average_price records found\n');
      } else {
        // Use the earliest missing date as start, but truncate to beginning of day
        let startDate = new Date(earliestMissing);
        startDate.setHours(0, 0, 0, 0);
        
        // Apply RECENT_DAYS filter if set
        if (recentDays) {
          const cutoffDate = new Date();
          cutoffDate.setDate(cutoffDate.getDate() - recentDays);
          cutoffDate.setHours(0, 0, 0, 0);
          if (cutoffDate > startDate) {
            startDate = cutoffDate;
            console.log(`   Using RECENT_DAYS=${recentDays} filter: starting from ${startDate.toISOString().split('T')[0]}\n`);
          } else {
            console.log(`   Starting from earliest missing date: ${startDate.toISOString().split('T')[0]}\n`);
          }
        } else {
          console.log(`   Starting from earliest missing date: ${startDate.toISOString().split('T')[0]}\n`);
        }

        // 4. Get specific buckets that need backfilling
        console.log('4. Identifying specific buckets that need backfilling...');
        
        const dateFilter = `AND bucket >= '${startDate.toISOString()}'`;

        const missingBucketsResult = await databaseService.pool.query(`
          SELECT DISTINCT 
            date_trunc('day', bucket)::DATE as date,
            COUNT(*) as missing_count
          FROM ten_minute_volumes
          WHERE (average_price IS NULL OR average_price = 0)
            ${dateFilter}
          GROUP BY date_trunc('day', bucket)::DATE
          ORDER BY date_trunc('day', bucket)::DATE ASC
        `);

        const missingDays = missingBucketsResult.rows;
        console.log(`   Found ${missingDays.length} days with missing extended fields\n`);

        if (missingDays.length === 0) {
          console.log('✓ No days need backfilling\n');
        } else {
          // Show summary
          const totalMissing = missingDays.reduce((sum, day) => sum + parseInt(day.missing_count), 0);
          console.log(`   Total buckets needing backfill: ${totalMissing}`);
          if (missingDays.length > 0) {
            console.log(`   Date range: ${missingDays[0].date} to ${missingDays[missingDays.length - 1].date}\n`);
          }

          // Warn if too many days
          if (missingDays.length > 30 && !recentDays) {
            console.log(`⚠ WARNING: ${missingDays.length} days need backfilling. This may use significant Dune datapoints.`);
            console.log(`   Options:`);
            console.log(`   - Set RECENT_DAYS=30 to only backfill last 30 days`);
            console.log(`   - Set SKIP_DUNE=true to skip Dune and use DB aggregation only`);
            console.log(`   - Check your Dune subscription limits\n`);
          }

          // Backfill in 7-day increments (1008 buckets per chunk = 7 days * 24 hours * 6 buckets/hour)
          console.log('5. Backfilling 10-minute records from Dune (7-day chunks)...');
          console.log(`   Processing ${missingDays.length} days in 7-day increments (1008 buckets per chunk)...\n`);

        let totalBackfilled = 0;
        let totalUpdated = 0;
        let errors = 0;
        let apiLimitHit = false;

        // Group days into 7-day chunks
        const chunkSize = 7;
        const chunks: Array<{ startDate: Date; endDate: Date; days: any[] }> = [];
        
        for (let i = 0; i < missingDays.length; i += chunkSize) {
          const chunkDays = missingDays.slice(i, i + chunkSize);
          const startDate = new Date(chunkDays[0].date);
          startDate.setHours(0, 0, 0, 0);
          
          const endDate = new Date(chunkDays[chunkDays.length - 1].date);
          endDate.setHours(23, 59, 59, 999);
          endDate.setDate(endDate.getDate() + 1); // Exclusive end (start of next day)
          
          chunks.push({ startDate, endDate, days: chunkDays });
        }

        // Process chunks in chronological order (oldest first)
        for (let chunkIdx = 0; chunkIdx < chunks.length; chunkIdx++) {
          const chunk = chunks[chunkIdx];
          const startTime = chunk.startDate.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
          const endTime = chunk.endDate.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
          
          const totalMissingInChunk = chunk.days.reduce((sum, day) => sum + parseInt(day.missing_count), 0);
          const dayRange = `${chunk.days[0].date} to ${chunk.days[chunk.days.length - 1].date}`;

          console.log(`   [${chunkIdx + 1}/${chunks.length}] ${dayRange} (${totalMissingInChunk} missing buckets across ${chunk.days.length} days)...`);
          
          try {
            // Fetch 7-day chunk from Dune with explicit start_time and end_time (will only update missing fields)
            const count = await tenMinuteService.backfillExtendedFields(startTime, endTime);
            if (count > 0) {
              totalUpdated += count;
              console.log(`      ✓ Updated ${count} records`);
            } else {
              console.log(`      - Skipped (no missing fields)`);
            }

            totalBackfilled++;
            
            // Add delay between chunks to avoid rate limits (3 seconds between 7-day chunks)
            if (chunkIdx < chunks.length - 1) {
              await new Promise(resolve => setTimeout(resolve, 3000));
            }
          } catch (error: any) {
            errors++;
            if (error.message.includes('402') || error.message.includes('Payment Required')) {
              apiLimitHit = true;
              console.error(`\n      ✗ Dune API limit reached!`);
              console.error(`\n   Processed ${totalBackfilled} chunks (${totalBackfilled * 7} days), updated ${totalUpdated} records before limit.`);
              console.error(`\n   Options to continue:`);
              console.error(`   1. Wait for your Dune billing cycle to reset`);
              console.error(`   2. Upgrade your Dune subscription`);
              console.error(`   3. Run with SKIP_DUNE=true to use database aggregation only:`);
              console.error(`      SKIP_DUNE=true bun run scripts/backfillExtendedFields.ts`);
              console.error(`   4. Resume later - script will skip already-filled records`);
              console.error(`\n   Note: New data will automatically include extended fields going forward.\n`);
              break;
            } else {
              console.error(`      ✗ Error: ${error.message}`);
              // Continue with next chunk on non-limit errors
            }
          }

          if (apiLimitHit) break;
        }

        if (!apiLimitHit) {
          console.log(`\n✓ Backfilled ${totalBackfilled} chunks from Dune`);
          console.log(`  Total records updated: ${totalUpdated}`);
          if (errors > 0) {
            console.log(`  Errors encountered: ${errors}`);
          }
          console.log('');
        } else {
          console.log('\n⚠ Backfill incomplete due to API limits. See options above.\n');
        }
      }
      }
    }

    // 6. Run database aggregation backfill (always run this - it's free, no Dune calls)
    console.log('6. Running database aggregation backfill...');
    console.log('   (This aggregates from existing 10-minute data - no Dune API calls)\n');
    const backfillResults = await databaseService.backfillMissingFields();
    console.log(`   Updated hourly records: ${backfillResults.hourlyUpdated}`);
    console.log(`   Updated daily records: ${backfillResults.dailyUpdated}\n`);
    
    if (backfillResults.hourlyUpdated > 0 || backfillResults.dailyUpdated > 0) {
      console.log('   ✓ Database aggregation completed - hourly/daily records now have extended fields\n');
    } else {
      console.log('   ✓ No hourly/daily records needed updating\n');
    }

    // 7. Verify final state
    console.log('7. Verifying final state...');
    const finalCheck = await databaseService.pool.query(`
      SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN average_price IS NULL OR average_price = 0 THEN 1 END) as missing_average_price
      FROM ten_minute_volumes
    `);

    const finalStats = finalCheck.rows[0];
    console.log(`   Total 10-minute records: ${finalStats.total_records}`);
    console.log(`   Still missing average_price: ${finalStats.missing_average_price}\n`);

    if (parseInt(finalStats.missing_average_price) === 0) {
      console.log('✓ All records now have extended fields!\n');
    } else {
      console.log('⚠ Some records still missing fields (may need additional backfill)\n');
    }

    console.log('=== Backfill Completed ===');
    process.exit(0);
  } catch (error: any) {
    console.error('Error during backfill:', error);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run the backfill
backfillExtendedFields();
