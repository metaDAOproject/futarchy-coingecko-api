/**
 * Backfill Script for Meteora Daily Volumes
 * 
 * This script backfills historical Meteora pool fee data from Dune.
 * It fetches data from 2025-10-09 to present and stores it in the database.
 * 
 * Usage: 
 *   bun run scripts/backfillMeteoraVolumes.ts
 *   RECENT_DAYS=7 bun run scripts/backfillMeteoraVolumes.ts   # Only backfill last 7 days
 */

import { DatabaseService } from '../src/services/databaseService.js';
import { DuneService } from '../src/services/duneService.js';
import { MeteoraVolumeFetcherService } from '../src/services/meteoraVolumeFetcherService.js';
import { config } from '../src/config.js';

async function backfillMeteoraVolumes() {
  console.log('=== Starting Meteora Volumes Backfill ===\n');

  // Check environment variables for options
  const recentDays = process.env.RECENT_DAYS ? parseInt(process.env.RECENT_DAYS) : null;

  if (recentDays) {
    console.log(`⚠ RECENT_DAYS=${recentDays}: Will only backfill last ${recentDays} days\n`);
  }

  // Initialize services
  const databaseService = new DatabaseService();
  const duneService = new DuneService();
  const meteoraService = new MeteoraVolumeFetcherService(duneService, databaseService);

  try {
    // 1. Connect to database
    console.log('1. Connecting to database...');
    const dbConnected = await databaseService.initialize();
    if (!dbConnected) {
      console.error('Failed to connect to database');
      process.exit(1);
    }
    console.log('✓ Database connected\n');

    // 2. Check for existing records
    console.log('2. Checking for existing records...');
    if (!databaseService.pool) {
      console.error('Database pool not available');
      process.exit(1);
    }

    const existingCheck = await databaseService.pool.query(`
      SELECT 
        COUNT(*) as total_records,
        MIN(date) as earliest_date,
        MAX(date) as latest_date
      FROM daily_meteora_volumes
    `);

    const stats = existingCheck.rows[0];
    console.log(`   Total records: ${stats.total_records}`);
    console.log(`   Date range: ${stats.earliest_date || 'N/A'} to ${stats.latest_date || 'N/A'}\n`);

    // 3. Determine start date
    const startDate = new Date('2025-10-09');
    let actualStartDate = startDate;

    if (recentDays) {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - recentDays);
      if (cutoffDate > startDate) {
        actualStartDate = cutoffDate;
        console.log(`   Limiting to last ${recentDays} days (since ${actualStartDate.toISOString().split('T')[0]})\n`);
      } else {
        console.log(`   Starting from ${startDate.toISOString().split('T')[0]} (launch date)\n`);
      }
    } else {
      console.log(`   Starting from ${startDate.toISOString().split('T')[0]} (launch date)\n`);
    }

    // 4. Check if query ID is configured
    if (!config.dune.meteoraVolumeQueryId) {
      console.error('❌ DUNE_METEORA_VOLUME_QUERY_ID not configured');
      console.error('   Please set DUNE_METEORA_VOLUME_QUERY_ID in your .env file');
      process.exit(1);
    }

    console.log(`3. Using Dune query ID: ${config.dune.meteoraVolumeQueryId}\n`);

    // 5. Initialize the service
    console.log('4. Initializing Meteora volume service...');
    await meteoraService.initialize();
    console.log('✓ Service initialized\n');

    // 6. Perform backfill
    console.log('5. Starting backfill from Dune...');
    const startDateStr = actualStartDate.toISOString().split('T')[0]!;
    
    try {
      const result = await meteoraService.forceRefresh();
      
      if (result.success) {
        console.log(`✓ Backfill completed successfully`);
        console.log(`  ${result.message}`);
        if (result.recordsUpserted !== undefined) {
          console.log(`  Records upserted: ${result.recordsUpserted}`);
        }
      } else {
        console.error(`✗ Backfill failed: ${result.message}`);
        process.exit(1);
      }
    } catch (error: any) {
      console.error('✗ Backfill error:', error.message);
      if (error.message?.includes('402') || error.message?.includes('Payment Required')) {
        console.error('\n   Dune API limit reached!');
        console.error('   Options to continue:');
        console.error('   1. Wait for your Dune billing cycle to reset');
        console.error('   2. Upgrade your Dune subscription');
        console.error('   3. Resume later - script will skip already-filled records');
      }
      throw error;
    }

    // 7. Verify final state
    console.log('\n6. Verifying final state...');
    const finalCheck = await databaseService.pool.query(`
      SELECT 
        COUNT(*) as total_records,
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(DISTINCT token) as unique_tokens
      FROM daily_meteora_volumes
    `);

    const finalStats = finalCheck.rows[0];
    console.log(`   Total records: ${finalStats.total_records}`);
    console.log(`   Date range: ${finalStats.earliest_date || 'N/A'} to ${finalStats.latest_date || 'N/A'}`);
    console.log(`   Unique tokens: ${finalStats.unique_tokens}\n`);

    console.log('=== Backfill Completed ===');
    process.exit(0);
  } catch (error: any) {
    console.error('Error during backfill:', error);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run the backfill
backfillMeteoraVolumes();
