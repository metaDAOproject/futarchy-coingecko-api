#!/usr/bin/env bun
/**
 * Backfill script - Run a full data insert from Dune to PostgreSQL
 * 
 * Usage:
 *   bun run scripts/backfill.ts [ten-minute]
 * 
 * Note: Hourly and daily data are now aggregated from 10-minute data using DB functions.
 * This script only supports 10-minute backfill. Hourly and daily will be automatically
 * aggregated from the 10-minute data.
 * 
 * Required environment variables:
 *   - DATABASE_URL: PostgreSQL connection string
 *   - DUNE_API_KEY: Your Dune API key
 *   - DUNE_TEN_MINUTE_VOLUME_QUERY_ID: For 10-minute volumes
 */

import { config } from '../src/config';

// Dynamically import services to avoid circular deps
async function main() {
  const args = process.argv.slice(2);
  const mode = args[0] || 'all';
  
  console.log('🚀 Starting backfill script...');
  console.log(`📊 Mode: ${mode}`);
  console.log('');
  
  // Check required env vars
  if (!process.env.DATABASE_URL) {
    console.error('❌ DATABASE_URL is required');
    process.exit(1);
  }
  
  if (!process.env.DUNE_API_KEY) {
    console.error('❌ DUNE_API_KEY is required');
    process.exit(1);
  }
  
  // Import services
  const { DatabaseService } = await import('../src/services/databaseService');
  const { DuneService } = await import('../src/services/duneService');
  const { FutarchyService } = await import('../src/services/futarchyService');
  
  // Initialize database
  const databaseService = new DatabaseService();
  await databaseService.initialize();
  
  if (!databaseService.isAvailable()) {
    console.error('❌ Failed to connect to database');
    process.exit(1);
  }
  
  console.log('✅ Database connected');
  
  // Initialize Dune service (uses config internally)
  const duneService = new DuneService();
  
  // Initialize FutarchyService for token list (uses config internally)
  const futarchyService = new FutarchyService();
  
  const allDaos = await futarchyService.getAllDaos();
  const tokenAddresses = allDaos.map(dao => dao.baseMint.toString());
  console.log(`📋 Found ${tokenAddresses.length} DAOs to backfill`);
  console.log('');
  
  try {
    // Only support ten-minute mode - hourly and daily are aggregated from 10-minute data
    if (mode === 'ten-minute' || mode === 'all' || !mode) {
      await backfillTenMinute(databaseService, duneService, tokenAddresses);
    } else if (mode === 'daily' || mode === 'hourly') {
      console.log(`⚠️  ${mode} mode is deprecated. Hourly and daily data are now aggregated from 10-minute data.`);
      console.log('   Please use "ten-minute" mode instead. Hourly and daily will be automatically aggregated.');
      process.exit(1);
    }
    
    console.log('');
    console.log('✅ Backfill complete!');
    console.log('   Note: Hourly and daily records will be automatically aggregated from 10-minute data.');
    
    // Print summary
    console.log('');
    console.log('📊 Database summary:');
    const tenMinCount = await databaseService.getTenMinuteRecordCount();
    console.log(`   10-minute records: ${tenMinCount}`);
    console.log('   (Hourly and daily records are aggregated from 10-minute data)');
    
  } catch (error: any) {
    console.error('❌ Backfill failed:', error.message);
    process.exit(1);
  } finally {
    await databaseService.close();
  }
}

async function backfillTenMinute(
  databaseService: any,
  duneService: any,
  tokenAddresses: string[]
) {
  const queryId = config.dune.tenMinuteVolumeQueryId;
  if (!queryId) {
    console.log('⏭️  Skipping 10-minute backfill - DUNE_TEN_MINUTE_VOLUME_QUERY_ID not set');
    return;
  }
  
  console.log('⏱️  Starting 10-MINUTE volume backfill...');
  console.log(`   Query ID: ${queryId}`);
  
  // Always backfill from the beginning
  const startTime = new Date('2026-01-24T00:00:00Z'); // Futarchy launch date
  const startTimeStr = startTime.toISOString().slice(0, 19).replace('T', ' ');
  console.log(`   Start time: ${startTimeStr} (always from beginning)`);
  
  const fetchStart = Date.now();
  
  // Build token list parameter
  const tokenListParam = tokenAddresses.length > 0 
    ? tokenAddresses.map(t => `'${t}'`).join(',')
    : "'__ALL__'";
  
  // Execute query
  const result = await duneService.executeQueryManually(queryId, {
    start_time: startTimeStr,
    token_list: tokenListParam,
  });
  
  if (!result || !result.rows || result.rows.length === 0) {
    console.log('   No data returned from Dune');
    return;
  }
  
  console.log(`   Fetched ${result.rows.length} rows from Dune`);
  
  // Debug: show first row structure
  if (result.rows.length > 0) {
    console.log(`   Sample row keys: ${Object.keys(result.rows[0]).join(', ')}`);
    console.log(`   Sample row:`, JSON.stringify(result.rows[0], null, 2));
  }
  
  // Transform and insert - Dune field names match DB field names
  const records = result.rows.map((row: any) => ({
    token: row.token,
    bucket: row.bucket,
    base_volume: row.base_volume || '0',
    target_volume: row.target_volume || '0',
    trade_count: parseInt(row.trade_count || row.num_swaps) || 0,
    high: row.high || '0',
    low: row.low || '0',
  }));
  
  const upserted = await databaseService.upsertTenMinuteVolumes(records);
  const duration = Date.now() - fetchStart;
  
  console.log(`   ✅ Upserted ${upserted} 10-minute records in ${duration}ms`);
  // Note: Not pruning data - keeping all historical records
}

main().catch(console.error);

