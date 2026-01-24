/**
 * Scheduling utilities to prevent refresh pileup.
 * 
 * Uses recursive setTimeout instead of setInterval to ensure
 * the next execution only starts after the current one completes.
 * This prevents overlapping executions when tasks take longer than expected.
 */

import { logger } from './logger.js';

export interface ScheduledTask {
  stop: () => void;
  isRunning: () => boolean;
  getLastRunTime: () => Date | null;
  getNextRunTime: () => Date | null;
}

export interface ScheduleOptions {
  name: string;
  intervalMs: number;
  immediate?: boolean;
  onError?: (error: Error) => void;
}

/**
 * Schedule a task to run at fixed intervals WITHOUT pileup.
 * Uses recursive setTimeout to ensure the next run only starts
 * after the current run completes + intervalMs.
 */
export function scheduleWithoutPileup(
  task: () => Promise<void>,
  options: ScheduleOptions
): ScheduledTask {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let running = false;
  let lastRunTime: Date | null = null;
  let nextRunTime: Date | null = null;
  let stopped = false;

  const scheduleNext = () => {
    if (stopped) return;
    
    nextRunTime = new Date(Date.now() + options.intervalMs);
    timeoutId = setTimeout(runTask, options.intervalMs);
  };

  const runTask = async () => {
    if (stopped) return;
    running = true;
    lastRunTime = new Date();
    
    try {
      await task();
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      if (options.onError) {
        options.onError(err);
      } else {
        logger.error(`[${options.name}] Task error:`, err);
      }
    } finally {
      running = false;
      scheduleNext();
    }
  };

  if (options.immediate) {
    runTask();
  } else {
    scheduleNext();
  }

  return {
    stop: () => {
      stopped = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      logger.info(`[${options.name}] Scheduled task stopped`);
    },
    isRunning: () => running,
    getLastRunTime: () => lastRunTime,
    getNextRunTime: () => nextRunTime,
  };
}

/**
 * Schedule a task to run at specific time boundaries (e.g., every 10 minutes at :00, :10, :20...).
 * Uses recursive setTimeout to prevent pileup.
 */
export function scheduleAtBoundary(
  task: () => Promise<void>,
  options: {
    name: string;
    boundaryMinutes: number;
    bufferSeconds?: number;
    onError?: (error: Error) => void;
  }
): ScheduledTask {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let running = false;
  let lastRunTime: Date | null = null;
  let nextRunTime: Date | null = null;
  let stopped = false;

  const getNextBoundary = (): Date => {
    const now = new Date();
    const currentMinute = now.getMinutes();
    const nextBoundaryMinute = Math.ceil((currentMinute + 1) / options.boundaryMinutes) * options.boundaryMinutes;
    
    const next = new Date(now);
    if (nextBoundaryMinute >= 60) {
      next.setHours(next.getHours() + 1);
      next.setMinutes(0, options.bufferSeconds ?? 0, 0);
    } else {
      next.setMinutes(nextBoundaryMinute, options.bufferSeconds ?? 0, 0);
    }
    next.setMilliseconds(0);
    
    return next;
  };

  const scheduleNext = () => {
    if (stopped) return;
    
    nextRunTime = getNextBoundary();
    const msUntilNext = nextRunTime.getTime() - Date.now();
    
    logger.info(`[${options.name}] Next run at ${nextRunTime.toISOString()} (in ${Math.round(msUntilNext / 1000)}s)`);
    timeoutId = setTimeout(runTask, msUntilNext);
  };

  const runTask = async () => {
    if (stopped) return;
    running = true;
    lastRunTime = new Date();
    
    try {
      await task();
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      if (options.onError) {
        options.onError(err);
      } else {
        logger.error(`[${options.name}] Task error:`, err);
      }
    } finally {
      running = false;
      scheduleNext();
    }
  };

  scheduleNext();

  return {
    stop: () => {
      stopped = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      logger.info(`[${options.name}] Scheduled task stopped`);
    },
    isRunning: () => running,
    getLastRunTime: () => lastRunTime,
    getNextRunTime: () => nextRunTime,
  };
}

/**
 * Schedule a task to run at a specific UTC hour each day.
 * Uses recursive setTimeout to prevent pileup.
 */
export function scheduleDailyAtUTC(
  task: () => Promise<void>,
  options: {
    name: string;
    hourUTC: number;
    minuteUTC?: number;
    onError?: (error: Error) => void;
  }
): ScheduledTask {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let running = false;
  let lastRunTime: Date | null = null;
  let nextRunTime: Date | null = null;
  let stopped = false;

  const getNextRun = (): Date => {
    const now = new Date();
    const next = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      options.hourUTC,
      options.minuteUTC ?? 0,
      0,
      0
    ));
    
    if (next.getTime() <= now.getTime()) {
      next.setUTCDate(next.getUTCDate() + 1);
    }
    
    return next;
  };

  const scheduleNext = () => {
    if (stopped) return;
    
    nextRunTime = getNextRun();
    const msUntilNext = nextRunTime.getTime() - Date.now();
    
    logger.info(`[${options.name}] Next run at ${nextRunTime.toISOString()} (in ${Math.round(msUntilNext / 3600000)}h)`);
    timeoutId = setTimeout(runTask, msUntilNext);
  };

  const runTask = async () => {
    if (stopped) return;
    running = true;
    lastRunTime = new Date();
    
    try {
      await task();
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      if (options.onError) {
        options.onError(err);
      } else {
        logger.error(`[${options.name}] Task error:`, err);
      }
    } finally {
      running = false;
      scheduleNext();
    }
  };

  scheduleNext();

  return {
    stop: () => {
      stopped = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      logger.info(`[${options.name}] Scheduled task stopped`);
    },
    isRunning: () => running,
    getLastRunTime: () => lastRunTime,
    getNextRunTime: () => nextRunTime,
  };
}
