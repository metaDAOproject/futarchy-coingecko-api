import { Router, type Request, type Response } from 'express';
import { PublicKey } from '@solana/web3.js';
import { parseSolanaAddress } from '../utils/validation.js';
import type { ServiceGetters } from './types.js';
import { asyncHandler, AppError } from '../middleware/errorHandler.js';
import { logger } from '../utils/logger.js';

export function createSupplyRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getSolanaService, getLaunchpadService } = services;

  // Get complete supply info for a token
  router.get('/api/supply/:mintAddress', asyncHandler(async (req: Request, res: Response) => {
    // Validate mint address
    const mintAddressResult = parseSolanaAddress(req.params.mintAddress, 'mintAddress');
    if (!mintAddressResult.success) {
      throw AppError.badRequest(mintAddressResult.error.message, 'INVALID_MINT_ADDRESS');
    }
    const mintAddress = mintAddressResult.value;
      const solanaService = getSolanaService();
      const launchpadService = getLaunchpadService();

      const allocation = await launchpadService.getTokenAllocationBreakdown(
        new PublicKey(mintAddress)
      );

      const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
        teamPerformancePackage: {
          amount: allocation.teamPerformancePackage.amount,
          address: allocation.teamPerformancePackage.address?.toString(),
        },
        futarchyAmmLiquidity: {
          amount: allocation.futarchyAmmLiquidity.amount,
          vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
        },
        meteoraLpLiquidity: {
          amount: allocation.meteoraLpLiquidity.amount,
          poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
          vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
        },
        additionalTokenAllocation: allocation.additionalTokenAllocation ? {
          amount: allocation.additionalTokenAllocation.amount,
          recipient: allocation.additionalTokenAllocation.recipient.toString(),
          claimed: allocation.additionalTokenAllocation.claimed,
          tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress?.toString(),
        } : undefined,
        daoAddress: allocation.daoAddress?.toString(),
        launchAddress: allocation.launchAddress?.toString(),
        version: allocation.version,
      });

      res.json({
        result: supplyInfo.totalSupply,
        data: supplyInfo,
      });
  }));

  // Get total supply for a token
  router.get('/api/supply/:mintAddress/total', asyncHandler(async (req: Request, res: Response) => {
    const mintAddressResult = parseSolanaAddress(req.params.mintAddress, 'mintAddress');
    if (!mintAddressResult.success) {
      throw AppError.badRequest(mintAddressResult.error.message, 'INVALID_MINT_ADDRESS');
    }

    const solanaService = getSolanaService();
    const totalSupply = await solanaService.getTotalSupply(mintAddressResult.value);

    res.json({
      result: totalSupply,
    });
  }));

  // Get circulating supply for a token
  router.get('/api/supply/:mintAddress/circulating', asyncHandler(async (req: Request, res: Response) => {
    const mintAddressResult = parseSolanaAddress(req.params.mintAddress, 'mintAddress');
    if (!mintAddressResult.success) {
      throw AppError.badRequest(mintAddressResult.error.message, 'INVALID_MINT_ADDRESS');
    }
    const mintAddress = mintAddressResult.value;

    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
      teamPerformancePackage: {
        amount: allocation.teamPerformancePackage.amount,
        address: allocation.teamPerformancePackage.address?.toString(),
      },
      futarchyAmmLiquidity: {
        amount: allocation.futarchyAmmLiquidity.amount,
        vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
      },
      meteoraLpLiquidity: {
        amount: allocation.meteoraLpLiquidity.amount,
        poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
      },
      additionalTokenAllocation: allocation.additionalTokenAllocation ? {
        amount: allocation.additionalTokenAllocation.amount,
        recipient: allocation.additionalTokenAllocation.recipient.toString(),
        claimed: allocation.additionalTokenAllocation.claimed,
        tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress?.toString(),
      } : undefined,
      daoAddress: allocation.daoAddress?.toString(),
      launchAddress: allocation.launchAddress?.toString(),
      version: allocation.version,
    });

    const response: { 
      result: string; 
      allocation?: {
        teamPerformancePackageAddress?: string;
        futarchyAmmVaultAddress?: string;
        meteoraPoolAddress?: string;
        meteoraVaultAddress?: string;
        additionalTokenAllocation?: {
          amount: string;
          recipient: string;
          claimed: boolean;
        };
        initialTokenAllocation?: {
          amount: string;
          claimed: boolean;
        };
        daoAddress?: string;
        launchAddress?: string;
        version?: string;
      };
    } = {
      result: supplyInfo.circulatingSupply,
    };
    
    if (allocation.teamPerformancePackage.address || 
        allocation.futarchyAmmLiquidity.vaultAddress || 
        allocation.meteoraLpLiquidity.poolAddress ||
        allocation.additionalTokenAllocation) {
      response.allocation = {
        teamPerformancePackageAddress: allocation.teamPerformancePackage.address?.toString(),
        futarchyAmmVaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
        meteoraPoolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        meteoraVaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
        additionalTokenAllocation: supplyInfo.allocation?.additionalTokenAllocation,
        initialTokenAllocation: supplyInfo.allocation?.initialTokenAllocation,
        daoAddress: allocation.daoAddress?.toString(),
        launchAddress: allocation.launchAddress?.toString(),
        version: allocation.version,
      };
    }

    res.json(response);
  }));

  // Jupiter-compatible circulating supply
  router.get('/api/supply/:mintAddress/jupiter/circulating', asyncHandler(async (req: Request, res: Response) => {
    const mintAddressResult = parseSolanaAddress(req.params.mintAddress, 'mintAddress');
    if (!mintAddressResult.success) {
      throw AppError.badRequest(mintAddressResult.error.message, 'INVALID_MINT_ADDRESS');
    }
    const mintAddress = mintAddressResult.value;

    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
      teamPerformancePackage: {
        amount: allocation.teamPerformancePackage.amount,
        address: allocation.teamPerformancePackage.address?.toString(),
      },
      futarchyAmmLiquidity: {
        amount: allocation.futarchyAmmLiquidity.amount,
        vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
      },
      meteoraLpLiquidity: {
        amount: allocation.meteoraLpLiquidity.amount,
        poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
      },
      additionalTokenAllocation: allocation.additionalTokenAllocation ? {
        amount: allocation.additionalTokenAllocation.amount,
        recipient: allocation.additionalTokenAllocation.recipient.toString(),
        claimed: allocation.additionalTokenAllocation.claimed,
        tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress?.toString(),
      } : undefined,
      daoAddress: allocation.daoAddress?.toString(),
      launchAddress: allocation.launchAddress?.toString(),
      version: allocation.version,
    });

    res.json({ circulatingSupply: parseFloat(supplyInfo.circulatingSupply) });
  }));

  // Jupiter-compatible total supply
  router.get('/api/supply/:mintAddress/jupiter/total', asyncHandler(async (req: Request, res: Response) => {
    const mintAddressResult = parseSolanaAddress(req.params.mintAddress, 'mintAddress');
    if (!mintAddressResult.success) {
      throw AppError.badRequest(mintAddressResult.error.message, 'INVALID_MINT_ADDRESS');
    }

    const solanaService = getSolanaService();
    const supplyInfo = await solanaService.getSupplyInfo(mintAddressResult.value);

    res.json({ totalSupply: parseFloat(supplyInfo.totalSupply) });
  }));

  return router;
}
