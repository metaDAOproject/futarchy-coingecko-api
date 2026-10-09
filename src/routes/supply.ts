import { Router, type Request, type Response } from 'express';
import { parseSolanaAddress, orBadRequest } from '../utils/validation.js';
import type { ServiceGetters } from './types.js';
import { asyncHandler } from '../middleware/errorHandler.js';
import { getSupplyInfoWithLaunchpadAllocation } from '../services/supplyWithLaunchpadAllocation.js';

export function createSupplyRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getSolanaService, getLaunchpadService } = services;

  // Get complete supply info for a token
  router.get('/api/supply/:mintAddress', asyncHandler(async (req: Request, res: Response) => {
    const mintAddress = orBadRequest(parseSolanaAddress(req.params.mintAddress, 'mintAddress'), 'INVALID_MINT_ADDRESS');
    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    const { supplyInfo } = await getSupplyInfoWithLaunchpadAllocation(
      mintAddress,
      solanaService,
      launchpadService,
    );

    res.json({
      result: supplyInfo.totalSupply,
      data: supplyInfo,
    });
  }));

  // Get total supply for a token
  router.get('/api/supply/:mintAddress/total', asyncHandler(async (req: Request, res: Response) => {
    const mintAddress = orBadRequest(parseSolanaAddress(req.params.mintAddress, 'mintAddress'), 'INVALID_MINT_ADDRESS');

    const solanaService = getSolanaService();
    const totalSupply = await solanaService.getTotalSupply(mintAddress);

    res.json({
      result: totalSupply,
    });
  }));

  // Get circulating supply for a token
  router.get('/api/supply/:mintAddress/circulating', asyncHandler(async (req: Request, res: Response) => {
    const mintAddress = orBadRequest(parseSolanaAddress(req.params.mintAddress, 'mintAddress'), 'INVALID_MINT_ADDRESS');

    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    const { supplyInfo, allocation } = await getSupplyInfoWithLaunchpadAllocation(
      mintAddress,
      solanaService,
      launchpadService,
    );

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
        daoTreasuryTokens?: {
          amount: string;
          vaultAddress?: string;
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
        allocation.additionalTokenAllocation ||
        !allocation.daoTreasuryTokens.amount.isZero()) {
      response.allocation = {
        teamPerformancePackageAddress: allocation.teamPerformancePackage.address?.toString(),
        futarchyAmmVaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
        meteoraPoolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        meteoraVaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
        additionalTokenAllocation: supplyInfo.allocation?.additionalTokenAllocation,
        initialTokenAllocation: supplyInfo.allocation?.initialTokenAllocation,
        daoTreasuryTokens: supplyInfo.allocation?.daoTreasuryTokens,
        daoAddress: allocation.daoAddress?.toString(),
        launchAddress: allocation.launchAddress?.toString(),
        version: allocation.version,
      };
    }

    res.json(response);
  }));

  // Jupiter-compatible circulating supply
  router.get('/api/supply/:mintAddress/jupiter/circulating', asyncHandler(async (req: Request, res: Response) => {
    const mintAddress = orBadRequest(parseSolanaAddress(req.params.mintAddress, 'mintAddress'), 'INVALID_MINT_ADDRESS');

    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    const { supplyInfo } = await getSupplyInfoWithLaunchpadAllocation(
      mintAddress,
      solanaService,
      launchpadService,
    );

    res.json({ circulatingSupply: parseFloat(supplyInfo.circulatingSupply) });
  }));

  // Jupiter-compatible total supply
  router.get('/api/supply/:mintAddress/jupiter/total', asyncHandler(async (req: Request, res: Response) => {
    const mintAddress = orBadRequest(parseSolanaAddress(req.params.mintAddress, 'mintAddress'), 'INVALID_MINT_ADDRESS');

    const solanaService = getSolanaService();
    const supplyInfo = await solanaService.getSupplyInfo(mintAddress);

    res.json({ totalSupply: parseFloat(supplyInfo.totalSupply) });
  }));

  return router;
}
