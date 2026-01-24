/**
 * Meteora Service
 * 
 * Provides mapping from Meteora pool owner addresses to token (baseMint) addresses.
 * This mapping is used to normalize Meteora pool data to match existing table structures.
 */

/**
 * Mapping from Meteora pool owner addresses to token (baseMint) addresses
 * 
 * The owner addresses correspond to the DAOs that have Meteora LP positions:
 * - Umbra, Ranger, Paystream, Loyal, Avici, ZKFG, Solomon
 * 
 * TODO: Update with actual baseMint addresses for each owner
 * These can be retrieved from FutarchyService.getAllDaos() by matching DAO addresses
 * or by querying the Meteora pools directly.
 */
export const METEORA_OWNER_TO_TOKEN_MAP: Map<string, string> = new Map([
  // Umbra
  ['6VsC8PuKkXm5xo54c2vbrAaSfQipkpGHqNuKTxXFySx6', ''], // TODO: Add Umbra baseMint
  // Ranger
  ['55H1Q1YrHJQ93uhG4jqrBBHx3a8H7TCM8kvf2UM2g5q3', ''], // TODO: Add Ranger baseMint
  // Paystream
  ['BpXtB2ASf2Tft97ewTd8PayXCqFQ6Wqod33qrwwfK9Vz', ''], // TODO: Add Paystream baseMint
  // Loyal
  ['AQyyTwCKemeeMu8ZPZFxrXMbVwAYTSbBhi1w4PBrhvYE', ''], // TODO: Add Loyal baseMint
  // Avici
  ['DGgYoUcu1aDZt4GEL5NQiducwHRGbkMWsUzsXh2j622G', ''], // TODO: Add Avici baseMint
  // ZKFG
  ['BNvDfXYG2FAyBDYD71Xr9GhKE18MbmhtjsLKsCuXho6z', ''], // TODO: Add ZKFG baseMint
  // Solomon
  ['98SPcyUZ2rqM2dgjCqqSXS4gJrNTLSNUAAVCF38xYj9u', ''], // TODO: Add Solomon baseMint
]);

/**
 * Get token address (baseMint) for a given Meteora owner address
 * @param ownerAddress The Meteora pool owner address
 * @returns The token (baseMint) address, or null if not found
 */
export function getTokenForOwner(ownerAddress: string): string | null {
  const normalizedOwner = ownerAddress.toLowerCase();
  const token = METEORA_OWNER_TO_TOKEN_MAP.get(normalizedOwner);
  return token || null;
}

/**
 * Get all known owner addresses
 * @returns Array of owner addresses
 */
export function getAllOwners(): string[] {
  return Array.from(METEORA_OWNER_TO_TOKEN_MAP.keys());
}

/**
 * Check if an owner address is known
 * @param ownerAddress The owner address to check
 * @returns True if the owner is in the mapping
 */
export function isKnownOwner(ownerAddress: string): boolean {
  return METEORA_OWNER_TO_TOKEN_MAP.has(ownerAddress.toLowerCase());
}
