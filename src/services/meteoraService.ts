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
  ['6vsc8pukkxm5xo54c2vbraasfqipkpghqnuktxxfysx6', 'PRVT6TB7uss3FrUd2D9xs2zqDBsa3GbMJMwCQsgmeta'], // TODO: Add Umbra baseMint
  // Ranger
  ['55h1q1yrhjq93uhg4jqrbbhx3a8h7tcm8kvf2um2g5q3', 'RNGRtJMbCveqCp7AC6U95KmrdKecFckaJZiWbPGmeta'], // TODO: Add Ranger baseMint
  // Paystream
  ['bpxtb2asf2tft97ewtd8payxcqfq6wqod33qrwwfk9vz', 'PAYZP1W3UmdEsNLJwmH61TNqACYJTvhXy8SCN4Tmeta'], // TODO: Add Paystream baseMint
  // Loyal
  ['aqyytwckemeemu8zpzfxrxmbvwaytsbbhi1w4pbrhvye', 'LYLikzBQtpa9ZgVrJsqYGQpR3cC1WMJrBHaXGrQmeta'], // TODO: Add Loyal baseMint
  // Avici
  ['dggyoucu1adzt4gel5nqiducwhrgbkmwsuzsxh2j622g', 'BANKJmvhT8tiJRsBSS1n2HryMBPvT5Ze4HU95DUAmeta'], // TODO: Add Avici baseMint
  // ZKFG
  ['bnvdfxyg2faybdyd71xr9ghke18mbmhtjslkscuxho6z', 'ZKFHiLAfAFMTcDAuCtjNW54VzpERvoe7PBF9mYgmeta'], // TODO: Add ZKFG baseMint
  // Solomon
  ['98spcyuz2rqm2dgjcqqsxs4gjrntlsnuaavcf38xyj9u', 'SoLo9oxzLDpcq1dpqAgMwgce5WqkRDtNXK7EPnbmeta'], // TODO: Add Solomon baseMint
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
