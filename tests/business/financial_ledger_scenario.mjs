/**
 * Business Logic Test Scenario: Financial Ledger & Balance Invariants
 * Separated cleanly from the native kernel engine.
 */

import fs from 'node:fs';
import path from 'node:path';

export const FinancialSchema = JSON.parse(
  fs.readFileSync(new URL('../../config/declarative/schemas/finance_balances.json', import.meta.url), 'utf-8')
);

export function simulateTransfer(senderBalance, amount) {
  if (amount <= 0) {
    throw new Error('Transfer amount must be positive');
  }
  const newBalance = senderBalance - amount;
  if (newBalance < (FinancialSchema.rules.min_value ?? 0)) {
    throw new Error(`ERR_SCHEMA_VIOLATION: sender balance cannot fall below ${FinancialSchema.rules.min_value}`);
  }
  return newBalance;
}
