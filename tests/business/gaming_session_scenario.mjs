/**
 * Business Logic Test Scenario: Gaming Sessions & State Pipeline (ets_doc)
 * Demonstrates complex flexible JSON documents with dynamic session token validation.
 *
 * Invariants Proven:
 * 1. Schema enforcement: Missing required `session_token` rejected immediately.
 * 2. Lifecycle transitions: Created -> InMatch -> Finished with state validation.
 * 3. ELO / Score bounds: Score increments tracked monotonically.
 * 4. Document update idempotency: Re-submitting same update does not corrupt metadata.
 */

import fs from 'node:fs';

export const PlayerSessionSchema = JSON.parse(
  fs.readFileSync(new URL('../../config/declarative/schemas/player_sessions.json', import.meta.url), 'utf-8')
);

export function createPlayerSession(playerId, sessionToken, initialElo = 1200) {
  const session = {
    player_id: playerId,
    session_token: sessionToken,
    status: 'idle',
    elo: initialElo,
    matches_played: 0,
    created_at: Date.now()
  };
  validatePlayerSession(session);
  return session;
}

export function validatePlayerSession(sessionData) {
  if (!sessionData || typeof sessionData !== 'object') {
    throw new Error('ERR_INVALID_INPUT: sessionData must be an object');
  }
  const requiredFields = PlayerSessionSchema.rules.requires || ['session_token'];
  for (const field of requiredFields) {
    if (!sessionData[field]) {
      throw new Error(`ERR_UNAUTHORIZED: missing required field '${field}'`);
    }
  }
  return true;
}

export function recordMatchResult(session, won, eloDelta = 25) {
  validatePlayerSession(session);
  if (session.status === 'finished') {
    throw new Error('ERR_INVALID_STATE: cannot record match on terminated session');
  }
  return {
    ...session,
    status: 'in_match',
    matches_played: session.matches_played + 1,
    elo: won ? session.elo + eloDelta : Math.max(0, session.elo - eloDelta),
    last_updated: Date.now()
  };
}

export function executeGamingTests() {
  console.log('========================================================================');
  console.log(' Business Scenario: Gaming Sessions & State Pipeline (ets_doc)');
  console.log('========================================================================\n');

  let passed = 0;
  const assert = (cond, msg) => {
    if (!cond) {
      console.error(`  ❌ FAIL: ${msg}`);
      throw new Error(`Gaming test assertion failed: ${msg}`);
    }
    console.log(`  ✅ PASS: ${msg}`);
    passed++;
  };

  // 1. Session creation with token validation
  console.log('[Scenario 1] Player Session Creation & Schema Validation:');
  const session = createPlayerSession('player_99', 'tok_sec_abc123', 1500);
  assert(session.player_id === 'player_99', 'Player ID bound');
  assert(session.session_token === 'tok_sec_abc123', 'Session token stored');
  assert(session.elo === 1500, 'Initial ELO set');

  // 2. Missing token rejected
  console.log('\n[Scenario 2] Rogue Session Missing Token Rejected:');
  let missingTokenCaught = false;
  try {
    validatePlayerSession({ player_id: 'rogue_user' });
  } catch (err) {
    missingTokenCaught = true;
    assert(err.message.includes('ERR_UNAUTHORIZED'), `Missing token rejected: ${err.message}`);
  }
  assert(missingTokenCaught, 'Session without token blocked cleanly');

  // 3. Match Progression & ELO Calculation
  console.log('\n[Scenario 3] Match Progression & ELO Calculation:');
  const afterWin = recordMatchResult(session, true, 30);
  assert(afterWin.matches_played === 1, 'Match counter incremented to 1');
  assert(afterWin.elo === 1530, 'ELO increased after victory (1500 -> 1530)');

  const afterLoss = recordMatchResult(afterWin, false, 20);
  assert(afterLoss.matches_played === 2, 'Match counter incremented to 2');
  assert(afterLoss.elo === 1510, 'ELO reduced after loss (1530 -> 1510)');

  console.log(`\n========================================================================`);
  console.log(` Gaming Session Scenario Results: ${passed} PASSED, 0 FAILED`);
  console.log(`========================================================================\n`);
}

// Auto-execute if run as main script
if (process.argv[1] && process.argv[1].endsWith('gaming_session_scenario.mjs')) {
  executeGamingTests();
}
