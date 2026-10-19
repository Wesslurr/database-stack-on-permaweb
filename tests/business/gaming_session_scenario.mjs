/**
 * Business Logic Test Scenario: Gaming Sessions & State Pipeline
 * Separated cleanly from the native kernel engine.
 */

import fs from 'node:fs';

export const PlayerSessionSchema = JSON.parse(
  fs.readFileSync(new URL('../../config/declarative/schemas/player_sessions.json', import.meta.url), 'utf-8')
);

export function validatePlayerSession(sessionData) {
  if (!sessionData || typeof sessionData !== 'object') {
    throw new Error('ERR_INVALID_INPUT: sessionData must be an object');
  }
  if (!sessionData.session_token) {
    throw new Error('ERR_UNAUTHORIZED: missing required session_token');
  }
  return true;
}
