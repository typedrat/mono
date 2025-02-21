import {assert} from '../../../shared/src/asserts.ts';
import type {
  Clause,
  LiteralValue,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';

export function foldConstants(clause: Clause): Clause {
  if (clause.type === 'literal') {
    return clause;
  }
  switch (clause.condition.type) {
    case 'and':
      clause.condition.clauses = clause.condition.clauses.map(foldConstants);
      return simplifyAnd(clause);
    case 'or':
      clause.condition.clauses.forEach(foldConstants);
      return simplifyOr(clause);
    case 'simple':
      return simplifySimple(clause);
    case 'correlatedSubquery':
      throw new Error('TODO');
  }
}

function simplifyAnd(clause: Clause): Clause {
  assert(clause.type === 'condition');
  assert(clause.condition.type === 'and');
  const subClauses = clause.condition.clauses;
  if (subClauses.some(c => c.type === 'literal' && c.value === false)) {
    return {type: 'literal', value: false};
  }
  if (subClauses.every(c => c.type === 'literal' && c.value === true)) {
    return {type: 'literal', value: true};
  }
  return clause;
}

function simplifyOr(clause: Clause): Clause {
  assert(clause.type === 'condition');
  assert(clause.condition.type === 'or');
  const subClauses = clause.condition.clauses;
  if (subClauses.some(c => c.type === 'literal' && c.value === true)) {
    return {type: 'literal', value: true};
  }
  if (subClauses.every(c => c.type === 'literal' && c.value === false)) {
    return {type: 'literal', value: false};
  }
  return clause;
}

function simplifySimple(clause: Clause): Clause {
  assert(clause.type === 'condition');
  assert(clause.condition.type === 'simple');
  const left = getLiteralValue(clause.condition.left);
  const right = getLiteralValue(clause.condition.right);
  if (left === undefined || right === undefined) {
    return clause;
  }
  const value = evaluate(clause.condition.op, left, right);
  if (value === undefined) {
    return clause;
  }
  return {
    type: 'literal',
    value,
  };
}

function getLiteralValue(value: ValuePosition): LiteralValue | undefined {
  if (value.type === 'literal') {
    return value.value;
  }
  return undefined;
}

function evaluate(
  op: string,
  left: LiteralValue,
  right: LiteralValue,
): LiteralValue | undefined {
  throw new Error('TODO');
}
