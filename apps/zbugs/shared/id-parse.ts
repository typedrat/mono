export function parseIssueId(
  issueId: string,
): ['id', string] | ['shortID', number] {
  if (/[^\d]/.test(issueId)) {
    return ['id', issueId] as const;
  }
  return ['shortID', parseInt(issueId)] as const;
}
