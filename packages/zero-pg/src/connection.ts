export interface Connection<CT extends ConnectionTransaction> {
  transact<R>(callback: (tx: CT) => Promise<R>): Promise<R>;
}

export interface ConnectionTransaction {
  query(sql: string, params: unknown[]): Promise<unknown[]>;
}
