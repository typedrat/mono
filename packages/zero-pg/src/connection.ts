export interface Connection {
  transact<R>(callback: (tx: ConnectionTransaction) => Promise<R>): Promise<R>;
}

export interface ConnectionTransaction {
  query(sql: string, params: unknown[]): Promise<unknown[]>;
}
