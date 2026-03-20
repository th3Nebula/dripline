// Define your domain types here.
// Every entity should have: id, created, updated

export interface Plugin {
  id: string;
  name: string;
  version: string;
  tables: string[];
  created: string;
  updated: string;
}

export interface Connection {
  id: string;
  plugin: string;
  config: Record<string, string>;
  created: string;
  updated: string;
}
