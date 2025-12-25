declare module "connect-sqlite3" {
  import type session from "express-session";
  const connectSqlite3: (session: typeof session) => any;
  export default connectSqlite3;
}
