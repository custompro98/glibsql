import { Ok, Error, Result } from "./gleam.mjs";
import { createClient } from "@libsql/client";

/**
 * @param {string} url
 * @param {string} token
 * @returns {Result<import("@libsql/client").Client}
 */
export function do_build(url, token) {
  try {
    return new Ok(
      createClient({
        url: url,
        authToken: token,
      }),
    );
  } catch (error) {
    return new Error(error);
  }
}

/**
 * @param {import("@libsql/client").Client} client
 * @param {string} query
 */
export async function do_execute(client, query) {
  try {
    const { rowsAffected, lastInsertRowid, rows } = await client.execute(query);

    return new Ok({
      rows_affected: rowsAffected,
      last_insert_rowid: lastInsertRowid,
      rows,
    });
  } catch (error) {
    console.log(`Error: ${error}`);
    return new Error(error);
  }
}
