import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { once } from "node:events";
import { fileURLToPath } from "node:url";

export async function startFixture() {
  const path = fileURLToPath(new URL("../../tests/fixture_server.py", import.meta.url));
  const fixture = spawn("python3", [path], { stdio: ["ignore", "pipe", "inherit"] });
  const lines = createInterface({ input: fixture.stdout });
  const [url] = await Promise.race([
    once(lines, "line"),
    once(fixture, "exit").then(([code]) => { throw new Error(`Fixture exited with status ${code}`); }),
  ]);
  return {
    url,
    close() {
      lines.close();
      fixture.kill();
    },
  };
}
