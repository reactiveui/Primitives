import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

export async function waitForDebuggingPort(path, browser, output, deadline,
    readPortFile = readFile, wait = delay, platform = process.platform) {
    let port;
    while (!port) {
        deadline.throwIfAborted();
        if (browser.exitCode !== null) {
            throw new Error(`Browser exited before debugging started (exit ${browser.exitCode}): ${output()}`);
        }
        try { port = Number((await readPortFile(path, "utf8")).split("\n")[0]); }
        catch (error) {
            // Chromium creates this file during startup. Windows can deny the read while it writes it.
            if (error.code !== "ENOENT" && !(platform === "win32" && error.code === "EBUSY")) throw error;
        }
        if (!port) await wait(25, undefined, { signal: deadline });
    }
    return port;
}
