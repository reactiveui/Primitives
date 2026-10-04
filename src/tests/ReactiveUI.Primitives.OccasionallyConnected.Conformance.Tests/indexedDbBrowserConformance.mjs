import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { readFile, mkdtemp, rm, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const deadline = AbortSignal.timeout(45000);
const profile = await mkdtemp(join(tmpdir(), "rxui-indexeddb-conformance-"));
const candidates = process.env.RXUI_CONFORMANCE_BROWSER
    ? [process.env.RXUI_CONFORMANCE_BROWSER]
    : process.platform === "win32"
        ? [join(process.env["PROGRAMFILES(X86)"] ?? "", "Microsoft", "Edge", "Application", "msedge.exe"),
            join(process.env.PROGRAMFILES ?? "", "Google", "Chrome", "Application", "chrome.exe")]
        : process.platform === "darwin"
            ? ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"]
            : ["/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"];
let executable;
for (const candidate of candidates) {
    try { await access(candidate); executable = candidate; break; }
    catch (error) { if (error.code !== "ENOENT") throw error; }
}
if (!executable) throw new Error("A real Chromium browser is required. Set RXUI_CONFORMANCE_BROWSER.");

const moduleSource = await readFile(process.argv[2] ?? new URL("./indexedDbInterop.js", import.meta.url));
const server = createServer((request, response) => {
    response.setHeader("Content-Type", request.url === "/indexedDbInterop.js" ? "text/javascript" : "text/html");
    response.end(request.url === "/indexedDbInterop.js" ? moduleSource : "<!doctype html><title>IndexedDB conformance</title>");
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
const browserArguments = [
    "--headless=new", "--no-sandbox", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
    "--remote-debugging-port=0", `--user-data-dir=${profile}`, origin
];
let browser = spawn(executable, browserArguments, { stdio: ["ignore", "pipe", "pipe"] });
let output = "";
browser.stdout.on("data", data => { output += data; });
browser.stderr.on("data", data => { output += data; });
let socket;
try {
    let port;
    while (!port) {
        deadline.throwIfAborted();
        if (browser.exitCode !== null) throw new Error(`Browser exited before debugging started (exit ${browser.exitCode}): ${output}`);
        try { port = Number((await readFile(join(profile, "DevToolsActivePort"), "utf8")).split("\n")[0]); }
        catch (error) { if (error.code !== "ENOENT") throw error; }
        if (!port) await delay(25, undefined, { signal: deadline });
    }
    let page;
    while (!page) {
        const pages = await (await fetch(`http://127.0.0.1:${port}/json/list`, { signal: deadline })).json();
        page = pages.find(item => item.type === "page" && item.url.startsWith(origin));
        if (!page) await delay(25, undefined, { signal: deadline });
    }
    socket = new WebSocket(page.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
        socket.addEventListener("open", resolve, { once: true });
        socket.addEventListener("error", reject, { once: true });
    });
    let sequence = 0;
    const pending = new Map();
    socket.addEventListener("message", event => {
        const message = JSON.parse(event.data);
        const request = pending.get(message.id);
        if (!request) return;
        pending.delete(message.id);
        if (message.error) request.reject(new Error(JSON.stringify(message.error)));
        else request.resolve(message.result);
    });
    function call(method, params) {
        return new Promise((resolve, reject) => {
            const id = ++sequence;
            pending.set(id, { resolve, reject });
            socket.send(JSON.stringify({ id, method, params }));
        });
    }
    async function evaluate(expression) {
        const result = await call("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
        if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
        return result.result.value;
    }
    async function waitForDocument() {
        while (!await evaluate(`location.origin === ${JSON.stringify(origin)} && document.readyState === "complete"`)) {
            await delay(25, undefined, { signal: deadline });
        }
    }
    await waitForDocument();
    const checks = await evaluate(`(async () => {
        const api = await import("/indexedDbInterop.js");
        const db = "conformance";
        const table = "state";
        const key = "client";
        const checks = [];
        checks.push(await api.loadStore(db, table, key) === null);
        checks.push(await api.compareExchangeStore(db, table, key, 0, JSON.stringify({ Generation: 1, marker: "first" })));
        const writes = await Promise.all([
            api.compareExchangeStore(db, table, key, 1, JSON.stringify({ Generation: 2, marker: "left" })),
            api.compareExchangeStore(db, table, key, 1, JSON.stringify({ Generation: 2, marker: "right" }))
        ]);
        checks.push(writes.filter(Boolean).length === 1);
        const committed = await api.loadStore(db, table, key);
        checks.push(JSON.parse(committed).Generation === 2);
        checks.push(!await api.compareExchangeStore(db, table, key, 1, JSON.stringify({ Generation: 3, marker: "stale" })));
        checks.push(await api.loadStore(db, table, key) === committed);
        let rejected = false;
        try { await api.compareExchangeStore(db, table, key, 2, "{"); }
        catch (error) { rejected = error instanceof SyntaxError; }
        checks.push(rejected);
        checks.push(await api.loadStore(db, table, key) === committed);
        sessionStorage.setItem("committed", committed);
        return checks;
    })()`);
    // A new page cannot reuse module objects or their open database handles.
    const target = await call("Target.createTarget", { url: origin });
    const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`, { signal: deadline })).json();
    const reopened = targets.find(item => item.id === target.targetId);
    if (!reopened) throw new Error("The reopened browser page was not created.");
    const before = await evaluate("sessionStorage.getItem('committed')");
    socket.close();
    socket = new WebSocket(reopened.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
        socket.addEventListener("open", resolve, { once: true });
        socket.addEventListener("error", reject, { once: true });
    });
    socket.addEventListener("message", event => {
        const message = JSON.parse(event.data);
        const request = pending.get(message.id);
        if (!request) return;
        pending.delete(message.id);
        if (message.error) request.reject(new Error(JSON.stringify(message.error)));
        else request.resolve(message.result);
    });
    await waitForDocument();
    const persisted = await evaluate(`(async () => {
        const api = await import("/indexedDbInterop.js");
        return await api.loadStore("conformance", "state", "client");
    })()`);
    checks.push(persisted === before);
    // Reopen the persistent profile in a different browser process.
    socket.send(JSON.stringify({ id: -1, method: "Browser.close" }));
    socket.close();
    await new Promise(resolve => browser.exitCode !== null ? resolve() : browser.once("exit", resolve));
    await rm(join(profile, "DevToolsActivePort"), { force: true });
    browser = spawn(executable, browserArguments, { stdio: ["ignore", "pipe", "pipe"] });
    browser.stdout.on("data", data => { output += data; });
    browser.stderr.on("data", data => { output += data; });
    port = undefined;
    while (!port) {
        deadline.throwIfAborted();
        if (browser.exitCode !== null) throw new Error(`Reopened browser exited (exit ${browser.exitCode}): ${output}`);
        try { port = Number((await readFile(join(profile, "DevToolsActivePort"), "utf8")).split("\n")[0]); }
        catch (error) { if (error.code !== "ENOENT") throw error; }
        if (!port) await delay(25, undefined, { signal: deadline });
    }
    page = undefined;
    while (!page) {
        const pages = await (await fetch(`http://127.0.0.1:${port}/json/list`, { signal: deadline })).json();
        page = pages.find(item => item.type === "page" && item.url.startsWith(origin));
        if (!page) await delay(25, undefined, { signal: deadline });
    }
    socket = new WebSocket(page.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
        socket.addEventListener("open", resolve, { once: true });
        socket.addEventListener("error", reject, { once: true });
    });
    socket.addEventListener("message", event => {
        const message = JSON.parse(event.data);
        const request = pending.get(message.id);
        if (!request) return;
        pending.delete(message.id);
        if (message.error) request.reject(new Error(JSON.stringify(message.error)));
        else request.resolve(message.result);
    });
    await waitForDocument();
    checks.push(await evaluate(`(async () => {
        const api = await import("/indexedDbInterop.js");
        return await api.loadStore("conformance", "state", "client");
    })()`) === before);
    console.log(JSON.stringify(checks));
} finally {
    if (socket?.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ id: -1, method: "Browser.close" }));
    }

    socket?.close();
    await new Promise(resolve => {
        if (browser.exitCode !== null) resolve();
        else {
            const shutdown = setTimeout(() => browser.kill(), 1000);
            browser.once("exit", () => { clearTimeout(shutdown); resolve(); });
        }
    });
    await new Promise(resolve => server.close(resolve));
    await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
