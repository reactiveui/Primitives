import { setImmediate } from "node:timers/promises";
import { observe, unobserve } from "./browserLifecycle.mjs";

globalThis.window = new EventTarget();
globalThis.document = new EventTarget();
document.visibilityState = "visible";
Object.defineProperty(globalThis, "navigator", { value: { onLine: true }, configurable: true });

const results = [];
const calls = [];
const receiver = { invokeMethodAsync: async (...args) => { calls.push(args); } };
observe("lifecycle", receiver);
await setImmediate();
results.push(calls.length === 1 && calls[0][0] === "OnBrowserStateChangedAsync"
    && calls[0][1] === true && calls[0][2] === false);

const events = [
    [window, "offline", () => { navigator.onLine = false; }, false, false],
    [window, "online", () => { navigator.onLine = true; }, true, false],
    [document, "visibilitychange", () => { document.visibilityState = "hidden"; }, true, true],
    [document, "visibilitychange", () => { document.visibilityState = "visible"; }, true, false],
    [document, "freeze", () => {}, true, true],
    [document, "resume", () => {}, true, false],
    [window, "pagehide", () => {}, true, true],
    [document, "visibilitychange", () => {}, true, true],
    [window, "pageshow", () => {}, true, false]
];
for (const [target, name, update, available, suspended] of events) {
    update();
    target.dispatchEvent(new Event(name));
    await setImmediate();
    results.push(calls.at(-1)[1] === available && calls.at(-1)[2] === suspended);
}

unobserve("lifecycle");
const removedCount = calls.length;
for (const [target, name] of events) target.dispatchEvent(new Event(name));
await setImmediate();
results.push(calls.length === removedCount);

let release;
let active = 0;
let maximum = 0;
const bounded = [];
observe("bounded", {
    async invokeMethodAsync(...args) {
        active++;
        maximum = Math.max(maximum, active);
        bounded.push(args);
        if (bounded.length === 1) await new Promise(resolve => { release = resolve; });
        active--;
    }
});
for (let i = 0; i < 1000; i++) {
    navigator.onLine = i % 2 === 0;
    window.dispatchEvent(new Event("online"));
}
navigator.onLine = false;
window.dispatchEvent(new Event("offline"));
results.push(bounded.length === 1 && maximum === 1);
release();
await setImmediate();
results.push(bounded.length === 2 && bounded.at(-1)[1] === false && maximum === 1);
unobserve("bounded");

const isolated = [];
observe("one", { invokeMethodAsync: async () => { isolated.push("one"); } });
observe("two", { invokeMethodAsync: async () => { isolated.push("two"); } });
await setImmediate();
unobserve("one");
isolated.length = 0;
window.dispatchEvent(new Event("online"));
await setImmediate();
results.push(isolated.length === 1 && isolated[0] === "two");
unobserve("two");

let failures = 0;
observe("recover", {
    async invokeMethodAsync() {
        failures++;
        if (failures === 1) throw new Error("disconnected");
    }
});
await setImmediate();
window.dispatchEvent(new Event("online"));
await setImmediate();
results.push(failures === 2);
unobserve("recover");

let departedCalls = 0;
let releaseDeparted;
observe("departed", {
    async invokeMethodAsync() {
        departedCalls++;
        await new Promise(resolve => { releaseDeparted = resolve; });
    }
});
window.dispatchEvent(new Event("offline"));
unobserve("departed");
releaseDeparted();
await setImmediate();
results.push(departedCalls === 1);

const resumed = [];
observe("resumed", { invokeMethodAsync: async (...args) => { resumed.push(args); } });
await setImmediate();
document.dispatchEvent(new Event("freeze"));
await setImmediate();
window.dispatchEvent(new Event("pagehide"));
await setImmediate();
document.dispatchEvent(new Event("resume"));
await setImmediate();
results.push(resumed.at(-1)[2] === true);
window.dispatchEvent(new Event("pageshow"));
await setImmediate();
results.push(resumed.length === 5 && resumed.at(-1)[2] === false);
unobserve("resumed");

console.log(JSON.stringify(results));
