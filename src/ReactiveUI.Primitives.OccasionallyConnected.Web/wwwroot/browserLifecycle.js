const registrations = new Map();

export function unobserve(id) {
    registrations.get(id)?.dispose();
}

export function observe(id, receiver) {
    unobserve(id);
    let disposed = false;
    let inFlight = false;
    let dirty = false;
    let departed = false;
    let frozen = false;
    const listeners = [];

    async function notify() {
        dirty = true;
        if (inFlight || disposed) return;
        inFlight = true;
        try {
            while (dirty && !disposed) {
                dirty = false;
                await receiver.invokeMethodAsync(
                    "OnBrowserStateChangedAsync",
                    navigator.onLine === true,
                    departed || frozen || document.visibilityState !== "visible");
            }
        } catch {
            // A disconnected circuit cannot receive hints. A later event can retry.
        } finally {
            inFlight = false;
        }
    }

    function listen(target, name, handler) {
        target.addEventListener(name, handler);
        listeners.push(() => target.removeEventListener(name, handler));
    }

    listen(window, "online", notify);
    listen(window, "offline", notify);
    listen(document, "visibilitychange", notify);
    listen(document, "freeze", () => { frozen = true; void notify(); });
    listen(document, "resume", () => { frozen = false; void notify(); });
    listen(window, "pagehide", () => { departed = true; void notify(); });
    listen(window, "pageshow", () => { departed = false; frozen = false; void notify(); });
    void notify();

    const registration = {
        dispose() {
            disposed = true;
            dirty = false;
            for (const remove of listeners) remove();
            listeners.length = 0;
            registrations.delete(id);
        }
    };
    registrations.set(id, registration);
    return registration;
}
