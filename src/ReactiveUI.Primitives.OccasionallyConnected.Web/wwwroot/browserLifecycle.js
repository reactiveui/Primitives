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

    function notify() {
        dirty = true;
        startDelivery();
    }

    function startDelivery() {
        if (inFlight || disposed || !dirty) return;
        dirty = false;
        inFlight = true;
        void deliver();
    }

    async function deliver() {
        try {
            await receiver.invokeMethodAsync(
                "OnBrowserStateChangedAsync",
                navigator.onLine === true,
                departed || frozen || document.visibilityState !== "visible");
        } catch {
            // A disconnected circuit cannot receive hints. A later event can retry.
            inFlight = false;
            return;
        }

        inFlight = false;
        startDelivery();
    }

    function listen(target, name, handler) {
        target.addEventListener(name, handler);
        listeners.push(() => target.removeEventListener(name, handler));
    }

    listen(window, "online", notify);
    listen(window, "offline", notify);
    listen(document, "visibilitychange", notify);
    listen(document, "freeze", () => { frozen = true; notify(); });
    listen(document, "resume", () => { frozen = false; notify(); });
    listen(window, "pagehide", () => { departed = true; notify(); });
    listen(window, "pageshow", () => { departed = false; frozen = false; notify(); });
    notify();

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
