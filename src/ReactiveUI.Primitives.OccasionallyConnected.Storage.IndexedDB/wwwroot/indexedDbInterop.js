const openDatabase = async (databaseName, objectStoreName) => {
  await ensureIndexedDb();

  return await new Promise((resolve, reject) => {
    const request = globalThis.indexedDB.open(databaseName, 1);

    request.onerror = () => reject(request.error ?? new Error("IndexedDB open failed."));
    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(objectStoreName)) {
        database.createObjectStore(objectStoreName, { keyPath: "key" });
      }
    };
    request.onsuccess = () => resolve(request.result);
  });
};

const ensureIndexedDb = async () => {
  if (!globalThis.indexedDB) {
    throw new Error("IndexedDB is unavailable in this browser environment.");
  }
};

const runRequest = async (requestFactory) =>
  await new Promise((resolve, reject) => {
    const request = requestFactory();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed."));
    request.onsuccess = () => resolve(request.result);
  });

const withStore = async (databaseName, objectStoreName, mode, action) => {
  const database = await openDatabase(databaseName, objectStoreName);

  try {
    const transaction = database.transaction(objectStoreName, mode);
    const store = transaction.objectStore(objectStoreName);
    const result = await action(store);

    await new Promise((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onerror = () =>
        reject(transaction.error ?? new Error("IndexedDB transaction failed."));
      transaction.onabort = () =>
        reject(transaction.error ?? new Error("IndexedDB transaction aborted."));
    });

    return result;
  } finally {
    database.close();
  }
};

export async function loadStore(databaseName, objectStoreName, key) {
  return await withStore(databaseName, objectStoreName, "readonly", async (store) => {
    const record = await runRequest(() => store.get(key));
    return record?.json ?? null;
  });
}

export async function compareExchangeStore(
  databaseName,
  objectStoreName,
  key,
  expectedGeneration,
  json) {
  return await withStore(databaseName, objectStoreName, "readwrite", async (store) => {
    const current = await runRequest(() => store.get(key));
    const currentGeneration = current?.generation ?? 0;
    if (currentGeneration !== expectedGeneration) {
      return false;
    }

    const next = JSON.parse(json);
    await runRequest(() =>
      store.put({
        generation: next.Generation ?? 0,
        json,
        key
      }));
    return true;
  });
}
