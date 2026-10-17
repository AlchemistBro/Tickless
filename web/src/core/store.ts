import { useSyncExternalStore } from "react";

/** A minimal observable value, enough for app-wide state without a framework. */
export class Store<T> {
  private listeners = new Set<() => void>();
  constructor(private value: T) {}

  get = (): T => this.value;

  set = (next: T): void => {
    if (Object.is(next, this.value)) return;
    this.value = next;
    this.listeners.forEach((l) => l());
  };

  update = (f: (cur: T) => T): void => this.set(f(this.value));

  subscribe = (l: () => void): (() => void) => {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  };
}

export function useStore<T>(store: Store<T>): T {
  return useSyncExternalStore(store.subscribe, store.get);
}

/** A store whose value is mirrored to localStorage (failures are ignored). */
export function persistedStore<T>(key: string, initial: T): Store<T> {
  let start = initial;
  try {
    const raw = localStorage.getItem(key);
    if (raw) start = JSON.parse(raw) as T;
  } catch {
    /* corrupted or unavailable storage: fall back to the default */
  }
  const s = new Store<T>(start);
  s.subscribe(() => {
    try {
      localStorage.setItem(key, JSON.stringify(s.get()));
    } catch {
      /* storage full or blocked */
    }
  });
  return s;
}
